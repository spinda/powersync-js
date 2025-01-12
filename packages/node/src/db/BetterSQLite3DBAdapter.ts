import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import BetterSQLite3Database from 'better-sqlite3';
import { packageDirectory } from 'pkg-dir';

import {
  BaseObserver,
  BatchedUpdateNotification,
  DBAdapter,
  DBAdapterListener,
  LockContext,
  Transaction,
  DBLockOptions,
  QueryResult,
  SQLOpenOptions,
} from '@powersync/common';

type BetterSQLite3Database = BetterSQLite3Database.Database;

const pkgDirPath = await packageDirectory({ cwd: path.dirname(fileURLToPath(import.meta.url)) });
if (pkgDirPath === undefined) {
  throw new Error("Couldn't find package directory");
}

const extensionJsonFilePath = path.join(pkgDirPath, 'extension.json');
const { tag: extensionTag, assets: extensionAssets } =
  JSON.parse(await fs.readFile(extensionJsonFilePath, 'utf8'));

const platform = os.platform();
const platformExtensionAssets = extensionAssets[platform];
if (!platformExtensionAssets) {
  throw new Error(`Unsupported platform: ${platform}`);
}

const arch = os.arch();
const extensionAsset = platformExtensionAssets[arch];
if (!extensionAsset) {
  throw new Error(`Unsupported arch for platform ${platform}: ${arch}`);
}

const extensionDirPath = path.join(pkgDirPath, 'extension', extensionTag);
const extensionFilePath = path.join(extensionDirPath, extensionAsset.fileName);

export type BetterSQLite3LockContext = LockContext & {
  executeBatch(query: string, params?: any[][]): Promise<QueryResult>;
};

export type BetterSQLite3Transaction = Transaction & BetterSQLite3LockContext;

const READ_CONNECTIONS = 5;

/**
 * Adapter for better-sqlite3
 */
export class BetterSQLite3DBAdapter extends BaseObserver<DBAdapterListener> implements DBAdapter {
  public readonly name: string;

  private readonly readConnections: Array<Connection>;
  private readonly writeConnection: Connection;

  private readonly readQueue: Array<(connection: Connection) => void> = [];
  private readonly writeQueue: Array<() => void> = [];

  private readonly uncommittedUpdatedTables = new Set<string>();
  private readonly committedUpdatedTables = new Set<string>();

  constructor(options: SQLOpenOptions) {
    super();

    this.name = options.dbFilename;

    let dbFilePath = options.dbFilename;
    if (options.dbLocation !== undefined) {
      dbFilePath = path.join(options.dbLocation, dbFilePath);
    }

    this.writeConnection = new Connection(dbFilePath);
    this.writeConnection.baseDB.pragma('journal_mode = WAL');

    this.writeConnection.baseDB.updateHook((_op, _dbName, tableName, _rowid) => {
      this.uncommittedUpdatedTables.add(tableName);
    });
    this.writeConnection.baseDB.commitHook(() => {
      for (const tableName of this.uncommittedUpdatedTables) {
        this.committedUpdatedTables.add(tableName);
      }
      this.uncommittedUpdatedTables.clear();
      return true;
    });
    this.writeConnection.baseDB.rollbackHook(() => {
      this.uncommittedUpdatedTables.clear();
    });

    this.readConnections = [];
    for (let i = 0; i < READ_CONNECTIONS; i++) {
      const readConnection = new Connection(dbFilePath);
      readConnection.baseDB.pragma('query_only = true');
      this.readConnections.push(readConnection);
    }
  }

  close() {
    this.writeConnection.baseDB.close();
    for (const connection of this.readConnections) {
      connection.baseDB.close();
    }
  }

  readLock<T>(
    fn: (tx: BetterSQLite3LockContext) => Promise<T>,
    _options?: DBLockOptions | undefined,
  ): Promise<T> {
    let resolveConnectionPromise!: (connection: Connection) => void;
    const connectionPromise = new Promise<Connection>((resolve, _reject) => {
      resolveConnectionPromise = resolve;
    });

    const connection = this.readConnections.find((connection) => !connection.isBusy);
    if (connection) {
      connection.isBusy = true;
      resolveConnectionPromise(connection);
    } else {
      this.readQueue.push(resolveConnectionPromise);
    }

    return (async () => {
      const connection = await connectionPromise;

      try {
        return await fn(connection);
      } finally {
        const next = this.readQueue.shift();
        if (next) {
          next(connection);
        } else {
          connection.isBusy = false;
        }
      }
    })();
  }

  writeLock<T>(
    fn: (tx: BetterSQLite3LockContext) => Promise<T>,
    _options?: DBLockOptions | undefined,
  ): Promise<T> {
    let resolveLockPromise!: () => void;
    const lockPromise = new Promise<void>((resolve, _reject) => {
      resolveLockPromise = resolve;
    });

    if (!this.writeConnection.isBusy) {
      this.writeConnection.isBusy = true;
      resolveLockPromise();
    } else {
      this.writeQueue.push(resolveLockPromise);
    }

    return (async () => {
      await lockPromise;

      try {
        try {
          return await fn(this.writeConnection);
        } finally {
          if (this.committedUpdatedTables.size > 0) {
            const event: BatchedUpdateNotification = {
              tables: [...this.committedUpdatedTables],
              groupedUpdates: {},
              rawUpdates: [],
            };
            this.committedUpdatedTables.clear();
            this.iterateListeners((cb) => cb.tablesUpdated?.(event));
          }
        }
      } finally {
        const next = this.writeQueue.shift();
        if (next) {
          next();
        } else {
          this.writeConnection.isBusy = false;
        }
      }
    })();
  }

  readTransaction<T>(
    fn: (tx: BetterSQLite3Transaction) => Promise<T>,
    _options?: DBLockOptions | undefined,
  ): Promise<T> {
    return this.readLock((ctx) => this.internalTransaction(ctx as Connection, fn));
  }

  writeTransaction<T>(
    fn: (tx: BetterSQLite3Transaction) => Promise<T>,
    _options?: DBLockOptions | undefined,
  ): Promise<T> {
    return this.writeLock((ctx) => this.internalTransaction(ctx as Connection, fn));
  }

  private async internalTransaction<T>(
    connection: Connection,
    fn: (tx: BetterSQLite3Transaction) => Promise<T>,
  ): Promise<T> {
    let finalized = false;
    const commit = async (): Promise<QueryResult> => {
      if (!finalized) {
        finalized = true;
        connection.baseDB.exec('COMMIT');
      }
      return { rowsAffected: 0 };
    };
    const rollback = async (): Promise<QueryResult> => {
      if (!finalized) {
        finalized = true;
        connection.baseDB.exec('ROLLBACK');
      }
      return { rowsAffected: 0 };
    };
    try {
      connection.baseDB.exec('BEGIN');
      const result = await fn({
        execute: (query, params) => connection.execute(query, params),
        executeBatch: (query, params) => connection.executeBatch(query, params),
        get: (query, params) => connection.get(query, params),
        getAll: (query, params) => connection.getAll(query, params),
        getOptional: (query, params) => connection.getOptional(query, params),
        commit,
        rollback
      });
      await commit();
      return result;
    } catch (ex) {
      await rollback();
      throw ex;
    }
  }

  getAll<T>(sql: string, parameters?: any[]): Promise<T[]> {
    return this.readLock((ctx) => ctx.getAll(sql, parameters));
  }

  getOptional<T>(sql: string, parameters?: any[]): Promise<T | null> {
    return this.readLock((ctx) => ctx.getOptional(sql, parameters));
  }

  get<T>(sql: string, parameters?: any[]): Promise<T> {
    return this.readLock((ctx) => ctx.get(sql, parameters));
  }

  execute(query: string, params?: any[] | undefined): Promise<QueryResult> {
    return this.writeLock((ctx) => ctx.execute(query, params));
  }

  executeBatch(query: string, params?: any[][]): Promise<QueryResult> {
    return this.writeTransaction((ctx) => ctx.executeBatch(query, params));
  }

  async refreshSchema() {
    await this.writeConnection.refreshSchema();

    for (const readConnection of this.readConnections) {
      await readConnection.refreshSchema();
    }
  }
}

class Connection implements BetterSQLite3LockContext {
  readonly baseDB: BetterSQLite3Database;
  isBusy = false;

  constructor(dbFilePath: string) {
    this.baseDB = new BetterSQLite3Database(dbFilePath);
    this.baseDB.loadExtension(extensionFilePath, 'sqlite3_powersync_init');
  }

  async execute(query: string, params?: any[]): Promise<QueryResult> {
    params = params ?? [];

    const stmt = this.baseDB.prepare(query);
    if (stmt.reader) {
      const rows = stmt.all(...params);
      return {
        rowsAffected: 0,
        rows: {
          _array: rows,
          length: rows.length,
          item: (idx: number) => rows[idx],
        },
      };
    } else {
      const info = stmt.run(...params);
      return {
        rowsAffected: info.changes,
        insertId: Number(info.lastInsertRowid),
      };
    }
  }

  async executeBatch(query: string, params: any[][] = []): Promise<QueryResult> {
    params = params ?? [];

    let rowsAffected = 0;

    const stmt = this.baseDB.prepare(query);
    for (const paramSet of params) {
      const info = stmt.run(...paramSet);
      rowsAffected += info.changes;
    }

    return { rowsAffected };
  }

  async getAll<T>(sql: string, parameters?: any[]): Promise<T[]> {
    const res = await this.execute(sql, parameters);
    return res.rows?._array ?? [];
  }

  async getOptional<T>(sql: string, parameters?: any[]): Promise<T | null> {
    const res = await this.execute(sql, parameters);
    return res.rows?.item(0) ?? null;
  }

  async get<T>(sql: string, parameters?: any[]): Promise<T> {
    const res = await this.execute(sql, parameters);
    const first = res.rows?.item(0);
    if (!first) {
      throw new Error('Result set is empty');
    }
    return first;
  }

  async refreshSchema() {
    await this.baseDB.pragma("table_info('sqlite_master')");
  }
}
