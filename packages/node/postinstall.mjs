#!/usr/bin/env node

// This script downloads the platform- and architecture-specific prebuilt asset
// for the PowerSync SQLite extension on post-install step of this package. It
// may be better to replace this with publishing those assets as npm packages
// like `powersync-sqlite-core-darwin-arm64`, etc.

import crypto from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import { packageDirectory } from 'pkg-dir';

const pkgDirPath = await packageDirectory({ cwd: path.dirname(fileURLToPath(import.meta.url)) });
if (pkgDirPath === undefined) {
  throw new Error("Couldn't find package directory");
}

const extensionJsonFilePath = path.join(pkgDirPath, 'extension.json');
const { repo, tag, assets } = JSON.parse(await fs.readFile(extensionJsonFilePath, 'utf8'));

const platform = os.platform();
const platformAssets = assets[platform];
if (!platformAssets) {
  throw new Error(`No prebuilt ${repo} assets available for platform ${platform}`);
}

const arch = os.arch();
const asset = platformAssets[arch];
if (!asset) {
  throw new Error(`No prebuilt ${repo} asset available for platform ${platform} and arch ${arch}`);
}

const assetFileUrl = `https://github.com/${repo}/releases/download/${tag}/${asset.fileName}`;

const extensionDirPath = path.join(pkgDirPath, 'extension', tag);
const extensionFilePath = path.join(extensionDirPath, asset.fileName);

try {
  const existingExtensionFile = createReadStream(extensionFilePath);
  const existingExtensionFileHasher = crypto.createHash('sha256');
  await pipeline(existingExtensionFile, existingExtensionFileHasher);
  const existingExtensionFileHash = existingExtensionFileHasher.digest('hex').toLowerCase();

  if (existingExtensionFileHash === asset.sha256) {
    console.log(`Prebuilt asset at ${extensionFilePath} matches SHA-256 hash; skipping download`);
    process.exit(0);
  }

  console.warn(
    `Prebuilt asset at File at ${extensionFilePath} doesn't match SHA-256 hash: ` +
      `expected ${asset.sha256}, found ${existingExtensionFileHash} - redownloading`
  );
} catch (err) {
  if (err.code !== 'ENOENT') {
    throw err;
  }
}

await fs.mkdir(extensionDirPath, { recursive: true });

console.log(`Downloading ${asset.fileName} from ${assetFileUrl}`);

const tmpDirPath = await fs.mkdtemp(path.join(os.tmpdir(), 'powersync-'));


try {
  const tmpFilePath = path.join(tmpDirPath, asset.fileName);

  const res = await fetch(assetFileUrl);
  if (!res.ok) {
    throw new Error(
      `Failed to download ${asset.fileName} from ${assetFileUrl} ` +
        `(${res.status} ${res.statusText})`,
    );
  }

  const extensionFileHasher = crypto.createHash('sha256');
  const hashTransform = new Transform({
    transform(chunk, _encoding, cb) {
      extensionFileHasher.update(chunk);
      cb(null, chunk);
    },
  });

  const tmpFileStream = createWriteStream(tmpFilePath, { flags: 'w' });
  await pipeline(res.body, hashTransform, tmpFileStream);

  const extensionFileHash = extensionFileHasher.digest('hex').toLowerCase();
  if (asset.sha256 !== extensionFileHash) {
    throw new Error(
      `SHA-256 hash check failed for ${asset.fileName} downloaded from ${assetFileUrl}: ` +
        `expected ${asset.sha256}, found ${extensionFileHash}`);
  }

  await fs.rename(tmpFilePath, extensionFilePath);
  console.log('Download complete and SHA-256 hash check passed');
} finally {
  await fs.rm(tmpDirPath, { recursive: true, force: true });
}
