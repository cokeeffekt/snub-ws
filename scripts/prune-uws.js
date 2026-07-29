#!/usr/bin/env node
//
// Optional disk-space cleanup — not run automatically.
//
// snub-ws installs two pinned uWebSockets.js builds (uws-legacy, uws-modern) so it can load on
// any supported Node version. Each ships a prebuilt binary for every platform/arch/ABI combo,
// ~39 files and ~218 MB in total, of which a given machine can load exactly one. This deletes
// the binaries for other platforms, leaving every ABI for the current platform+arch so that
// switching Node versions still works without a reinstall.
//
// Intended for an image build step, after `npm ci` and before the runtime stage:
//     RUN npm ci --omit=dev && npx snub-ws-prune
//
// Do not run it if the same node_modules tree is reused across operating systems or CPU
// architectures — the binaries it removes are the ones those other platforms need.
//
// Usage: node scripts/prune-uws.js [--dry-run]

const fs = require('fs');
const path = require('path');

const dryRun = process.argv.includes('--dry-run');
const keepPrefix = `uws_${process.platform}_${process.arch}_`;

function locate(name) {
  try {
    // resolve() does not execute the module, so this works even for the build that
    // cannot load on this Node version — which is precisely the one worth pruning.
    return path.dirname(require.resolve(name));
  } catch {
    return null;
  }
}

let removed = 0;
let bytes = 0;
let kept = 0;

for (const name of ['uws-modern', 'uws-legacy']) {
  const dir = locate(name);
  if (!dir) {
    console.log(`${name}: not installed, skipping`);
    continue;
  }

  for (const file of fs.readdirSync(dir)) {
    if (!file.endsWith('.node')) continue;
    if (file.startsWith(keepPrefix)) {
      kept++;
      continue;
    }
    const full = path.join(dir, file);
    bytes += fs.statSync(full).size;
    if (!dryRun) fs.unlinkSync(full);
    removed++;
  }
  console.log(
    `${name}: ${dryRun ? 'would remove' : 'removed'} binaries from ${dir}`
  );
}

const mb = (bytes / 1024 / 1024).toFixed(0);
console.log(
  `${
    dryRun ? 'Would free' : 'Freed'
  } ${mb} MB — removed ${removed} binaries, kept ${kept} matching ${keepPrefix}*`
);

if (kept === 0) {
  console.error(
    `\nWARNING: no binaries matched ${keepPrefix}* — snub-ws will not load on this platform.`
  );
  process.exitCode = 1;
}
