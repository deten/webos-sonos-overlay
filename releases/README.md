# Releases

One folder per deployed build, named `v<version>` after the `version` field in
`package.json`. Each holds the exact artifacts that were installed on the TV:

| File | What it is |
|---|---|
| `tv-service.bundle.js` | The bundled background service |
| `com.brineandbuild.sonosoverlay.ipk` | Installable package: overlay, setup app, and service |
| `manifest.json` | Version, build timestamp, source commit, and a SHA-256 per file |
| `NOTES.md` | What changed in this build |

To cut a new one, bump `version` in `package.json` and run `npm run release`.
It refuses to overwrite an existing folder unless given `--force`.

These folders are snapshots, not build inputs — nothing reads from them. Delete
any of them whenever they stop being useful.

## Rolling back

Copy the older `tv-service.bundle.js` to the TV in place of the current one and
restart the service, or install that folder's `.ipk`.
