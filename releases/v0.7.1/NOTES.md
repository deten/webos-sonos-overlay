# v0.7.1

- Stamp the version from package.json into the packaged appinfo.json and the ipk
  control file. Both were pinned at 1.0.0, so an installed copy reported a higher
  version than the repository manifest and Homebrew Channel would never have
  offered an update.
