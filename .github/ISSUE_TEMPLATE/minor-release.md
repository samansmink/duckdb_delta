---
name: Minor Release
about: Things to be done for every minor release.
title: ''
labels: ''
assignees: ''

---

# Minor release checklist `<version>`
This issue contains the list of things that we need to do every release. This list is kept as a issue template in the duckdblabs/duckdb-internal repository for easy reuse. When the template is used, each item on the list should be assigned one or multiple people by tagging their name behind it or creating a standalone internal issue that is linked behin the relevant item.

Specific instructions or details about steps in this list can be found in [update document](https://docs.google.com/document/d/1RzeVo4dgE-7omQvfW66vhiLTvF_70DoIFwLUG64Flro/edit?usp=sharing).

## `T - 1 Month` (replace with date)
- [ ] Announce feature freeze date to team

### Extensions
- [ ] Apply all [patches](https://github.com/duckdb/duckdb/tree/main/.github/patches/extensions) in duckdb/duckdb
- [ ] Bump all extensions to latest in the [config](https://github.com/duckdb/duckdb/blob/main/.github/config/out_of_tree_extensions.cmake) in duckdb/duckdb
- [ ] Have out-of-tree extensions point to main ensuring their extension-specific CI is run against main
- [ ] Review Extension C API struct for new additions
- [ ] Trigger [Build All](https://github.com/duckdb/community-extensions/blob/main/.github/workflows/build_all.yml) in duckdb/community-extensions and review failures. (optionally pinging relevant maintainers)

### Clients
- [ ] Bump DuckDB-WASM to main

### CI review
- [ ] Review CI failures on main, open issues for those to be fixed, tag with `<version>` milestone
- [ ] Review all issues, tag relevant issues with `<version>` milestone
- [ ] Review VCPKG version for bumping
- [ ] Review [Fuzzer issues](https://github.com/duckdb/duckdb-fuzzer/issues) and create internal issues and tag with `<version>` milestone where needed
- [ ] Check relevant benchmarks (Clickbench, H2OAI) for potential regressions/issues

## `T - 2 weeks`  (replace with date)
- [ ] Start feature freeze
- [ ] Create feature branch, optionally retarget open PR to feature
- [ ] Call [InvokeCI](https://github.com/duckdb/duckdb/blob/main/.github/workflows/InvokeCI.yml) workflow to ensure it works
- [ ] Stress test produced binaries, especially on cross compatibility issue: secrets / database compatibility / on the wire serialization, load old extension with new duckdb (or new [C] extensions with latest release duckdb) and ensure no crashes. Stress test autoloading (current tests are mostly in statically linked mode). Check versions to be correct in all code & binaries. Most of this could & should be automated, but still needs to be checked it's actually performed.
- [ ] Check produced extensions & binaries versus latest past release (they should be about the same number / size, differences should be accounted for).
- [ ] Review progress of `<version>` milestone issues
- [ ] Review hardcoded versions in duckdb library: C-API version, storage version, serialization version, etc. Bump as appropriate.
- [ ] Review the need to write update instructions for extension developers and add item to post release list

## `T - 1 week`  (replace with date)
- [ ] Call [InvokeCI](https://github.com/duckdb/duckdb/blob/main/.github/workflows/InvokeCI.yml) workflow for real and deploy to `extensions.duckdb.org/<version_tag>`
- [ ] Stress test produced binaries, especially on cross compatibility issue: secrets / database compatibility / on the wire serialization, load old extension with new duckdb (or new [C] extensions with latest release duckdb) and ensure no crashes. Stress test autoloading (current tests are mostly in statically linked mode). Check versions to be correct in all code & binaries. Most of this could & should be automated, but still needs to be checked it's actually performed.
- [ ] Check produced extensions & binaries versus latest past release (they should be about the same number / size, differences should be accounted for).
- [ ] Trigger [Build All](https://github.com/duckdb/community-extensions/blob/main/.github/workflows/build_all.yml) in duckdb/community-extensions and deploy to `community-extensions.duckdb.org/<version_tag>`
- [ ] Identify extensions that need to be released manually and add them to the relevant post-release item
- [ ] Open PR for documentation bump
- [ ] Prepare release blogpost

## `Release day` (replace with date)
- [ ] Tag duckdb/duckdb
- [ ] Publish release blog post
- [ ] Publish docs
- [ ] Add date of the release to the release calendar (`duckdb-web`)
- [ ] Add next release date to the release calendar (`duckdb-web`)
- [ ] Enjoy a cold beer!

## Post release
### Client releases
- [ ] Java
- [ ] WASM
- [ ] Node (Update vendored version, double check release hash!)
- [ ] Node-neo
- [ ] DuckDB-RS
- [ ] R (Kirill does this?, double-check release Hash!)
- [ ] Go
- [ ] Julia (and yggdrassil?)
- [ ] OBDC
- [ ] Swift (?)

### Extensions
- [ ] bump extension-ci-tools
- [ ] bump extension-template
- [ ] bump extension-template-rs
- [ ] bump extension-template-c
- [ ] bump all out-of-tree extensions to tagged release
- [ ] release any extension binaries that were not yet produced

### Socials
- [ ] Announce on Twitter, LinkedIn, Bluesky, Mastodon(?) Threads, Discord
- [ ] Update [DB Engines Ranking](https://db-engines.com/en/system/DuckDB) by emailing [office@db-engines.com](mailto:office@db-engines.com)
- [ ] Monitor Hacker News for ~24 hours after the release

### Others
- [ ] pg_duckdb
- [ ] pyodide
- [ ] schedule next releaser: https://duckdb.org/docs/dev/release_calendar
- [ ] review the release template issue for potential updates/changes
- [ ] create calendar item for the T - 1 month date (the start of next release)
- [ ] check homebrew update

### Benchmarks
- [ ] update h2oai
- [ ] update clickbench
