# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **only** adheres to the following _(as defined at [Semantic Versioning](https://semver.org/spec/v2.0.0.html))_:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> - MAJOR version when you make incompatible API changes
> - MINOR version when you add functionality in a backward compatible manner
> - PATCH version when you make backward compatible bug fixes

## [5.1.0] - 2026-06-22

This release implements support for trailing checksums, the ability to read checksums directly from the storage device, and fixes bugs for archive mode and cache file usage.

### Fixed

- Fix build by conditionally using `CRC64NVME_CHKSUM_PREFIX` (#2275).
- Fix incorrect determination of thread count (#2295).
- Prevent CREATE operation for archive mode (irods/irods#8502).

### Added

- Add build hook option for compiling against specific version of release package (irods/irods_development_environment#165).
- Implement GetObjectAttributes API (#2271).
- Add support for reading checksums from storage device (#2271).
- Add build hook option for compiling in debug mode (#2282).
- Add support for sending CRC64/NVME checksum with uploads (#2284, #2285).
