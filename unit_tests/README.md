# S3 Transport Unit Tests

Unit tests for the iRODS S3 transport layer using the Catch2 framework.

## Prerequisites

- iRODS development packages installed
- Catch2 (2.13.2+ or 3.4+)
- fmt library
- Boost (system, filesystem, thread)
- AWS CLI (`aws`) in PATH and configured (`aws configure`)
- Access to an S3-compatible endpoint (AWS S3 or MinIO)

## Create Test Data Files

The tests require data files of various sizes. Run the provided script to generate them:

```bash
cd unit_tests
./create_unit_test_files.sh build
```

This downloads a public domain text and creates four files in `build/`:

| File | Size | Description |
|------|------|-------------|
| `zero_file` | 0 B | Empty file |
| `small_file` | 148 B | Small text excerpt |
| `medium_file` | ~13 MB | Text repeated 4x |
| `large_file` | ~410 MB | Text doubled 7x |

## Build

The unit tests are built as part of the top-level CMake build with the `IRODS_UNIT_TESTS_BUILD` flag enabled:

```bash
mkdir -p build
cd build
cmake .. -DIRODS_UNIT_TESTS_BUILD=YES
make -j4
```

The test executable is built at `build/unit_tests/irods_s3_transport`.

> [!NOTE]
> The unit tests cannot be built standalone from the `unit_tests/` directory because they depend on object libraries (`s3_transport_obj`, `libs3_obj`) defined in the top-level build.

## Configuration

### Keyfile

Create a keyfile with two lines containing your S3 credentials:

```
<access_key>
<secret_access_key>
```

### S3 Endpoint

The tests default to `s3.amazonaws.com`. For RustFS or other S3-compatible endpoints, use the `--hostname` option.

### Options for Endpoints Other Than AWS S3

| Option | Description |
|--------|-------------|
| `--skip-https-tests` | Skip tests that use `https`. Use this when the endpoint only serves plain HTTP. |
| `--force-amz-date` | Send the `x-amz-date` header instead of the `Date` header (`S3_STSDATE=amz` instead of `date`) in tests that would otherwise use `S3_STSDATE=date`. Use this for endpoints that require the `x-amz-date` header (e.g. RustFS). |

The server-side encryption test ("upload medium file as single part with server encrypt") requires the endpoint to support SSE-S3. For RustFS without a KMS configured, start the server with a master key set, otherwise the upload is rejected with `InvalidRequest`:

```bash
export RUSTFS_SSE_S3_MASTER_KEY=$(head -c32 /dev/urandom | base64)
rustfs server ...
```

## Run Tests

From the top-level `build/` directory:

```bash
# Run all tests against MinIO
./unit_tests/irods_s3_transport --hostname localhost:9000 --keyfile /path/to/keyfile

# Run all tests against AWS S3
./unit_tests/irods_s3_transport --keyfile /path/to/keyfile

# Run all tests against a local plain-HTTP RustFS server
./unit_tests/irods_s3_transport --hostname localhost:9000 --keyfile /path/to/keyfile --skip-https-tests --force-amz-date
```

### Run Specific Test Tags

```bash
# Quick tests
./unit_tests/irods_s3_transport --hostname localhost:9000 --keyfile /path/to/keyfile "[quick_test]"

# Upload tests with trailing checksum
./unit_tests/irods_s3_transport --hostname localhost:9000 --keyfile /path/to/keyfile "[trailing_checksum]"

# Part split calculation tests (no S3 access needed)
./unit_tests/irods_s3_transport --hostname localhost:9000 --keyfile /path/to/keyfile "[part_splits]"
```

### Environment Variables

As an alternative to command-line options:

```bash
export S3_TEST_KEYFILE=/path/to/keyfile
export S3_TEST_HOSTNAME=localhost:9000
./unit_tests/irods_s3_transport
```

### Other Catch2 Options

```bash
# List all available tests
./unit_tests/irods_s3_transport --list-tests

# List all available tags
./unit_tests/irods_s3_transport --list-tags

# Verbose output
./unit_tests/irods_s3_transport -s
```

## Available Test Tags

| Tag | Description |
|-----|-------------|
| `[quick_test]` | Quick upload/download smoke tests |
| `[upload][thread]` | Multi-threaded upload tests |
| `[download][thread]` | Multi-threaded download tests |
| `[upload_process][process]` | Multi-process upload tests |
| `[download_process][process]` | Multi-process download tests |
| `[trailing_checksum]` | CRC64/NVME trailing checksum upload test |
| `[single_part]` | Single-part upload test |
| `[minimum_part_size]` | Upload with minimum part size |
| `[rw][thread]` | Read/write thread tests |
| `[seek_end]` | Seek-to-end position tests |
| `[part_splits]` | Multipart split calculation tests |
| `[shmem2]` | Shared memory tests |
