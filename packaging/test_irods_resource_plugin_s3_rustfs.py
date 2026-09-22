from .resource_suite_s3_nocache import Test_S3_NoCache_Base
from .resource_suite_s3_nocache import Test_S3_NoCache_Large_File_Tests_Base
from .resource_suite_s3_nocache import Test_S3_NoCache_MPU_Disabled_Base
from .resource_suite_s3_cache import Test_S3_Cache_Base

import psutil
import shutil
import subprocess
import sys
import unittest

from ..configuration import IrodsConfig

IRODS_SUPPORTS_CRC64NVME = IrodsConfig().version_tuple > (5, 0, 2)

def _get_rustfs_version():
    """Get the RustFS server version by running the rustfs binary.

    Searches for the rustfs binary in PATH first, then falls back to /rustfs
    (used when the binary is downloaded directly rather than installed as a package).

    Trailing checksum (CRC64NVME) support was confirmed against RustFS's first stable
    release (1.0.0), which is also the earliest release available, so unlike the MinIO-based
    check this only confirms a working rustfs binary is present rather than comparing against
    a minimum version. If a RustFS release is later found to have broken trailing checksum
    support, add a version floor here similar to the MinIO check.

    Returns a tuple of (version_string_or_None, error_string_or_None).
    """
    rustfs_path = shutil.which('rustfs') or '/rustfs'
    try:
        result = subprocess.run(
            [rustfs_path, '--version'],
            capture_output=True, text=True, timeout=5
        )
        output = (result.stdout + result.stderr).strip()
        if result.returncode == 0 and output:
            return output, None
        return None, f'Unexpected output running [{rustfs_path} --version]: [{output}]'
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return None, f'Failed to run [{rustfs_path}]: {e}'

class Test_Compound_With_S3_Resource(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3sse = 0 # server side encryption
        super(Test_Compound_With_S3_Resource, self).__init__(*args, **kwargs)

class Test_Compound_With_S3_Resource_EU_Central_1(Test_S3_Cache_Base, unittest.TestCase):
    '''
    This also tests signature V4 with the x-amz-date header.
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3stsdate=''
        self.s3region='eu-central-1'
        self.s3endPoint='localhost:9001'
        super(Test_Compound_With_S3_Resource_EU_Central_1, self).__init__(*args, **kwargs)


class Test_S3_NoCache_V4(Test_S3_NoCache_Large_File_Tests_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_V4, self).__init__(*args, **kwargs)

    # issue 2024
    @unittest.skip("File removal too slow with MinIO")
    def test_put_get_file_greater_than_8GiB_two_threads(self):
        Test_S3_NoCache_Large_File_Tests_Base.test_put_get_file_greater_than_8GiB_two_threads(self)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 4 * (4*1024*1024*1024 + 2), "not enough free space for four 4 GiB files (upload, download, and two on-disk MinIO)")
    def test_put_get_file_greater_than_4GiB_one_thread(self):
        Test_S3_NoCache_Large_File_Tests_Base.test_put_get_file_greater_than_4GiB_one_thread(self)

class Test_S3_NoCache_MPU_Disabled(Test_S3_NoCache_MPU_Disabled_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=0
        super(Test_S3_NoCache_MPU_Disabled, self).__init__(*args, **kwargs)

class Test_S3_NoCache_Decoupled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=1
        self.archive_naming_policy = 'decoupled'
        super(Test_S3_NoCache_Decoupled, self).__init__(*args, **kwargs)

    @unittest.skip('test does not work in decoupled because we are using same bucket for multiple resources')
    def test_iget_with_stale_replica(self):  # formerly known as 'dirty'
        pass

    @unittest.skip('test does not work in decoupled because we are using same bucket for multiple resources')
    def test_irepl_with_purgec(self):
        pass

    @unittest.skip('test does not work in decoupled because we are using same bucket for multiple resources')
    def test_put_get_small_file_in_repl_node(self):
        pass

    @unittest.skip('test does not work in decoupled because we are using same bucket for multiple resources')
    def test_put_get_large_file_in_repl_node(self):
        pass

    @unittest.skip('test does not work in decoupled because we are using same bucket for multiple resources')
    def test_s3_in_replication_node__issues_2102_2122(self):
        pass

class Test_S3_NoCache_EU_Central_1(Test_S3_NoCache_Base, unittest.TestCase):
    '''
    This also tests signature V4 with the x-amz-date header.
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3region='eu-central-1'
        self.s3endPoint='localhost:9001'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_EU_Central_1, self).__init__(*args, **kwargs)

_rustfs_version, _rustfs_version_error = _get_rustfs_version()
@unittest.skipUnless(IRODS_SUPPORTS_CRC64NVME, 'iRODS server must support CRC64NVME')
@unittest.skipUnless(_rustfs_version is not None, f'Could not confirm a working rustfs binary for trailing checksum support.  Error: {_rustfs_version_error}.')
class Test_S3_NoCache_Trailing_Checksum(Test_S3_NoCache_Large_File_Tests_Base, unittest.TestCase):
    '''
    Tests S3 uploads with trailing checksums enabled (CRC64/NVME).
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/rustfs.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=1
        self.s3EnableTrailingChecksumOnUpload=1
        self.s3EnableDirectChecksumRead=1
        super(Test_S3_NoCache_Trailing_Checksum, self).__init__(*args, **kwargs)
