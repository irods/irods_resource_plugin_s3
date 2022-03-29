from .resource_suite_s3_nocache import Test_S3_NoCache_Base
from .resource_suite_s3_cache import Test_S3_Cache_Base

import psutil
import sys
import unittest

class Test_Compound_With_S3_Resource(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/minio.keypair'
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
        self.keypairfile='/var/lib/irods/minio.keypair'
        self.s3stsdate=''
        self.s3region='eu-central-1'
        self.s3endPoint='localhost:9001'
        super(Test_Compound_With_S3_Resource_EU_Central_1, self).__init__(*args, **kwargs)


class Test_S3_NoCache_V4(Test_S3_NoCache_Base, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/minio.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_V4, self).__init__(*args, **kwargs)

    # issue 2024
    @unittest.skip("File removal too slow with MinIO")
    def test_put_get_file_greater_than_8GiB_two_threads(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_8GiB_two_threads(self)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 4 * (4*1024*1024*1024 + 2), "not enough free space for four 4 GiB files (upload, download, and two on-disk MinIO)")
    def test_put_get_file_greater_than_4GiB_one_thread(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_4GiB_one_thread(self)

class Test_S3_NoCache_MPU_Disabled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/minio.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = 'localhost:9000'
        self.s3EnableMPU=0
        super(Test_S3_NoCache_MPU_Disabled, self).__init__(*args, **kwargs)

    # issue 2024
    @unittest.skip("File removal too slow with MinIO")
    def test_put_get_file_greater_than_8GiB_two_threads(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_8GiB_two_threads(self)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 4 * (4*1024*1024*1024 + 2), "not enough free space for four 4 GiB files (upload, download, and two on-disk MinIO)")
    def test_put_get_file_greater_than_4GiB_one_thread(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_4GiB_one_thread(self)

class Test_S3_NoCache_Decoupled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/minio.keypair'
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

    # issue 2024
    @unittest.skip("File removal too slow with MinIO")
    def test_put_get_file_greater_than_8GiB_two_threads(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_8GiB_two_threads(self)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 4 * (4*1024*1024*1024 + 2), "not enough free space for four 4 GiB files (upload, download, and two on-disk MinIO)")
    def test_put_get_file_greater_than_4GiB_one_thread(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_4GiB_one_thread(self)

class Test_S3_NoCache_EU_Central_1(Test_S3_NoCache_Base, unittest.TestCase):
    '''
    This also tests signature V4 with the x-amz-date header.
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.proto = 'HTTP'
        self.keypairfile='/var/lib/irods/minio.keypair'
        self.s3region='eu-central-1'
        self.s3endPoint='localhost:9001'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_EU_Central_1, self).__init__(*args, **kwargs)

    # issue 2024
    @unittest.skip("File removal too slow with MinIO")
    def test_put_get_file_greater_than_8GiB_two_threads(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_8GiB_two_threads(self)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 4 * (4*1024*1024*1024 + 2), "not enough free space for four 4 GiB files (upload, download, and two on-disk MinIO)")
    def test_put_get_file_greater_than_4GiB_one_thread(self):
        Test_S3_NoCache_Base.test_put_get_file_greater_than_4GiB_one_thread(self)
