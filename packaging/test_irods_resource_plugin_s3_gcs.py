from .resource_suite_s3_nocache import Test_S3_NoCache_Base
from .resource_suite_s3_cache import Test_S3_Cache_Base

import sys
import unittest

class Test_Compound_With_S3_Resource(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/google-gcs.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='us-east4'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/google_endpoint.txt')
        self.s3sse = 0 # server side encryption
        super(Test_Compound_With_S3_Resource, self).__init__(*args, **kwargs)

class Test_S3_NoCache_V4(Test_S3_NoCache_Base, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/google-gcs.keypair'
        self.s3region='us-east4'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/google_endpoint.txt')
        self.s3EnableMPU=1
        self.s3DisableCopyObject=1
        super(Test_S3_NoCache_V4, self).__init__(*args, **kwargs)

class Test_S3_NoCache_MPU_Disabled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/google-gcs.keypair'
        self.s3region='us-east4'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/google_endpoint.txt')
        self.s3EnableMPU=0
        super(Test_S3_NoCache_MPU_Disabled, self).__init__(*args, **kwargs)


class Test_S3_NoCache_Decoupled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/google-gcs.keypair'
        self.s3region='us-east4'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/google_endpoint.txt')
        self.s3EnableMPU=0
        self.archive_naming_policy = 'decoupled'
        super(Test_S3_NoCache_Decoupled, self).__init__(*args, **kwargs)

    @unittest.skipIf(True, 'test does not work in decoupled because we are using same bucket for multiple resources')
    def test_iget_with_stale_replica(self):  # formerly known as 'dirty'
        pass

    @unittest.skipIf(True, 'test does not work in decoupled because we are using same bucket for multiple resources')
    def test_irepl_with_purgec(self):
        pass

    @unittest.skipIf(True, 'test does not work in decoupled because we are using same bucket for multiple resources')
    def test_put_get_small_file_in_repl_node(self):
        pass

    @unittest.skipIf(True, 'test does not work in decoupled because we are using same bucket for multiple resources')
    def test_put_get_large_file_in_repl_node(self):
        pass


