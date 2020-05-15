from resource_suite_s3_nocache import Test_S3_NoCache_Base
from resource_suite_s3_cache import Test_S3_Cache_Base

import sys
if sys.version_info >= (2,7):
    import unittest
else:
    import unittest2 as unittest

class Test_Compound_With_S3_Resource(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/ceph_s3_key.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='us-east-1'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/ceph_endpoint.txt')
        self.s3sse = 0 # server side encryption
        super(Test_Compound_With_S3_Resource, self).__init__(*args, **kwargs)

class Test_S3_NoCache_V4(Test_S3_NoCache_Base, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/ceph_s3_key.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/ceph_endpoint.txt')
        self.s3EnableMPU=1
        super(Test_S3_NoCache_V4, self).__init__(*args, **kwargs)

class Test_S3_NoCache_MPU_Disabled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/var/lib/irods/ceph_s3_key.keypair'
        self.s3region='us-east-1'
        self.s3endPoint = self.read_endpoint('/var/lib/irods/ceph_endpoint.txt')
        self.s3EnableMPU=0
        super(Test_S3_NoCache_MPU_Disabled, self).__init__(*args, **kwargs)
