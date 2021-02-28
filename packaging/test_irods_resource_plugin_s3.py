try:
   from minio import Minio
   from minio.error import ResponseError
except ImportError:
   print('This test requires minio: perhaps try pip install minio')
   exit()

import commands
import datetime
import os
import platform
import random
import re
import shutil
import string
import subprocess
import urllib3

from resource_suite_s3_nocache import Test_S3_NoCache_Base
from resource_suite_s3_cache import Test_S3_Cache_Base

import sys
if sys.version_info >= (2,7):
    import unittest
else:
    import unittest2 as unittest

from .. import lib
from . import session
from ..configuration import IrodsConfig
from .resource_suite import ResourceSuite
from .test_chunkydevtest import ChunkyDevTest


class Test_Compound_With_S3_Resource(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        self.s3sse = 0 # server side encryption
        super(Test_Compound_With_S3_Resource, self).__init__(*args, **kwargs)

class Test_Compound_With_S3_Resource_EU_Central_1(Test_S3_Cache_Base, unittest.TestCase):
    '''
    This also tests signature V4 with the x-amz-date header.
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='eu-central-1'
        self.s3endPoint='s3.eu-central-1.amazonaws.com'
        super(Test_Compound_With_S3_Resource_EU_Central_1, self).__init__(*args, **kwargs)

class Test_Compound_With_S3_Resource_STSDate_Header(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate='date'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        super(Test_Compound_With_S3_Resource_STSDate_Header, self).__init__(*args, **kwargs)

class Test_Compound_With_S3_Resource_STSDate_Header_V4(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate='date'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        super(Test_Compound_With_S3_Resource_STSDate_Header_V4, self).__init__(*args, **kwargs)

class Test_Compound_With_S3_Resource_V4_SSE(Test_S3_Cache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3sse=1
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        super(Test_Compound_With_S3_Resource_V4_SSE, self).__init__(*args, **kwargs)

class Test_S3_NoCache_V4(Test_S3_NoCache_Base, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_V4, self).__init__(*args, **kwargs)

class Test_S3_NoCache_EU_Central_1(Test_S3_NoCache_Base, unittest.TestCase):
    '''
    This also tests signature V4 with the x-amz-date header.
    '''
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.s3region='eu-central-1'
        self.s3endPoint='s3.eu-central-1.amazonaws.com'
        self.s3EnableMPU=1
        super(Test_S3_NoCache_EU_Central_1, self).__init__(*args, **kwargs)

class Test_S3_NoCache_MPU_Disabled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        self.s3EnableMPU=0
        super(Test_S3_NoCache_MPU_Disabled, self).__init__(*args, **kwargs)

class Test_S3_NoCache_SSE(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        self.s3sse=1
        self.s3EnableMPU=1
        super(Test_S3_NoCache_SSE, self).__init__(*args, **kwargs)

class Test_S3_NoCache_Decoupled(Test_S3_NoCache_Base, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Set up the test."""
        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
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


