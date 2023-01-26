from __future__ import print_function

try:
    from minio import Minio
except ImportError:
    print('This test requires minio: perhaps try pip install minio')
    exit()

try:
    from minio.error import InvalidResponseError as ResponseError
except ImportError:
    try:
        from minio.error import ResponseError
    except ImportError:
        print('Failed to import InvalidResponseError or ResponseError')
        exit()

import datetime
import os
import platform
import random
import re
import shutil
import string
import subprocess
import urllib3
import distro

from .resource_suite_s3_nocache import Test_S3_NoCache_Base

import sys
if sys.version_info >= (2,7):
    import unittest
else:
    import unittest2 as unittest

from .. import lib
from . import s3plugin_lib
from . import session
from ..configuration import IrodsConfig
from .resource_suite import ResourceSuite
from .test_chunkydevtest import ChunkyDevTest

class Test_S3_Cache_Base(ResourceSuite, ChunkyDevTest):
    def __init__(self, *args, **kwargs):
        """Set up the cache test."""
        # if self.proto is defined use it else default to HTTPS

        if not hasattr(self, 'proto'):
            self.proto = 'HTTPS'

        # if self.archive_naming_policy is defined use it
        # else default to 'consistent'
        if not hasattr(self, 'archive_naming_policy'):
            self.archive_naming_policy = 'consistent'

        super(Test_S3_Cache_Base, self).__init__(*args, **kwargs)

    def setUp(self):
        # set up aws configuration
        self.read_aws_keys()

        # set up s3 bucket
        try:
            httpClient = urllib3.poolmanager.ProxyManager(
                os.environ['http_proxy'],
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                cert_reqs='CERT_REQUIRED',
                retries=urllib3.Retry(
                    total=5,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
        except KeyError:
            httpClient = None

        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                http_client=httpClient,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
            distro_str = '{}-{}'.format(distro.id(), distro.version()).replace(' ', '').replace('.', '')
            self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d%H-%M-%S-%f-')
            self.s3bucketname += ''.join(random.choice(string.ascii_letters) for i in range(10))
            self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
            s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        # set up resources

        hostname = lib.get_hostname()
        s3params = 'S3_RETRY_COUNT=15;S3_WAIT_TIME_SECONDS=1;S3_PROTO=%s;S3_MPU_CHUNK=10;S3_MPU_THREADS=4;S3_ENABLE_MD5=1' % self.proto
        s3params += ';S3_STSDATE=' + self.s3stsdate
        s3params += ';S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        s3params += ';S3_AUTH_FILE=' +  self.keypairfile
        s3params += ';S3_REGIONNAME=' + self.s3region
        s3params += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        if hasattr(self, 's3sse'):
            s3params += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)

        s3params=os.environ.get('S3PARAMS', s3params);

        with session.make_session_for_existing_admin() as admin_session:
            irods_config = IrodsConfig()
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            admin_session.assert_icommand("iadmin mkresc demoResc compound", 'STDOUT_SINGLELINE', 'compound')
            admin_session.assert_icommand("iadmin mkresc cacheResc 'unixfilesystem' " + hostname + ":" + irods_config.irods_directory + "/cacheRescVault", 'STDOUT_SINGLELINE', 'cacheResc')
            admin_session.assert_icommand('iadmin mkresc archiveResc s3 '+hostname+':/'+self.s3bucketname+'/irods/Vault "'+s3params+'"', 'STDOUT_SINGLELINE', 'archiveResc')
            admin_session.assert_icommand("iadmin addchildtoresc demoResc cacheResc cache")
            admin_session.assert_icommand("iadmin addchildtoresc demoResc archiveResc archive")

        super(Test_S3_Cache_Base, self).setUp()

    def tearDown(self):
        super(Test_S3_Cache_Base, self).tearDown()

        # delete s3 bucket
        try:
            httpClient = urllib3.poolmanager.ProxyManager(
                os.environ['http_proxy'],
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                cert_reqs='CERT_REQUIRED',
                retries=urllib3.Retry(
                    total=5,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
        except KeyError:
            httpClient = None

        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                http_client=httpClient,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        objects = s3_client.list_objects(self.s3bucketname, recursive=True)

        if not hasattr(self, 'static_bucket_name'):
            s3_client.remove_bucket(self.s3bucketname)

        # tear down resources
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc archiveResc")
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc cacheResc")
            admin_session.assert_icommand("iadmin rmresc archiveResc")
            admin_session.assert_icommand("iadmin rmresc cacheResc")
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

        shutil.rmtree(IrodsConfig().irods_directory + "/cacheRescVault", ignore_errors=True)

    def read_aws_keys(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            self.aws_access_key_id = f.readline().rstrip()
            self.aws_secret_access_key = f.readline().rstrip()

    # read the endpoint address from the file endpointfile
    @staticmethod
    def read_endpoint(endpointfile):
        # read endpoint file
        with open(endpointfile) as f:
            return f.readline().rstrip()

    def test_irm_specific_replica(self):
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+self.testfile) # creates replica
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed twice
        self.admin.assert_icommand("itrim -N 1 -n 0 " + self.testfile, 'STDOUT')  # remove original from cacheResc only
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',["2 "+self.testresc,self.testfile]) # replica 2 should still be there
        self.admin.assert_icommand_fail("ils -L "+self.testfile,'STDOUT_SINGLELINE',["0 "+self.admin.default_resource,self.testfile]) # replica 0 should be gone
        trashpath = self.admin.session_collection_trash
        self.admin.assert_icommand_fail("ils -L "+trashpath+"/"+self.testfile,'STDOUT_SINGLELINE',["0 "+self.admin.default_resource,self.testfile]) # replica should not be in trash

    @unittest.skip("--wlock has possible race condition due to Compound/Replication PDMO")
    def test_local_iput_collision_with_wlock(self):
        pass

    @unittest.skip("NOTSURE / FIXME ... -K not supported, perhaps")
    def test_local_iput_checksum(self):
        pass

    @unittest.skip("EMPTY_RESC_PATH - no vault path for coordinating resources")
    def test_ireg_as_rodsuser_in_vault(self):
        pass

    @unittest.skip("No Vault for S3 archive resource")
    def test_iput_overwrite_others_file__ticket_2086(self):
        pass

    def test_local_iput_with_force_and_destination_resource__ticket_1706(self):
        # local setup
        filename = "iputwithforceanddestination.txt"
        filepath = lib.create_local_testfile(filename)
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        self.admin.assert_icommand("ils -L "+filename,'STDERR_SINGLELINE',"does not exist")                           # should not be listed
        self.admin.assert_icommand("iput "+filename)                                                      # put file
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)                                # replicate to test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)                                    #
        self.admin.assert_icommand("iput -f -R %s %s %s" % (self.testresc, doublefile, filename) )        # overwrite test repl with different data
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," "+filename])                        # default resource cache should have dirty copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," "+filename])                        # default resource archive should have dirty copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," "+doublesize+" "," "+filename]) # default resource cache should not have doublesize file
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," "+doublesize+" "," "+filename]) # default resource archive should not have doublesize file
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "," "+doublesize+" ","& "+filename])    # targeted resource should have new double clean copy
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    ###################
    # irepl
    ###################

    def test_irepl_update_replicas(self):
        # local setup
        filename = "updatereplicasfile.txt"
        filepath = lib.create_local_testfile(filename)
        hostname = lib.get_hostname()
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)

        # assertions
        self.admin.assert_icommand("iadmin mkresc thirdresc unixfilesystem %s:/tmp/thirdrescVault" % hostname, 'STDOUT_SINGLELINE', "Creating")   # create third resource
        self.admin.assert_icommand("iadmin mkresc fourthresc unixfilesystem %s:/tmp/fourthrescVault" % hostname, 'STDOUT_SINGLELINE', "Creating") # create fourth resource
        self.admin.assert_icommand("ils -L "+filename,'STDERR_SINGLELINE',"does not exist")              # should not be listed
        self.admin.assert_icommand("iput "+filename)                                         # put file
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)                   # replicate to test resource
        self.admin.assert_icommand("irepl -R thirdresc "+filename)                           # replicate to third resource
        self.admin.assert_icommand("irepl -R fourthresc "+filename)                          # replicate to fourth resource
        self.admin.assert_icommand("iput -f -R "+self.testresc+" "+doublefile+" "+filename)  # repave overtop test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)                       # for debugging

        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 4 "," & "+filename]) # should have a dirty copy

        self.admin.assert_icommand(['irepl', filename])                # update replica on default resource

        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," & "+filename]) # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," & "+filename]) # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 4 "," & "+filename])     # should have a dirty copy

        self.admin.assert_icommand("irepl -aU "+filename)                                # update all replicas

        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 4 "," & "+filename])     # should have a clean copy

        self.admin.assert_icommand("irm -f "+filename)                                   # cleanup file
        self.admin.assert_icommand("iadmin rmresc thirdresc")                            # remove third resource
        self.admin.assert_icommand("iadmin rmresc fourthresc")                           # remove third resource

        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    def test_irepl_over_existing_second_replica__ticket_1705(self):
        # local setup
        filename = "secondreplicatest.txt"
        filepath = lib.create_local_testfile(filename)
        # assertions
        self.admin.assert_icommand("ils -L "+filename,'STDERR_SINGLELINE',"does not exist")          # should not be listed
        self.admin.assert_icommand("iput -R "+self.testresc+" "+filename)                # put file
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)                   # for debugging
        self.admin.assert_icommand("irepl "+filename)                                    # replicate to default resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)                   # for debugging
        self.admin.assert_icommand(['irepl', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop default resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should not have a replica 3
        self.admin.assert_icommand(['irepl', '-R', self.testresc, filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop test resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should not have a replica 3
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_third_replica__ticket_1705(self):
        # local setup
        filename = "thirdreplicatest.txt"
        filepath = lib.create_local_testfile(filename)
        hostname = lib.get_hostname()
        # assertions
        self.admin.assert_icommand("iadmin mkresc thirdresc unixfilesystem %s:/tmp/thirdrescVault" % hostname, 'STDOUT_SINGLELINE', "Creating") # create third resource
        self.admin.assert_icommand("ils -L "+filename,'STDERR_SINGLELINE',"does not exist") # should not be listed
        self.admin.assert_icommand("iput "+filename)                            # put file
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)      # replicate to test resource
        self.admin.assert_icommand("irepl -R thirdresc "+filename)              # replicate to third resource
        self.admin.assert_icommand(['irepl', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop default resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand(['irepl', '-R', self.testresc, filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand(['irepl', '-R', 'thirdresc', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop third resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 4 "," & "+filename]) # should not have a replica 4
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 5 "," & "+filename]) # should not have a replica 5
        self.admin.assert_icommand("irm -f "+filename)                          # cleanup file
        self.admin.assert_icommand("iadmin rmresc thirdresc")                   # remove third resource
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_bad_replica__ticket_1705(self):
        # local setup
        filename = "reploverwritebad.txt"
        filepath = lib.create_local_testfile(filename)
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)                            # put file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)      # replicate to test resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        # overwrite default repl with different data
        self.admin.assert_icommand("iput -f %s %s" % (doublefile, filename))
        # default resource cache should have clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " & " + filename])
        # default resource cache should have new double clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " " + doublesize + " ", " & " + filename])
        # default resource archive should have clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " & " + filename])
        # default resource archive should have new double clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " " + doublesize + " ", " & " + filename])
        # test resource should not have doublesize file
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE',
                                        [" 2 " + self.testresc, " " + doublesize + " ", "  " + filename])
        # replicate back onto test resource
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)
        # test resource should have new clean doublesize file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE',
                                   [" 2 " + self.testresc, " " + doublesize + " ", " & " + filename])
        # should not have a replica 3
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 3 ", " & " + filename])
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    def test_iput_with_purgec(self):
        # local setup
        filename = "purgecfile.txt"
        filepath = os.path.abspath(filename)
        with open(filepath, 'wt') as f:
            print("TESTFILE -- [" + filepath + "]", file=f, end='')

        try:
            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', filename)  # should not be listed
            self.admin.assert_icommand("iput --purgec " + filename)  # put file
            # should not be listed (trimmed)
            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])
            # should be listed once - replica 1
            self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", filename])
            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", filename])  # should be listed only once
            self.admin.assert_icommand(['irm', '-f', filename])

            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', filename)  # should not be listed
            self.admin.assert_icommand(['iput', '-b', '--purgec', filename])  # put file... in bulk!
            # should not be listed (trimmed)
            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])
            # should be listed once - replica 1
            self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", filename])
            self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", filename])  # should be listed only once

        finally:
            s3plugin_lib.remove_if_exists(filepath)

    def test_iget_with_purgec(self):
        # local setup
        filename = "purgecgetfile.txt"
        filepath = os.path.abspath(filename)
        f = open(filepath,'w')
        f.write("TESTFILE -- ["+filepath+"]")
        f.close()

        # assertions
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        self.admin.assert_icommand("iput "+filename) # put file
        self.admin.assert_icommand("iget -f --purgec "+filename) # get file and purge 'cached' replica
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed once
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should not be listed

        # local cleanup
        output = subprocess.getstatusoutput( 'rm '+filepath )

    def test_irepl_with_purgec(self):
        # local setup
        filename = "purgecreplfile.txt"
        filepath = os.path.abspath(filename)
        f = open(filepath,'w')
        f.write("TESTFILE -- ["+filepath+"]")
        f.close()

        # assertions
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        self.admin.assert_icommand("iput "+filename) # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " --purgec " + filename)  # replicate to test resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed twice - 2 of 3
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should be listed twice - 1 of 3

        # local cleanup
        output = subprocess.getstatusoutput( 'rm '+filepath )

    def test_decoupled_naming_policy(self):
        if self.archive_naming_policy != 'decoupled':
            self.skipTest("Archive naming policy is not set to 'decoupled'")

        # local setup
        filename = self.testfile

        # run as regular user
        session = self.user0
        collection = session.session_collection

        # iquest to get the object id of the replica on the S3 archive
        id_query = ( "select DATA_ID where COLL_NAME =" + "'" + collection + "'" +
                       " and DATA_NAME =" + "'" + filename + "'" +
                       " and DATA_REPL_NUM ='1'" )

        # iquest to get the pysical path of the replica on the S3 archive
        path_query = ( "select DATA_PATH where COLL_NAME =" + "'" + collection + "'" +
                       " and DATA_NAME =" + "'" + filename + "'" +
                       " and DATA_REPL_NUM ='1'" )

        # assertions
        session.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        session.assert_icommand("iput "+filename) # put file

        # get object id
        object_id = session.run_icommand('iquest "%s" ' + '"' + id_query + '"')[0].strip()

        # physical path we expect to see: /{bucket_name}/{reversed_id}/{obj_name}
        target_path = '/' + self.s3bucketname + '/' + object_id[::-1] + '/' + filename

        # get object path
        physical_path = session.run_icommand('iquest "%s" ' + '"' + path_query + '"')[0].strip()

        # verify object path
        self.assertEqual(target_path, physical_path)

        # move the file
        new_filename = "%s.new" % filename
        session.assert_icommand("imv %s %s" % (filename, new_filename))

        # get and purge cache replica
        session.assert_icommand("iget -f --purgec %s" % new_filename) # get file and purge 'cached' replica

        # get again now that it is not in cache
        session.assert_icommand("iget -f %s" % new_filename) # get file

        # cleanup
        session.run_icommand('irm -f ' + new_filename)

    def test_decoupled_naming_policy_issue1855(self):
        if self.archive_naming_policy != 'decoupled':
            self.skipTest("Archive naming policy is not set to 'decoupled'")

        # local setup
        filename = self.testfile

        # run as regular user
        session = self.user0
        collection = session.session_collection

        # modify the s3 archive resource so that it only has the bucket name in the context
        self.admin.assert_icommand('iadmin modresc archiveResc path /%s' % self.s3bucketname, 'STDOUT_SINGLELINE', 'Previous resource path:')

        # iquest to get the object id of the replica on the S3 archive
        id_query = ( "select DATA_ID where COLL_NAME =" + "'" + collection + "'" +
                       " and DATA_NAME =" + "'" + filename + "'" +
                       " and DATA_REPL_NUM ='1'" )

        # iquest to get the pysical path of the replica on the S3 archive
        path_query = ( "select DATA_PATH where COLL_NAME =" + "'" + collection + "'" +
                       " and DATA_NAME =" + "'" + filename + "'" +
                       " and DATA_REPL_NUM ='1'" )

        # assertions
        session.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        session.assert_icommand("iput "+filename) # put file

        # get object id
        object_id = session.run_icommand('iquest "%s" ' + '"' + id_query + '"')[0].strip()

        # physical path we expect to see: /{bucket_name}/{reversed_id}/{obj_name}
        target_path = '/' + self.s3bucketname + '/' + object_id[::-1] + '/' + filename

        # get object path
        physical_path = session.run_icommand('iquest "%s" ' + '"' + path_query + '"')[0].strip()

        # verify object path
        self.assertEqual(target_path, physical_path)

        # move the file
        new_filename = "%s.new" % filename
        session.assert_icommand("imv %s %s" % (filename, new_filename))

        # get and purge cache replica
        session.assert_icommand("iget -f --purgec %s" % new_filename) # get file and purge 'cached' replica

        # get again now that it is not in cache
        session.assert_icommand("iget -f %s" % new_filename) # get file

        # cleanup
        session.run_icommand('irm -f ' + filename)

    @unittest.skip("skip until minio added to CI")
    def test_multiple_s3_endpoints_replication_issue1858(self):

        # local setup
        filename = self.testfile

        # run as regular user
        session = self.user0
        collection = session.session_collection

        # set up resources

        # TODO change these as necessary
        minio_auth_file = '/var/lib/irods/s3.keypair'
        minio_bucket_name = 'irods-bucket'

        hostname = lib.get_hostname()
        s3params_aws = 'S3_RETRY_COUNT=1;S3_WAIT_TIME_SECONDS=1;S3_PROTO=%s;S3_MPU_CHUNK=10;S3_MPU_THREADS=4;S3_ENABLE_MD5=1' % self.proto
        s3params_aws += ';S3_DEFAULT_HOSTNAME=%s' % self.s3endPoint
        s3params_aws += ';S3_AUTH_FILE=%s' % self.keypairfile
        s3params_aws += ';S3_REGIONNAME=%s' % self.s3region
        s3params_aws += ';ARCHIVE_NAMING_POLICY=%s' % self.archive_naming_policy

        s3params_minio = 'S3_RETRY_COUNT=1;S3_WAIT_TIME_SECONDS=1;S3_PROTO=%s;S3_MPU_CHUNK=10;S3_MPU_THREADS=4;S3_ENABLE_MD5=1' % self.proto
        s3params_minio += ';S3_DEFAULT_HOSTNAME=%s:9000' % hostname
        s3params_minio += ';S3_AUTH_FILE=%s' % minio_auth_file
        s3params_minio += ';S3_REGIONNAME=%s' % self.s3region
        s3params_minio += ';ARCHIVE_NAMING_POLICY=%s' % self.archive_naming_policy

        try:

            # make resource tree with repl and two compound resources underneath
            self.admin.assert_icommand('iadmin mkresc s3repl_1858 replication', 'STDOUT_SINGLELINE', 'Creating')
            self.admin.assert_icommand('iadmin mkresc s3compound1_1858 compound', 'STDOUT_SINGLELINE', 'Creating')
            self.admin.assert_icommand('iadmin mkresc s3compound2_1858 compound', 'STDOUT_SINGLELINE', 'Creating')
            self.admin.assert_icommand('iadmin mkresc s3cache1_1858 unixfilesystem %s:/tmp/s3cache1_1858 unixfilesystem' % hostname, 'STDOUT_SINGLELINE', 'Creating')
            self.admin.assert_icommand('iadmin mkresc s3archive1_1858 s3 %s:/%s/irods/Vault %s' % (hostname, self.s3bucketname, s3params_aws), 'STDOUT_SINGLELINE', 's3archive1_1858')
            self.admin.assert_icommand('iadmin mkresc s3cache2_1858 unixfilesystem %s:/tmp/s3cache2_1858 unixfilesystem' % hostname, 'STDOUT_SINGLELINE', 'Creating')
            self.admin.assert_icommand('iadmin mkresc s3archive2_1858 s3 %s:/%s/irods/s3archive2_1858_vault %s' % (hostname, minio_bucket_name, s3params_minio), 'STDOUT_SINGLELINE', 's3archive2_1858')
            self.admin.assert_icommand('iadmin addchildtoresc s3repl_1858 s3compound1_1858')
            self.admin.assert_icommand('iadmin addchildtoresc s3repl_1858 s3compound2_1858')
            self.admin.assert_icommand('iadmin addchildtoresc s3compound1_1858 s3cache1_1858 cache')
            self.admin.assert_icommand('iadmin addchildtoresc s3compound1_1858 s3archive1_1858 archive')
            self.admin.assert_icommand('iadmin addchildtoresc s3compound2_1858 s3cache2_1858 cache')
            self.admin.assert_icommand('iadmin addchildtoresc s3compound2_1858 s3archive2_1858 archive')

            # put a file to this tree
            session.assert_icommand('iput -R s3repl_1858 %s' % filename) # put file

            # make sure we have four replicas
            session.assert_icommand('ils -L %s' % filename, 'STDOUT_MULTILINE', ['s3repl_1858;s3compound1_1858;s3cache1_1858',
                                                                                 's3repl_1858;s3compound1_1858;s3archive1_1858',
                                                                                 's3repl_1858;s3compound2_1858;s3cache2_1858',
                                                                                 's3repl_1858;s3compound2_1858;s3archive2_1858'])
        finally:

            # remove the file
            session.assert_icommand('irm -f %s' % filename) # remove file

            # cleanup
            self.admin.assert_icommand('iadmin rmchildfromresc s3repl_1858 s3compound1_1858')
            self.admin.assert_icommand('iadmin rmchildfromresc s3repl_1858 s3compound2_1858')
            self.admin.assert_icommand('iadmin rmchildfromresc s3compound1_1858 s3cache1_1858 cache')
            self.admin.assert_icommand('iadmin rmchildfromresc s3compound1_1858 s3archive1_1858 archive')
            self.admin.assert_icommand('iadmin rmchildfromresc s3compound2_1858 s3cache2_1858 cache')
            self.admin.assert_icommand('iadmin rmchildfromresc s3compound2_1858 s3archive2_1858 archive')
            self.admin.assert_icommand('iadmin rmresc s3repl_1858')
            self.admin.assert_icommand('iadmin rmresc s3compound1_1858')
            self.admin.assert_icommand('iadmin rmresc s3compound2_1858')
            self.admin.assert_icommand('iadmin rmresc s3cache1_1858')
            self.admin.assert_icommand('iadmin rmresc s3archive1_1858')
            self.admin.assert_icommand('iadmin rmresc s3cache2_1858')
            self.admin.assert_icommand('iadmin rmresc s3archive2_1858')


    def test_itouch_nonexistent_file__issue_6479(self):
        replica_number_in_s3 = 1
        filename = 'test_itouch_nonexistent_file__issue_6479'
        logical_path = os.path.join(self.user0.session_collection, filename)

        try:
            # Just itouch and ensure that the data object is created successfully.
            self.user0.assert_icommand(['itouch', logical_path])
            self.assertTrue(lib.replica_exists(self.user0, logical_path, 0))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Trim the replica in cache so that we know we are working with the S3 replica.
            self.user0.assert_icommand(['itrim', '-N1', '-n0', logical_path], 'STDOUT')
            self.assertFalse(lib.replica_exists(self.user0, logical_path, 0))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))

            # Ensure that the replica actually exists. A new replica will be made on the cache resource.
            self.user0.assert_icommand(['iget', logical_path, '-'])
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, 2))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_istream_nonexistent_file__issue_6479(self):
        replica_number_in_s3 = 1
        filename = 'test_istream_nonexistent_file__issue_6479'
        logical_path = os.path.join(self.user0.session_collection, filename)
        content = 'streamin and screamin'

        try:
            # istream to a new data object and ensure that it is created successfully.
            self.user0.assert_icommand(['istream', 'write', logical_path], input=content)

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            self.assertTrue(lib.replica_exists(self.user0, logical_path, 0))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # Trim the replica in cache so that we know we are working with the S3 replica.
            self.user0.assert_icommand(['itrim', '-N1', '-n0', logical_path], 'STDOUT')
            self.assertFalse(lib.replica_exists(self.user0, logical_path, 0))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))

            # Ensure that the replica actually contains the contents streamed into it. A new replica will be made on
            # the cache resource.
            self.user0.assert_icommand(['iget', logical_path, '-'], 'STDOUT', content)
            self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))
            self.assertTrue(lib.replica_exists(self.user0, logical_path, 2))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_with_invalid_secret_key_and_overwrite__issue_6154(self):
        replica_number_in_s3 = 1
        filename = 'test_iput_with_invalid_secret_key__issue_6154'
        logical_path = os.path.join(self.user0.session_collection, filename)
        physical_path = os.path.join(self.user0.local_session_dir, filename)
        file_size_in_bytes = 10
        access_key = 'nopenopenopenope'
        secret_key = 'wrongwrongwrong!'

        try:
            lib.make_file(physical_path, file_size_in_bytes)

            with lib.file_backed_up(self.keypairfile):
                # Invalidate the existing keypairfile so that the S3 resource cannot communicate with the S3 backend.
                with open(self.keypairfile, 'w') as f:
                    f.write('{}\n{}'.format(access_key, secret_key))

                # Put the physical file, which should fail, leaving a data object with a stale replica. The main
                # purpose of this test is to ensure that the system is in a state from which it can recover.
                self.user0.assert_icommand(['iput', physical_path, logical_path], 'STDERR', 'S3_PUT_ERROR')
                self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Now overwrite the data object with wild success. This is here to ensure that things are back to normal.
            self.user0.assert_icommand(['iput', '-f', physical_path, logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_with_invalid_secret_key_and_remove__issue_6154(self):
        replica_number_in_s3 = 1
        filename = 'test_iput_with_invalid_secret_key__issue_6154'
        logical_path = os.path.join(self.user0.session_collection, filename)
        physical_path = os.path.join(self.user0.local_session_dir, filename)
        file_size_in_bytes = 10
        access_key = 'nopenopenopenope'
        secret_key = 'wrongwrongwrong!'

        try:
            lib.make_file(physical_path, file_size_in_bytes)

            with lib.file_backed_up(self.keypairfile):
                # Invalidate the existing keypairfile so that the S3 resource cannot communicate with the S3 backend.
                with open(self.keypairfile, 'w') as f:
                    f.write('{}\n{}'.format(access_key, secret_key))

                # Put the physical file, which should fail, leaving a data object with a stale replica. The main
                # purpose of this test is to ensure that the system is in a state from which it can recover.
                self.user0.assert_icommand(['iput', physical_path, logical_path], 'STDERR', 'S3_PUT_ERROR')
                self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

                # debugging
                self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

                # Attempt to remove the data object, which fails due to the invalid secret key.
                self.user0.assert_icommand(['irm', '-f', logical_path], 'STDERR', 'S3_FILE_UNLINK_ERR')
                self.assertTrue(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Attempt to remove the data object, which succeeds because the S3 object doesn't exist anyway.
            self.user0.assert_icommand(['irm', '-f', logical_path])
            self.assertFalse(lib.replica_exists(self.user0, logical_path, 0))
            self.assertFalse(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_and_replicate_with_invalid_secret_key__issue_6154(self):
        replica_number_in_s3 = 2
        filename = 'test_iput_and_replicate_with_invalid_secret_key__issue_6154'
        logical_path = os.path.join(self.user0.session_collection, filename)
        physical_path = os.path.join(self.user0.local_session_dir, filename)
        file_size_in_bytes = 10
        access_key = 'nopenopenopenope'
        secret_key = 'wrongwrongwrong!'
        test_resc = 'test_resc'

        try:
            lib.create_ufs_resource(test_resc, self.admin)

            lib.make_file(physical_path, file_size_in_bytes)

            # Put the physical file to the test resource. The test will replicate to S3.
            self.user0.assert_icommand(['iput', '-R', test_resc, physical_path, logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))

            with lib.file_backed_up(self.keypairfile):
                # Invalidate the existing keypairfile so that the S3 resource cannot communicate with the S3 backend.
                with open(self.keypairfile, 'w') as f:
                    f.write('{}\n{}'.format(access_key, secret_key))

                # Replicate the data object to the compound resource hierarchy. The replica in the cache should be good
                # and the replica in the archive should be stale due to the invalid secret key.
                self.user0.assert_icommand(['irepl', logical_path], 'STDERR', 'S3_PUT_ERROR')
                self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 1))
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Trim the cache replica so that we can try the replication again with a good secret key.
            self.user0.assert_icommand(['itrim', '-N1', '-n1', logical_path], 'STDOUT')
            self.user0.assert_icommand(['itrim', '-N1', '-n', str(replica_number_in_s3), logical_path], 'STDOUT')
            self.assertFalse(lib.replica_exists(self.user0, logical_path, 1))
            self.assertFalse(lib.replica_exists(self.user0, logical_path, replica_number_in_s3))

            # Now replicate with a good set of S3 keys and watch for success.
            self.user0.assert_icommand(['irepl', logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 1))
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in range(0, 3):
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])
            lib.remove_resource(test_resc, self.admin)


    def test_iput_and_icp_with_invalid_secret_key__issue_6154(self):
        replica_number_in_s3 = 1
        filename = 'test_iput_and_icp_with_invalid_secret_key__issue_6154'
        original_logical_path = os.path.join(self.user0.session_collection, filename + '_orig')
        logical_path = os.path.join(self.user0.session_collection, filename)
        physical_path = os.path.join(self.user0.local_session_dir, filename)
        file_size_in_bytes = 10
        access_key = 'nopenopenopenope'
        secret_key = 'wrongwrongwrong!'
        test_resc = 'test_resc'

        try:
            lib.create_ufs_resource(test_resc, self.admin)

            lib.make_file(physical_path, file_size_in_bytes)

            # Put the physical file to the test resource. The test will copy to S3.
            self.user0.assert_icommand(['iput', '-R', test_resc, physical_path, original_logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, os.path.basename(original_logical_path), 0))

            with lib.file_backed_up(self.keypairfile):
                # Invalidate the existing keypairfile so that the S3 resource cannot communicate with the S3 backend.
                with open(self.keypairfile, 'w') as f:
                    f.write('{}\n{}'.format(access_key, secret_key))

                # Copy the physical file, which should fail, leaving a data object with a stale replica. The main
                # purpose of this test is to ensure that the system is in a state from which it can recover.
                self.user0.assert_icommand(['icp', original_logical_path, logical_path], 'STDERR', 'S3_PUT_ERROR')
                self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Now overwrite the data object with wild success. This is here to ensure that things are back to normal.
            self.user0.assert_icommand(['icp', '-f', original_logical_path, logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, 0))
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

        finally:
            self.user0.run_icommand(['irm', '-f', original_logical_path])
            lib.remove_resource(test_resc, self.admin)

            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


class Test_S3_Cache_Glacier_Base(session.make_sessions_mixin([('otherrods', 'rods')], [('alice', 'apass'), ('bobby', 'bpass')])):

    def __init__(self, *args, **kwargs):
        """Set up the cacheless test."""
        # if self.proto is defined use it else default to HTTPS
        if not hasattr(self, 'proto'):
            self.proto = 'HTTPS'

        # if self.archive_naming_policy is defined use it
        # else default to 'consistent'
        if not hasattr(self, 'archive_naming_policy'):
            self.archive_naming_policy = 'consistent'

        super(Test_S3_Cache_Glacier_Base, self).__init__(*args, **kwargs)

    def setUp(self):

        super(Test_S3_Cache_Glacier_Base, self).setUp()

        self.admin = self.admin_sessions[0]
        self.user0 = self.user_sessions[0]
        self.user1 = self.user_sessions[1]

        # set up aws configuration
        self.read_aws_keys()

        # set up s3 bucket
        try:
            httpClient = urllib3.poolmanager.ProxyManager(
                os.environ['http_proxy'],
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                cert_reqs='CERT_REQUIRED',
                retries=urllib3.Retry(
                    total=5,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
        except KeyError:
            httpClient = None

        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                http_client=httpClient,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
            distro_str = '{}-{}'.format(distro.id(), distro.version()).replace(' ', '').replace('.', '')
            self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d%H-%M-%S-%f-')
            self.s3bucketname += ''.join(random.choice(string.ascii_letters) for i in range(10))
            self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
            s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        # set up resources

        hostname = lib.get_hostname()
        s3params = 'S3_RETRY_COUNT=15;S3_WAIT_TIME_SECONDS=1;S3_PROTO=%s;S3_MPU_CHUNK=10;S3_MPU_THREADS=4;S3_ENABLE_MD5=1' % self.proto
        s3params += ';S3_STSDATE=' + self.s3stsdate
        s3params += ';S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        s3params += ';S3_AUTH_FILE=' +  self.keypairfile
        s3params += ';S3_REGIONNAME=' + self.s3region
        s3params += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        if hasattr(self, 's3sse'):
            s3params += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)

        s3params=os.environ.get('S3PARAMS', s3params);

        with session.make_session_for_existing_admin() as admin_session:
            irods_config = IrodsConfig()
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            admin_session.assert_icommand("iadmin mkresc demoResc compound", 'STDOUT_SINGLELINE', 'compound')
            admin_session.assert_icommand("iadmin mkresc cacheResc 'unixfilesystem' " + hostname + ":" + irods_config.irods_directory + "/cacheRescVault", 'STDOUT_SINGLELINE', 'cacheResc')
            admin_session.assert_icommand('iadmin mkresc archiveResc s3 '+hostname+':/'+self.s3bucketname+'/irods/Vault "'+s3params+'"', 'STDOUT_SINGLELINE', 'archiveResc')
            admin_session.assert_icommand("iadmin addchildtoresc demoResc cacheResc cache")
            admin_session.assert_icommand("iadmin addchildtoresc demoResc archiveResc archive")

    def tearDown(self):
        super(Test_S3_Cache_Glacier_Base, self).tearDown()

        # delete s3 bucket
        try:
            httpClient = urllib3.poolmanager.ProxyManager(
                os.environ['http_proxy'],
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                cert_reqs='CERT_REQUIRED',
                retries=urllib3.Retry(
                    total=5,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
        except KeyError:
            httpClient = None

        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                http_client=httpClient,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        objects = s3_client.list_objects(self.s3bucketname, recursive=True)

        if not hasattr(self, 'static_bucket_name'):
            s3_client.remove_bucket(self.s3bucketname)

        # tear down resources
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc archiveResc")
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc cacheResc")
            admin_session.assert_icommand("iadmin rmresc archiveResc")
            admin_session.assert_icommand("iadmin rmresc cacheResc")
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

        shutil.rmtree(IrodsConfig().irods_directory + "/cacheRescVault", ignore_errors=True)

    def get_resource_context(self, resc_name):

        return self.admin.run_icommand('iquest "%s" "SELECT RESC_CONTEXT where RESC_NAME = \'{resc_name}\'"'.format(**locals()))[0].strip()

    def read_aws_keys(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            self.aws_access_key_id = f.readline().rstrip()
            self.aws_secret_access_key = f.readline().rstrip()

    def call_iget_get_status(self, rc1, rc2, file1, file2, file1_get, file2_get):
        _, _, rc1  = self.user0.run_icommand("iget -f {file1} {file1_get}".format(**locals()))
        _, _, rc2  = self.user0.run_icommand("iget -f {file2} {file2_get}".format(**locals()))
        return rc1 == 0 and rc2 == 0

    @unittest.skip("wait for fix of irods issue 6502")
    def test_put_get_glacier_expedited_retrieval(self):

        # get original resource context
        old_resource_context = self.get_resource_context("archiveResc")

        try:

            # update resource context for Glacier/expedited
            new_resource_context = "{old_resource_context};S3_STORAGE_CLASS=Glacier;S3_RESTORATION_TIER=expedited".format(**locals())

            self.admin.assert_icommand('iadmin modresc archiveResc context "{new_resource_context}"'.format(**locals()))

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f {file1}".format(**locals()))
            self.user0.assert_icommand("iput -f {file2}".format(**locals()))

            # new file, is it safe to assume cache is replica 0?
            self.user0.assert_icommand("itrim -N 1 -n 0 {file1}".format(**locals()), 'STDOUT')
            self.user0.assert_icommand("itrim -N 1 -n 0 {file2}".format(**locals()), 'STDOUT')

            # Once 6502 is fixed, revisit these assertions.  They may need to be changed to HIERARCHY_ERROR
            cmd = "iget -f {file1} {file1_get}".format(**locals())
            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in GLACIER and has been queued for restoration', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and has been queued for restoration', stdout))

            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in GLACIER and is currently being restored', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and is currently being restored', stdout))

            cmd = "iget -f {file2} {file2_get}".format(**locals())
            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in GLACIER and has been queued for restoration', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and has been queued for restoration', stdout))

            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in GLACIER and is currently being restored', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and is currently being restored', stdout))

            # Wait for the file to be restored from glacier.  Try every 20 seconds.
            # Wait up to 6 minutes (should be done in less than 5).
            rc1 = 1
            rc2 = 1

            lib.delayAssert(lambda:
                    self.call_iget_get_status(rc1, rc2, file1, file2, file1_get, file2_get), interval=20, maxrep=18)

            # make sure the files that were put and got are the same
            self.user0.assert_icommand("diff {file1} {file1_get}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("diff {file2} {file2_get}".format(**locals()), 'EMPTY')

        finally:

            # cleanup

            # restore old resource context
            self.admin.assert_icommand('iadmin modresc archiveResc context "{old_resource_context}"'.format(**locals()))

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)
