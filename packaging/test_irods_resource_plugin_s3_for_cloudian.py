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

class Test_Compound_With_S3_Resource(ResourceSuite, ChunkyDevTest, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.keypairfile='/etc/irods/cloudian_credentials.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='demoreg1'
        self.s3endPoint='s3.cloudianhyperstore.com'
        self.s3sse = 0 # server side encryption
        super(Test_Compound_With_S3_Resource, self).__init__(*args, **kwargs)

    def setUp(self):
        # skip ssl tests on ub12
        distro_str = ''.join(platform.linux_distribution()[:2]).replace(' ','')
        if self._testMethodName.startswith('test_ssl') and distro_str.lower().startswith('ubuntu12'):
           self.skipTest("skipping ssl tests on ubuntu 12")

        # set up aws configuration
        self.read_aws_keys()

        # set up s3 bucket
        #s3 = boto3.resource('s3', region_name=self.s3region)
        s3_client = Minio('http://' + self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key)

        self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d.%H-%M-%S-%f-')
        self.s3bucketname += ''.join(random.choice(string.letters) for i in xrange(10))
        self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
        s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        # set up resources
        hostname = lib.get_hostname()
        s3params = 'S3_RETRY_COUNT=15;S3_WAIT_TIME_SEC=1;S3_PROTO=HTTPS;S3_MPU_CHUNK=10;S3_MPU_THREADS=4;S3_ENABLE_MD5=1'
        s3params += ';S3_STSDATE=' + self.s3stsdate
        s3params += ';S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        s3params += ';S3_AUTH_FILE=' +  self.keypairfile
        s3params += ';S3_REGIONNAME=' + self.s3region
        s3params += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        try:
            s3params += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)
        except AttributeError:
            pass

        s3params=os.environ.get('S3PARAMS', s3params);

        with session.make_session_for_existing_admin() as admin_session:
            irods_config = IrodsConfig()
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            admin_session.assert_icommand("iadmin mkresc demoResc compound", 'STDOUT_SINGLELINE', 'compound')
            admin_session.assert_icommand("iadmin mkresc cacheResc 'unixfilesystem' " + hostname + ":" + irods_config.irods_directory + "/cacheRescVault", 'STDOUT_SINGLELINE', 'cacheResc')
            admin_session.assert_icommand('iadmin mkresc archiveResc s3 '+hostname+':/'+self.s3bucketname+'/irods/Vault "'+s3params+'"', 'STDOUT_SINGLELINE', 'archiveResc')
            admin_session.assert_icommand("iadmin addchildtoresc demoResc cacheResc cache")
            admin_session.assert_icommand("iadmin addchildtoresc demoResc archiveResc archive")

        super(Test_Compound_With_S3_Resource, self).setUp()

    def tearDown(self):
        super(Test_Compound_With_S3_Resource, self).tearDown()

        print(self.bucket)

        # delete s3 bucket
        s3_client = Minio('http://' + self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key)
        objects = s3_client.list_objects_v2(self.s3bucketname, recursive=True)
        try:
            for del_err in s3_client.remove_objects(self.s3bucketname, [object.object_name for object in objects]):
                print("Deletion Error: {}".format(del_err))
        except ResponseError as err:
            print(err)
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

    def test_irm_specific_replica(self):
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+self.testfile) # creates replica
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed twice
        self.admin.assert_icommand("irm -n 0 "+self.testfile, 'STDOUT_SINGLELINE','deprecated') # remove original from cacheResc only
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

        self.admin.assert_icommand("irepl -U "+filename)                                 # update last replica

        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "," & "+filename])     # should have a clean copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should have a dirty copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 4 "," & "+filename])     # should have a clean copy

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
        self.admin.assert_icommand("irepl "+filename)                           # replicate overtop default resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)      # replicate overtop test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand("irepl -R thirdresc "+filename)              # replicate overtop third resource
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
        self.admin.assert_icommand("ils -L "+filename,'STDERR_SINGLELINE',"does not exist") # should not be listed
        self.admin.assert_icommand("iput "+filename)                            # put file
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)      # replicate to test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',filename)          # for debugging
        self.admin.assert_icommand("iput -f %s %s" % (doublefile, filename) )   # overwrite default repl with different data
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," & "+filename]) # default resource cache should have clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 "," "+doublesize+" "," & "+filename]) # default resource cache should have new double clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," & "+filename]) # default resource archive should have clean copy
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 "," "+doublesize+" "," & "+filename]) # default resource archive should have new double clean copy
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "+self.testresc," "+doublesize+" ","  "+filename]) # test resource should not have doublesize file
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)      # replicate back onto test resource
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 "+self.testresc," "+doublesize+" "," & "+filename]) # test resource should have new clean doublesize file
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should not have a replica 3
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    def test_iput_with_purgec(self):
        # local setup
        filename = "purgecfile.txt"
        filepath = os.path.abspath(filename)
        f = open(filepath,'wb')
        f.write("TESTFILE -- ["+filepath+"]")
        f.close()

        # assertions
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        self.admin.assert_icommand("iput -f --purgec "+filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.') # get file and purge 'cached' replica
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed once - replica 1
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should be listed only once

        # local cleanup
        output = commands.getstatusoutput( 'rm '+filepath )

    def test_iget_with_purgec(self):
        # local setup
        filename = "purgecgetfile.txt"
        filepath = os.path.abspath(filename)
        f = open(filepath,'wb')
        f.write("TESTFILE -- ["+filepath+"]")
        f.close()

        # assertions
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        self.admin.assert_icommand("iput "+filename) # put file
        self.admin.assert_icommand("iget -f --purgec "+filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.') # get file and purge 'cached' replica
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed once
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should not be listed

        # local cleanup
        output = commands.getstatusoutput( 'rm '+filepath )

    def test_irepl_with_purgec(self):
        # local setup
        filename = "purgecreplfile.txt"
        filepath = os.path.abspath(filename)
        f = open(filepath,'wb')
        f.write("TESTFILE -- ["+filepath+"]")
        f.close()

        # assertions
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',filename) # should not be listed
        self.admin.assert_icommand("iput "+filename) # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " --purgec " + filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.')  # replicate to test resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed twice - 2 of 3
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should be listed twice - 1 of 3

        # local cleanup
        output = commands.getstatusoutput( 'rm '+filepath )

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

        # cleanup
        session.run_icommand('irm -f ' + filename)

