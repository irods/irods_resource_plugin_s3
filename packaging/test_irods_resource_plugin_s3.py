import commands
import os
import re
import shutil
import subprocess

import sys
if sys.version_info >= (2,7):
    import unittest
else:
    import unittest2 as unittest

import lib
from resource_suite import ResourceSuite
from test_chunkydevtest import ChunkyDevTest

class Test_Compound_with_S3_Resource(ResourceSuite, ChunkyDevTest, unittest.TestCase):
    def setUp(self):
        hostname = lib.get_hostname()
        with lib.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', stdin_string='yes\n')
            admin_session.assert_icommand("iadmin mkresc demoResc compound", 'STDOUT_SINGLELINE', 'compound')
            admin_session.assert_icommand("iadmin mkresc cacheResc 'unixfilesystem' "+hostname+":/var/lib/irods/cacheRescVault", 'STDOUT_SINGLELINE', 'cacheResc')
            admin_session.assert_icommand('iadmin mkresc archiveResc s3 '+hostname+':/irods-ci/irods/Vault "S3_DEFAULT_HOSTNAME=s3.amazonaws.com;S3_AUTH_FILE=/home/irodsbuild/secrets/amazon_web_services-CI.keypair;S3_RETRY_COUNT=15;S3_WAIT_TIME_SEC=1"', 'STDOUT_SINGLELINE', 'archiveResc')
            admin_session.assert_icommand("iadmin addchildtoresc demoResc cacheResc cache")
            admin_session.assert_icommand("iadmin addchildtoresc demoResc archiveResc archive")
        super(Test_Compound_with_S3_Resource, self).setUp()

    def tearDown(self):
        super(Test_Compound_with_S3_Resource, self).tearDown()
        with lib.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc archiveResc")
            admin_session.assert_icommand("iadmin rmchildfromresc demoResc cacheResc")
            admin_session.assert_icommand("iadmin rmresc archiveResc")
            admin_session.assert_icommand("iadmin rmresc cacheResc")
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', stdin_string='yes\n')
        shutil.rmtree(lib.get_irods_top_level_dir() + "/archiveRescVault", ignore_errors=True)
        shutil.rmtree(lib.get_irods_top_level_dir() + "/cacheRescVault", ignore_errors=True)

    def test_irm_specific_replica(self):
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+self.testfile) # creates replica
        self.admin.assert_icommand("ils -L "+self.testfile,'STDOUT_SINGLELINE',self.testfile) # should be listed twice
        self.admin.assert_icommand("irm -n 0 "+self.testfile) # remove original from cacheResc only
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
        self.admin.assert_icommand("irepl "+filename)                                    # replicate overtop default resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 3 "," & "+filename]) # should not have a replica 3
        self.admin.assert_icommand("irepl -R "+self.testresc+" "+filename)               # replicate overtop test resource
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
        self.admin.assert_icommand("iput --purgec "+filename) # put file
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
        self.admin.assert_icommand("iget -f --purgec "+filename) # get file and purge 'cached' replica
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
        self.admin.assert_icommand("irepl -R "+self.testresc+" --purgec "+filename) # replicate to test resource
        self.admin.assert_icommand_fail("ils -L "+filename,'STDOUT_SINGLELINE',[" 0 ",filename]) # should not be listed (trimmed)
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 1 ",filename]) # should be listed twice - 2 of 3
        self.admin.assert_icommand("ils -L "+filename,'STDOUT_SINGLELINE',[" 2 ",filename]) # should be listed twice - 1 of 3

        # local cleanup
        output = commands.getstatusoutput( 'rm '+filepath )
