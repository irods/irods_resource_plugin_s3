import sys
if (sys.version_info >= (2,7)):
    import unittest
else:
    import unittest2 as unittest
from pydevtest_common import assertiCmd, assertiCmdFail, getiCmdOutput, create_local_testfile, get_hostname
import pydevtest_sessions as s
from resource_suite import ResourceSuite, ShortAndSuite
from test_chunkydevtest import ChunkyDevTest
import socket
import os
import commands
import shutil
import subprocess
import re


class Test_Compound_with_S3_Resource(unittest.TestCase, ResourceSuite, ChunkyDevTest):

    hostname = socket.gethostname()
    my_test_resource = {
        "setup"    : [
            "iadmin modresc demoResc name origResc",
            "iadmin mkresc demoResc compound",
            "iadmin mkresc cacheResc 'unixfilesystem' "+hostname+":/var/lib/irods/cacheRescVault",
            "iadmin mkresc archiveResc s3 "+hostname+":/irods-ci/irods/Vault S3_AUTH_FILE=/home/irodsbuild/secrets/amazon_web_services-CI.keypair",
            "iadmin addchildtoresc demoResc cacheResc cache",
            "iadmin addchildtoresc demoResc archiveResc archive",
        ],
        "teardown" : [
            "iadmin rmchildfromresc demoResc archiveResc",
            "iadmin rmchildfromresc demoResc cacheResc",
            "iadmin rmresc archiveResc",
            "iadmin rmresc cacheResc",
            "iadmin rmresc demoResc",
            "iadmin modresc origResc name demoResc",
            "rm -rf /var/lib/irods/archiveRescVault",
            "rm -rf /var/lib/irods/cacheRescVault",
        ],
    }

    def setUp(self):
        ResourceSuite.__init__(self)
        s.twousers_up()
        self.run_resource_setup()

    def tearDown(self):
        self.run_resource_teardown()
        s.twousers_down()

    def test_irm_specific_replica(self):
        assertiCmd(s.adminsession,"ils -L "+self.testfile,"LIST",self.testfile) # should be listed
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+self.testfile) # creates replica
        assertiCmd(s.adminsession,"ils -L "+self.testfile,"LIST",self.testfile) # should be listed twice
        assertiCmd(s.adminsession,"irm -n 0 "+self.testfile) # remove original from cacheResc only
        assertiCmd(s.adminsession,"ils -L "+self.testfile,"LIST",["2 "+self.testresc,self.testfile]) # replica 2 should still be there
        assertiCmdFail(s.adminsession,"ils -L "+self.testfile,"LIST",["0 "+s.adminsession.getDefResource(),self.testfile]) # replica 0 should be gone
        trashpath = "/"+s.adminsession.getZoneName()+"/trash/home/"+s.adminsession.getUserName()+"/"+s.adminsession.sessionId
        assertiCmdFail(s.adminsession,"ils -L "+trashpath+"/"+self.testfile,"LIST",["0 "+s.adminsession.getDefResource(),self.testfile]) # replica should not be in trash

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
        filepath = create_local_testfile(filename)
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        assertiCmd(s.adminsession,"ils -L "+filename,"ERROR","does not exist")                           # should not be listed
        assertiCmd(s.adminsession,"iput "+filename)                                                      # put file
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)                                # replicate to test resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)                                    # 
        assertiCmd(s.adminsession,"iput -f -R %s %s %s" % (self.testresc, doublefile, filename) )        # overwrite test repl with different data
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," "+filename])                        # default resource cache should have dirty copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," "+filename])                        # default resource archive should have dirty copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," "+doublesize+" "," "+filename]) # default resource cache should not have doublesize file
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," "+doublesize+" "," "+filename]) # default resource archive should not have doublesize file
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 "," "+doublesize+" ","& "+filename])    # targeted resource should have new double clean copy
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    ###################
    # irepl
    ###################

    def test_irepl_update_replicas(self):
        # local setup
        filename = "updatereplicasfile.txt"
        filepath = create_local_testfile(filename)
        hostname = get_hostname()
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)

        # assertions
        assertiCmd(s.adminsession,"iadmin mkresc thirdresc unixfilesystem %s:/tmp/thirdrescVault" % hostname, "LIST", "Creating")   # create third resource
        assertiCmd(s.adminsession,"iadmin mkresc fourthresc unixfilesystem %s:/tmp/fourthrescVault" % hostname, "LIST", "Creating") # create fourth resource
        assertiCmd(s.adminsession,"ils -L "+filename,"ERROR","does not exist")              # should not be listed
        assertiCmd(s.adminsession,"iput "+filename)                                         # put file
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)                   # replicate to test resource
        assertiCmd(s.adminsession,"irepl -R thirdresc "+filename)                           # replicate to third resource
        assertiCmd(s.adminsession,"irepl -R fourthresc "+filename)                          # replicate to fourth resource
        assertiCmd(s.adminsession,"iput -f -R "+self.testresc+" "+doublefile+" "+filename)  # repave overtop test resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)                       # for debugging

        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," & "+filename]) # should have a dirty copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," & "+filename]) # should have a dirty copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 "," & "+filename])     # should have a clean copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename]) # should have a dirty copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 4 "," & "+filename]) # should have a dirty copy

        assertiCmd(s.adminsession,"irepl -U "+filename)                                 # update last replica

        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," & "+filename]) # should have a dirty copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," & "+filename]) # should have a dirty copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 "," & "+filename])     # should have a clean copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename]) # should have a dirty copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 4 "," & "+filename])     # should have a clean copy

        assertiCmd(s.adminsession,"irepl -aU "+filename)                                # update all replicas

        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," & "+filename])     # should have a clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," & "+filename])     # should have a clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 "," & "+filename])     # should have a clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename])     # should have a clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 4 "," & "+filename])     # should have a clean copy

        assertiCmd(s.adminsession,"irm -f "+filename)                                   # cleanup file
        assertiCmd(s.adminsession,"iadmin rmresc thirdresc")                            # remove third resource
        assertiCmd(s.adminsession,"iadmin rmresc fourthresc")                           # remove third resource

        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    def test_irepl_over_existing_second_replica__ticket_1705(self):
        # local setup
        filename = "secondreplicatest.txt"
        filepath = create_local_testfile(filename)
        # assertions
        assertiCmd(s.adminsession,"ils -L "+filename,"ERROR","does not exist")          # should not be listed
        assertiCmd(s.adminsession,"iput -R "+self.testresc+" "+filename)                # put file
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)                   # for debugging
        assertiCmd(s.adminsession,"irepl "+filename)                                    # replicate to default resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)                   # for debugging
        assertiCmd(s.adminsession,"irepl "+filename)                                    # replicate overtop default resource
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename]) # should not have a replica 3
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)               # replicate overtop test resource
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename]) # should not have a replica 3
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_third_replica__ticket_1705(self):
        # local setup
        filename = "thirdreplicatest.txt"
        filepath = create_local_testfile(filename)
        hostname = get_hostname()
        # assertions
        assertiCmd(s.adminsession,"iadmin mkresc thirdresc unixfilesystem %s:/tmp/thirdrescVault" % hostname, "LIST", "Creating") # create third resource
        assertiCmd(s.adminsession,"ils -L "+filename,"ERROR","does not exist") # should not be listed
        assertiCmd(s.adminsession,"iput "+filename)                            # put file
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)      # replicate to test resource
        assertiCmd(s.adminsession,"irepl -R thirdresc "+filename)              # replicate to third resource
        assertiCmd(s.adminsession,"irepl "+filename)                           # replicate overtop default resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)          # for debugging
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)      # replicate overtop test resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)          # for debugging
        assertiCmd(s.adminsession,"irepl -R thirdresc "+filename)              # replicate overtop third resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)          # for debugging
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 4 "," & "+filename]) # should not have a replica 4
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 5 "," & "+filename]) # should not have a replica 5
        assertiCmd(s.adminsession,"irm -f "+filename)                          # cleanup file
        assertiCmd(s.adminsession,"iadmin rmresc thirdresc")                   # remove third resource
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_bad_replica__ticket_1705(self):
        # local setup
        filename = "reploverwritebad.txt"
        filepath = create_local_testfile(filename)
        doublefile = "doublefile.txt"
        os.system("cat %s %s > %s" % (filename, filename, doublefile))
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        assertiCmd(s.adminsession,"ils -L "+filename,"ERROR","does not exist") # should not be listed
        assertiCmd(s.adminsession,"iput "+filename)                            # put file
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)          # for debugging
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)      # replicate to test resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",filename)          # for debugging
        assertiCmd(s.adminsession,"iput -f %s %s" % (doublefile, filename) )   # overwrite default repl with different data
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," & "+filename]) # default resource cache should have clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 0 "," "+doublesize+" "," & "+filename]) # default resource cache should have new double clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," & "+filename]) # default resource archive should have clean copy
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 "," "+doublesize+" "," & "+filename]) # default resource archive should have new double clean copy
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 2 "+self.testresc," "+doublesize+" ","  "+filename]) # test resource should not have doublesize file
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" "+filename)      # replicate back onto test resource
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 "+self.testresc," "+doublesize+" "," & "+filename]) # test resource should have new clean doublesize file
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 3 "," & "+filename]) # should not have a replica 3
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
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",filename) # should not be listed
        assertiCmd(s.adminsession,"iput --purgec "+filename) # put file
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 ",filename]) # should not be listed (trimmed)
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 ",filename]) # should be listed once - replica 1
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 2 ",filename]) # should be listed only once

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
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",filename) # should not be listed
        assertiCmd(s.adminsession,"iput "+filename) # put file
        assertiCmd(s.adminsession,"iget -f --purgec "+filename) # get file and purge 'cached' replica
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 ",filename]) # should not be listed (trimmed)
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 ",filename]) # should be listed once
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 2 ",filename]) # should not be listed

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
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",filename) # should not be listed
        assertiCmd(s.adminsession,"iput "+filename) # put file
        assertiCmd(s.adminsession,"irepl -R "+self.testresc+" --purgec "+filename) # replicate to test resource
        assertiCmdFail(s.adminsession,"ils -L "+filename,"LIST",[" 0 ",filename]) # should not be listed (trimmed)
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 1 ",filename]) # should be listed twice - 2 of 3
        assertiCmd(s.adminsession,"ils -L "+filename,"LIST",[" 2 ",filename]) # should be listed twice - 1 of 3

        # local cleanup
        output = commands.getstatusoutput( 'rm '+filepath )

