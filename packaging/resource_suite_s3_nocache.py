from __future__ import print_function
import base64
import commands
import copy
import datetime
import filecmp
import getpass
import hashlib
import os
import sys
import time
import platform
import random
import string
import io

try:
   from minio import Minio
   from minio.error import ResponseError
except ImportError:
   print('This test requires minio: perhaps try pip install minio')
   exit()

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from .. import test
from .. import lib
from ..configuration import IrodsConfig
from ..controller import IrodsController
from . import session


class Test_S3_NoCache_Base(session.make_sessions_mixin([('otherrods', 'rods')], [('alice', 'apass'), ('bobby', 'bpass')])):

    def __init__(self, *args, **kwargs):
        """Set up the cacheless test."""
        # if self.proto is defined use it else default to HTTPS
        try:
            self.proto = self.proto
        except AttributeError:
            self.proto = 'HTTPS'

        # if self.archive_naming_policy is defined use it
        # else default to 'consistent'
        try:
            self.archive_naming_policy = self.archive_naming_policy
        except AttributeError:
            self.archive_naming_policy = 'consistent'

        super(Test_S3_NoCache_Base, self).__init__(*args, **kwargs)

    def setUp(self):

        super(Test_S3_NoCache_Base, self).setUp()
        self.admin = self.admin_sessions[0]
        self.user0 = self.user_sessions[0]
        self.user1 = self.user_sessions[1]

        print("run_resource_setup - BEGIN")
        self.testfile = "testfile.txt"
        self.testdir = "testdir"

        hostname = lib.get_hostname()

        # read aws keys
        self.read_aws_keys()

        # set up s3 bucket
        if self.proto == 'HTTPS':
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key)
        else:
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key, secure=False)


        distro_str = ''.join(platform.linux_distribution()[:2]).replace(' ','')
        self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d.%H-%M-%S-%f-')
        self.s3bucketname += ''.join(random.choice(string.letters) for i in xrange(10))
        self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
        s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        self.testresc = "TestResc"
        self.testvault = "/tmp/" + self.testresc
        self.anotherresc = "AnotherResc"
        self.anothervault = "/tmp/" + self.anotherresc

        self.s3_context = 'S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        self.s3_context += ';S3_AUTH_FILE=' + self.keypairfile
        self.s3_context += ';S3_REGIONNAME=' + self.s3region
        self.s3_context += ';S3_RETRY_COUNT=2'
        self.s3_context += ';S3_WAIT_TIME_SEC=3'
        self.s3_context += ';S3_PROTO=' + self.proto
        self.s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        self.s3_context += ';HOST_MODE=cacheless_attached'
        self.s3_context += ';S3_ENABLE_MD5=1'
        self.s3_context += ';S3_ENABLE_MPU=' + str(self.s3EnableMPU)

        try:
            self.s3_context += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)
        except AttributeError:
            pass
        self.s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy

        self.admin.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

        self.admin.assert_icommand("iadmin mkresc demoResc s3 " + hostname + ":/" + self.s3bucketname + "/tmp/demoResc " + self.s3_context, 'STDOUT_SINGLELINE', 's3')

        self.admin.assert_icommand(
            ['iadmin', "mkresc", self.testresc, 's3', hostname + ":/" + self.s3bucketname + self.testvault, self.s3_context], 'STDOUT_SINGLELINE', 's3')
        self.admin.assert_icommand(
            ['iadmin', "mkresc", self.anotherresc, 's3', hostname + ":/" + self.s3bucketname + self.anothervault, self.s3_context], 'STDOUT_SINGLELINE', 's3')

        with open(self.testfile, 'wt') as f:
            print('I AM A TESTFILE -- [' + self.testfile + ']', file=f, end='')
        self.admin.run_icommand(['imkdir', self.testdir])
        self.admin.run_icommand(['iput', self.testfile])
        self.admin.run_icommand(['icp', self.testfile, '../../public/'])
        self.admin.run_icommand(['ichmod', 'read', self.user0.username, '../../public/' + self.testfile])
        self.admin.run_icommand(['ichmod', 'write', self.user1.username, '../../public/' + self.testfile])
        print('run_resource_setup - END')

    def tearDown(self):
        print("run_resource_teardown - BEGIN")
        os.unlink(self.testfile)
        self.admin.run_icommand('icd')
        self.admin.run_icommand(['irm', self.testfile, '../public/' + self.testfile])
        self.admin.run_icommand('irm -rf ../../bundle')

        super(Test_S3_NoCache_Base, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand('irmtrash -M')
            admin_session.run_icommand(['iadmin', 'rmresc', self.testresc])
            admin_session.run_icommand(['iadmin', 'rmresc', self.anotherresc])
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            print("run_resource_teardown - END")

        # delete s3 bucket
        if self.proto == 'HTTPS':
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key)
        else:
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key, secure=False)
        objects = s3_client.list_objects_v2(self.s3bucketname, recursive=True)

        try:
            for del_err in s3_client.remove_objects(self.s3bucketname, [object.object_name for object in objects]):
                print("Deletion Error: {}".format(del_err))
        except ResponseError as err:
            print(err)

        s3_client.remove_bucket(self.s3bucketname)

    def read_aws_keys(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            self.aws_access_key_id = f.readline().rstrip()
            self.aws_secret_access_key = f.readline().rstrip()

    # read the endpoint address from the file endpointfile
    @classmethod
    def read_endpoint(cls, endpointfile):
        # read endpoint file
        with open(endpointfile) as f:
            return f.readline().rstrip()

    def set_up_aws_config_dir(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            aws_access_key_id = f.readline().rstrip()
            aws_secret_access_key = f.readline().rstrip()

        # make config dir
        aws_cfg_dir_path = os.path.join(os.path.expanduser('~'), '.aws')
        try:
            os.makedirs(aws_cfg_dir_path)
        except OSError:
            if not os.path.isdir(aws_cfg_dir_path):
                raise

        # make config file
        with open(os.path.join(aws_cfg_dir_path, 'config'), 'w') as cfg_file:
            cfg_file.write('[default]\n')
            cfg_file.write('region=' + self.s3region + '\n')

        # make credentials file
        with open(os.path.join(aws_cfg_dir_path, 'credentials'), 'w') as cred_file:
            cred_file.write('[default]\n')
            cred_file.write('aws_access_key_id = ' + aws_access_key_id + '\n')
            cred_file.write('aws_secret_access_key = ' + aws_secret_access_key + '\n')

    ###################
    # iget
    ###################

    def test_local_iget(self):
        # local setup
        localfile = "local.txt"
        # assertions
        self.admin.assert_icommand("iget " + self.testfile + " " + localfile)  # iget
        output, _ = lib.execute_command(['ls', localfile])
        print("  output: [" + output + "]")
        assert output.strip() == localfile
        # local cleanup
        if os.path.exists(localfile):
            os.unlink(localfile)

    def test_local_iget_with_overwrite(self):
        # local setup
        localfile = "local.txt"
        # assertions
        self.admin.assert_icommand("iget " + self.testfile + " " + localfile)  # iget
        self.admin.assert_icommand_fail("iget " + self.testfile + " " + localfile)  # already exists
        self.admin.assert_icommand("iget -f " + self.testfile + " " + localfile)  # already exists, so force
        output, _ = lib.execute_command(['ls', localfile])
        print("  output: [" + output + "]")
        assert output.strip() == localfile
        # local cleanup
        if os.path.exists(localfile):
            os.unlink(localfile)

    def test_local_iget_with_bad_option(self):
        # assertions
        self.admin.assert_icommand_fail("iget -z")  # run iget with bad option

    @unittest.skipIf(True, 'test does not work in decoupled because we are using same bucket for multiple resources')
    def test_iget_with_stale_replica(self):  # formerly known as 'dirty'
        # local setup
        filename = "original.txt"
        filepath = lib.create_local_testfile(filename)
        updated_filename = "updated_file_with_longer_filename.txt"
        lib.create_local_testfile(updated_filename)
        retrievedfile = "retrievedfile.txt"
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)  # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)  # replicate file
        # force new put on second resource
        self.admin.assert_icommand("iput -f -R " + self.testresc + " " + updated_filename + " " + filename)
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)  # debugging
        # should get orig file (replica 0)
        self.admin.assert_icommand("iget -f -n 0 " + filename + " " + retrievedfile)

        assert filecmp.cmp(filename, retrievedfile)  # confirm retrieved is correct
        self.admin.assert_icommand("iget -f " + filename + " " + retrievedfile)  # should get updated file
        assert filecmp.cmp(updated_filename, retrievedfile)  # confirm retrieved is correct
        # local cleanup
        if os.path.exists(filename):
            os.unlink(filename)
        if os.path.exists(updated_filename):
            os.unlink(updated_filename)
        if os.path.exists(retrievedfile):
            os.unlink(retrievedfile)

    def test_iget_with_purgec(self):
        # local setup
        filename = "purgecgetfile.txt"
        filepath = lib.create_local_testfile(filename)
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)  # put file
        self.admin.assert_icommand("iget -f --purgec "+filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.') # get file and purge 'cached' replica
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])  # should be listed once
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", filename])  # should be listed only once
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", filename])  # should be listed only once

        # local cleanup
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_iget_specify_resource_with_single_thread__issue_3140(self):
        # local setup
        filename = "test_file_3140.txt"
        filepath = lib.create_local_testfile(filename)
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)  # put file
        self.admin.assert_icommand("iget -f -R demoResc -N0 " + filename)  # get file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])  # should be listed once

        # local cleanup
        commands.getstatusoutput('rm ' + filepath)

    ###################
    # imv
    ###################

    def test_local_imv(self):
        # local setup
        movedfile = "moved_file.txt"
        # assertions
        self.admin.assert_icommand("imv " + self.testfile + " " + movedfile)  # move
        self.admin.assert_icommand("ils -L " + movedfile, 'STDOUT_SINGLELINE', movedfile)  # should be listed
        # local cleanup

    def test_local_imv_to_directory(self):
        # local setup
        # assertions
        self.admin.assert_icommand("imv " + self.testfile + " " + self.testdir)  # move
        self.admin.assert_icommand("ils -L " + self.testdir, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        # local cleanup

    def test_local_imv_to_existing_filename(self):
        # local setup
        copyfile = "anotherfile.txt"
        # assertions
        self.admin.assert_icommand("icp " + self.testfile + " " + copyfile)  # icp
        # cannot overwrite existing file
        self.admin.assert_icommand("imv " + self.testfile + " " + copyfile, 'STDERR_SINGLELINE', "CAT_NAME_EXISTS_AS_DATAOBJ")
        # local cleanup

    def test_local_imv_collection_to_sibling_collection__ticket_2448(self):
        self.admin.assert_icommand("imkdir first_dir")  # first collection
        self.admin.assert_icommand("icp " + self.testfile + " first_dir")  # add file
        self.admin.assert_icommand("imkdir second_dir")  # second collection
        self.admin.assert_icommand("imv -v first_dir second_dir", 'STDOUT_SINGLELINE', "first_dir")  # imv into sibling
        self.admin.assert_icommand_fail("ils -L", 'STDOUT_SINGLELINE', "first_dir")  # should not be listed
        self.admin.assert_icommand("ils -L second_dir", 'STDOUT_SINGLELINE', "second_dir/first_dir")  # should be listed
        self.admin.assert_icommand("ils -L second_dir/first_dir", 'STDOUT_SINGLELINE', self.testfile)  # should be listed

    def test_local_imv_collection_to_collection_with_modify_access_not_own__ticket_2317(self):
        publicpath = "/" + self.admin.zone_name + "/home/public"
        targetpath = publicpath + "/target"
        sourcepath = publicpath + "/source"
        # cleanup
        self.admin.assert_icommand("imkdir -p " + targetpath)  # target
        self.admin.assert_icommand("ichmod -M -r own " + self.admin.username + " " + targetpath)  # ichmod
        self.admin.assert_icommand("irm -rf " + targetpath)  # cleanup
        self.admin.assert_icommand("imkdir -p " + sourcepath)  # source
        self.admin.assert_icommand("ichmod -M -r own " + self.admin.username + " " + sourcepath)  # ichmod
        self.admin.assert_icommand("irm -rf " + sourcepath)  # cleanup
        # setup and test
        self.admin.assert_icommand("imkdir " + targetpath)  # target
        self.admin.assert_icommand("ils -rAL " + targetpath, 'STDOUT_SINGLELINE', "own")  # debugging
        self.admin.assert_icommand("ichmod -r write " + self.user0.username + " " + targetpath)  # ichmod
        self.admin.assert_icommand("ils -rAL " + targetpath, 'STDOUT_SINGLELINE', "modify object")  # debugging
        self.admin.assert_icommand("imkdir " + sourcepath)  # source
        self.admin.assert_icommand("ichmod -r own " + self.user0.username + " " + sourcepath)  # ichmod
        self.admin.assert_icommand("ils -AL " + sourcepath, 'STDOUT_SINGLELINE', "own")  # debugging
        self.user0.assert_icommand("imv " + sourcepath + " " + targetpath)  # imv
        self.admin.assert_icommand("ils -AL " + targetpath, 'STDOUT_SINGLELINE', targetpath + "/source")  # debugging
        self.admin.assert_icommand("irm -rf " + targetpath)  # cleanup
        # cleanup
        self.admin.assert_icommand("imkdir -p " + targetpath)  # target
        self.admin.assert_icommand("ichmod -M -r own " + self.admin.username + " " + targetpath)  # ichmod
        self.admin.assert_icommand("irm -rf " + targetpath)  # cleanup
        self.admin.assert_icommand("imkdir -p " + sourcepath)  # source
        self.admin.assert_icommand("ichmod -M -r own " + self.admin.username + " " + sourcepath)  # ichmod
        self.admin.assert_icommand("irm -rf " + sourcepath)  # cleanup

    ###################
    # iphymv
    ###################

    def test_iphymv_to_nonexistent_resource(self):
        self.admin.assert_icommand("ils -L", 'STDOUT_SINGLELINE', self.testfile)  # debug
        self.admin.assert_icommand("iphymv -R nonexistentresc " + self.testfile,
                                   'STDERR_SINGLELINE', "SYS_RESC_DOES_NOT_EXIST")  # should fail
        self.admin.assert_icommand("ils -L", 'STDOUT_SINGLELINE', self.testfile)  # debug

    def test_iphymv_admin_mode(self):
        lib.touch("file.txt")
        for i in range(0, 100):
            self.user0.assert_icommand("iput file.txt " + str(i) + ".txt", "EMPTY")
        self.admin.assert_icommand("iphymv -r -M -n0 -R " + self.testresc + " " + self.admin.session_collection)  # creates replica

    ###################
    # iput
    ###################
    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing")
    def test_ssl_iput_with_rods_env(self):
        config = IrodsConfig()
        server_key_path = os.path.join(self.admin.local_session_dir, 'server.key')
        #server_csr_path = os.path.join(self.admin.local_session_dir, 'server.csr')
        chain_pem_path = os.path.join(self.admin.local_session_dir, 'chain.pem')
        dhparams_pem_path = os.path.join(self.admin.local_session_dir, 'dhparams.pem')

        lib.execute_command(['openssl', 'genrsa', '-out', server_key_path, '1024'])
        #lib.execute_command('openssl req -batch -new -key %s -out %s' % (server_key_path, server_csr_path))
        lib.execute_command('openssl req -batch -new -x509 -key %s -out %s -days 365' % (server_key_path, chain_pem_path))
        lib.execute_command('openssl dhparam -2 -out %s 1024' % (dhparams_pem_path))  # normally 2048, but smaller size here for speed

        with lib.file_backed_up(config.client_environment_path):
            server_update = {
                'irods_ssl_certificate_chain_file': chain_pem_path,
                'irods_ssl_certificate_key_file': server_key_path,
                'irods_ssl_dh_params_file': dhparams_pem_path,
            }
            lib.update_json_file_from_dict(config.client_environment_path, server_update)

            client_update = {
                'irods_client_server_policy': 'CS_NEG_REQUIRE',
                'irods_ssl_verify_server': 'none',
            }

            session_env_backup = copy.deepcopy(self.admin.environment_file_contents)
            self.admin.environment_file_contents.update(client_update)

            filename = 'encryptedfile.txt'
            filepath = lib.create_local_testfile(filename)
            IrodsController().restart()
            self.admin.assert_icommand(['iinit', self.admin.password])
            self.admin.assert_icommand(['iput', filename])
            self.admin.assert_icommand(['ils', '-L', filename], 'STDOUT_SINGLELINE', filename)

            self.admin.environment_file_contents = session_env_backup

        IrodsController().restart()

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing")
    def test_ssl_iput_small_and_large_files(self):
        # set up client and server side for ssl handshake

        # server side certificate setup
        server_key_path = os.path.join(self.admin.local_session_dir, 'server.key')
        #server_csr_path = os.path.join(self.admin.local_session_dir, 'server.csr')
        chain_pem_path = os.path.join(self.admin.local_session_dir, 'chain.pem')
        dhparams_pem_path = os.path.join(self.admin.local_session_dir, 'dhparams.pem')

        lib.execute_command(['openssl', 'genrsa', '-out', server_key_path, '1024'])
        #lib.execute_command('openssl req -batch -new -key %s -out %s' % (server_key_path, server_csr_path))
        lib.execute_command('openssl req -batch -new -x509 -key %s -out %s -days 365' % (server_key_path, chain_pem_path))
        lib.execute_command('openssl dhparam -2 -out %s 1024' % (dhparams_pem_path))  # normally 2048, but smaller size here for speed

        # client side environment variables
        os.environ['irodsSSLVerifyServer'] = "none"

        # add client irodsEnv settings
        clientEnvFile = self.admin.local_session_dir + "/irods_environment.json"
        with lib.file_backed_up(clientEnvFile):
            env = {
                'irods_client_server_policy': 'CS_NEG_REQUIRE',
                'irods_ssl_verify_server': 'none',
            }
            lib.update_json_file_from_dict(clientEnvFile, env)

            # server reboot with new server side environment variables
            IrodsController(IrodsConfig(
                    injected_environment={
                        'irodsSSLCertificateChainFile': chain_pem_path,
                        'irodsSSLCertificateKeyFile': server_key_path,
                        'irodsSSLDHParamsFile': dhparams_pem_path})
                    ).restart()

            # do the encrypted put
            filename = "encryptedfile.txt"
            lib.create_local_testfile(filename)
            self.admin.assert_icommand(['iinit', self.admin.password])  # reinitialize
            # small file
            self.admin.assert_icommand("iput " + filename)  # encrypted put - small file
            self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)  # should be listed
            # large file
            largefilename = "BIGencryptedfile.txt"
            lib.make_file(largefilename, 60*(2**20))
            #os.system("ls -al "+largefilename)
            self.admin.assert_icommand("iput " + largefilename)  # encrypted put - large file
            self.admin.assert_icommand("ils -L " + largefilename, 'STDOUT_SINGLELINE', largefilename)  # should be listed

        # reset client environment
        os.unsetenv('irodsSSLVerifyServer')

        # clean up
        os.remove(filename)
        os.remove(largefilename)

        # restart iRODS server without altered environment
        IrodsController().restart()

    def test_local_iput_with_really_big_file__ticket_1623(self):
        filename = "reallybigfile.txt"
        # file size larger than 32 bit int
        lib.make_file(filename, pow(2, 31) + 100)
        print("file size = [" + str(os.stat(filename).st_size) + "]")
        # should not be listed
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', [filename, "does not exist"])
        self.admin.assert_icommand("iput " + filename)  # iput
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)  # should be listed
        if os.path.exists(filename):
            os.unlink(filename)

    def test_local_iput(self):
        '''also needs to count and confirm number of replicas after the put'''
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        self.admin.assert_icommand_fail("ils -L " + datafilename, 'STDOUT_SINGLELINE', datafilename)  # should not be listed
        self.admin.assert_icommand("iput " + datafilename)  # iput
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE', datafilename)  # should be listed
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    def test_local_iput_overwrite(self):
        self.admin.assert_icommand_fail("iput " + self.testfile)  # fail, already exists
        self.admin.assert_icommand("iput -f " + self.testfile)  # iput again, force

    def test_local_iput_lower_checksum(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        self.admin.assert_icommand("iput -k " + datafilename)  # iput
        with open(datafilename, 'rb') as f:
            checksum = base64.b64encode(hashlib.sha256(f.read()).digest()).decode()
        self.admin.assert_icommand("ils -L", 'STDOUT_SINGLELINE', "sha2:" + checksum)  # check proper checksum
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    def test_local_iput_upper_checksum(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        self.admin.assert_icommand("iput -K " + datafilename)  # iput
        with open(datafilename, 'rb') as f:
            checksum = base64.b64encode(hashlib.sha256(f.read()).digest()).decode()
        self.admin.assert_icommand("ils -L", 'STDOUT_SINGLELINE', "sha2:" + checksum)  # check proper checksum
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    def test_local_iput_onto_specific_resource(self):
        # local setup
        datafilename = "anotherfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        self.admin.assert_icommand_fail("ils -L " + datafilename, 'STDOUT_SINGLELINE', datafilename)  # should not be listed
        self.admin.assert_icommand("iput -R " + self.testresc + " " + datafilename)  # iput
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE', datafilename)  # should be listed
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE', self.testresc)  # should be listed
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    @unittest.skipIf(True, 'Enable once #2634 is resolved')
    def test_local_iput_interrupt_directory(self):
        # local setup
        datadir = "newdatadir"
        os.makedirs(datadir)
        datafiles = ["file1", "file2", "file3", "file4", "file5", "file6", "file7"]
        for datafilename in datafiles:
            print("-------------------")
            print("creating " + datafilename + "...")
            localpath = datadir + "/" + datafilename
            lib.make_file(localpath, 2**20)
        restartfile = "collectionrestartfile"
        # assertions
        iputcmd = "iput -X " + restartfile + " -r " + datadir
        if os.path.exists(restartfile):
            os.unlink(restartfile)
        self.admin.interrupt_icommand(iputcmd, restartfile, 10)  # once restartfile reaches 10 bytes
        assert os.path.exists(restartfile), restartfile + " should now exist, but did not"
        print("  restartfile [" + restartfile + "] contents --> [")
        with open(restartfile, 'r') as f:
            for line in f:
                print(line)
        print("]")
        self.admin.assert_icommand("ils -L " + datadir, 'STDOUT_SINGLELINE', datadir)  # just to show contents
        self.admin.assert_icommand(iputcmd, 'STDOUT_SINGLELINE', "File last completed")  # confirm the restart
        for datafilename in datafiles:
            self.admin.assert_icommand("ils -L " + datadir, 'STDOUT_SINGLELINE', datafilename)  # should be listed
        # local cleanup
        lib.execute_command(['rm', '-rf', datadir])
        if os.path.exists(restartfile):
            os.unlink(restartfile)

    @unittest.skipIf(True, 'Enable once race conditions fixed, see #2634')
    def test_local_iput_interrupt_largefile(self):
        # local setup
        datafilename = 'bigfile'
        file_size = int(6 * pow(10, 8))
        lib.make_file(datafilename, file_size)
        restartfile = 'bigrestartfile'
        iputcmd = 'iput --lfrestart {0} {1}'.format(restartfile, datafilename)
        if os.path.exists(restartfile):
            os.unlink(restartfile)
        self.admin.interrupt_icommand(iputcmd, restartfile, 300)  # once restartfile reaches 300 bytes
        time.sleep(2)  # wait for all interrupted threads to exit
        assert os.path.exists(restartfile), restartfile + " should now exist, but did not"
        print("  restartfile [" + restartfile + "] contents --> [")
        with open(restartfile, 'r') as f:
            for line in f:
                print(line)
        print("]")
        today = datetime.date.today()
        # length should not be zero
        self.admin.assert_icommand_fail("ils -L " + datafilename, 'STDOUT_SINGLELINE', [" 0 " + today.isoformat(), datafilename])
        # confirm the restart
        self.admin.assert_icommand(iputcmd, 'STDOUT_SINGLELINE', datafilename + " was restarted successfully")
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE',
                                   [" " + str(os.stat(datafilename).st_size) + " " + today.isoformat(), datafilename])  # length should be size on disk
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)
        if os.path.exists(restartfile):
            os.unlink(restartfile)

    @unittest.skip('N/A for S3')
    def test_local_iput_physicalpath_no_permission(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        self.admin.assert_icommand("iput -p /newfileinroot.txt " + datafilename, 'STDERR_SINGLELINE',
                                   ["UNIX_FILE_CREATE_ERR", "Permission denied"])  # should fail to write
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    @unittest.skip('N/A for S3')
    def test_local_iput_physicalpath(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        fullpath = IrodsConfig().irods_directory + "/newphysicalpath.txt"
        self.admin.assert_icommand("iput -p " + fullpath + " " + datafilename)  # should complete
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE', datafilename)  # should be listed
        self.admin.assert_icommand("ils -L " + datafilename, 'STDOUT_SINGLELINE', fullpath)  # should be listed
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    @unittest.skip('N/A for S3')
    def test_admin_local_iput_relative_physicalpath_into_server_bin(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        relpath = "relativephysicalpath.txt"
        # should disallow relative path
        self.admin.assert_icommand("iput -p " + relpath + " " + datafilename, 'STDERR_SINGLELINE', "absolute")
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    @unittest.skip('N/A for S3')
    def test_local_iput_relative_physicalpath_into_server_bin(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        relpath = "relativephysicalpath.txt"
        self.user0.assert_icommand("iput -p " + relpath + " " + datafilename, 'STDERR_SINGLELINE', "absolute")  # should error
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    def test_local_iput_with_changed_target_filename(self):
        # local setup
        datafilename = "newfile.txt"
        with open(datafilename, 'wt') as f:
            print("TESTFILE -- [" + datafilename + "]", file=f, end='')
        # assertions
        changedfilename = "different.txt"
        self.admin.assert_icommand("iput " + datafilename + " " + changedfilename)  # should complete
        self.admin.assert_icommand("ils -L " + changedfilename, 'STDOUT_SINGLELINE', changedfilename)  # should be listed
        # local cleanup
        if os.path.exists(datafilename):
            os.unlink(datafilename)

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing: Lists Vault files")
    def test_iput_overwrite_others_file__ticket_2086(self):
        # pre state
        self.admin.assert_icommand("ils -L", 'STDOUT_SINGLELINE', self.testfile)  # should be listed

        # local setup
        filename = "overwritefile.txt"
        filepath = lib.create_local_testfile(filename)

        # alice tries to put
        homepath = "/home/" + self.admin.username + "/" + self.admin._session_id + "/" + self.testfile
        logicalpath = "/" + self.admin.zone_name + homepath
        self.user0.assert_icommand("iput " + filepath + " " + logicalpath, 'STDERR_SINGLELINE', "CAT_NO_ACCESS_PERMISSION")  # iput

        # check physicalpaths (of all replicas)
        out, _, _ = self.admin.run_icommand(['ils', '-L'])
        print("[ils -L]:")
        print("[" + out + "]")
        lines = out.splitlines()
        for i in range(0, len(lines) - 1):
            if "0 demoResc" in lines[i]:
                if "/session-" in lines[i + 1]:
                    l = lines[i + 1]
                    physicalpath = l.split()[1]
                    # check file is on disk
                    print("[ls -l " + physicalpath + "]:")
                    lib.execute_command("ls -l " + physicalpath)
                    assert os.path.exists(physicalpath)

        # local cleanup
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_iput_with_purgec(self):
        # local setup
        filename = "purgecfile.txt"
        filepath = os.path.abspath(filename)
        with open(filepath, 'wt') as f:
            print("TESTFILE -- [" + filepath + "]", file=f, end='')

        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput -f --purgec "+filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.') # get file and purge 'cached' replica
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])  # should be listed once
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", filename])  # should be listed only once
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", filename])  # should be listed only once

        # local cleanup
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_local_iput_with_force_and_destination_resource__ticket_1706(self):
        # local setup
        filename = "iputwithforceanddestination.txt"
        filepath = lib.create_local_testfile(filename)
        doublefile = "doublefile.txt"
        lib.execute_command("cat %s %s > %s" % (filename, filename, doublefile), use_unsafe_shell=True)
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)  # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)  # replicate to test resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)
        # overwrite test repl with different data
        self.admin.assert_icommand("iput -f -R %s %s %s" % (self.testresc, doublefile, filename))
        # default resource should have dirty copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " " + filename])
        # default resource should not have doublesize file
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " " + doublesize + " ", " " + filename])
        # targeted resource should have new double clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " " + doublesize + " ", "& " + filename])
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    ###################
    # irepl
    ###################

    def test_irepl_invalid_input(self):
        # local setup
        filename = "somefile.txt"
        filepath = lib.create_local_testfile(filename)
        # assertions
        # should not be listed
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")
        self.admin.assert_icommand("iput " + filename)                                                 # put file
        # for debugging
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)
        # replicate to bad resource
        self.admin.assert_icommand("irepl -R nonresc " + filename, 'STDERR_SINGLELINE', "SYS_RESC_DOES_NOT_EXIST")
        self.admin.assert_icommand("irm -f " + filename)                                               # cleanup file
        # local cleanup
        os.remove(filepath)

#    def test_irepl_multithreaded(self):
#        # local setup
#        filename = "largefile.txt"
#        lib.make_file(filename, 64*1024*1024, 'arbitrary')
#        # assertions
#        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")     # should not be listed
#        self.admin.assert_icommand("iput " + filename)                                 # put file
#        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)             # for debugging
#        self.admin.assert_icommand("irepl -R " + self.testresc + " -N 3 " + filename)      # replicate to test resource
#        # test resource should be listed
#        self.admin.assert_icommand("ils -l " + filename, 'STDOUT_SINGLELINE', self.testresc)
#        self.admin.assert_icommand("irm -f " + filename)                               # cleanup file
#        # local cleanup
#        os.remove(filename)

    def test_irepl_update_replicas(self):
        # local setup
        filename = "updatereplicasfile.txt"
        filepath = lib.create_local_testfile(filename)
        hostname = lib.get_hostname()
        hostuser = getpass.getuser()
        doublefile = "doublefile.txt"
        lib.execute_command("cat %s %s > %s" % (filename, filename, doublefile), use_unsafe_shell=True)

        # assertions
        self.admin.assert_icommand("iadmin mkresc thirdresc s3 %s:/%s/tmp/%s/thirdrescVault %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")   # create third resource

        self.admin.assert_icommand("iadmin mkresc fourthresc s3 %s:/%s/tmp/%s/fourthrescVault %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")  # create fourth resource

        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")              # should not be listed
        self.admin.assert_icommand("iput " + filename)                                         # put file
        # replicate to test resource
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)
        # replicate to third resource
        self.admin.assert_icommand("irepl -R thirdresc " + filename)
        # replicate to fourth resource
        self.admin.assert_icommand("irepl -R fourthresc " + filename)
        # repave overtop test resource
        self.admin.assert_icommand("iput -f -R " + self.testresc + " " + doublefile + " " + filename)
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)                       # for debugging

        # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " & " + filename])
        # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 3 ", " & " + filename])

        self.admin.assert_icommand(['irepl', '-R', 'fourthresc', filename])                # update last replica

        # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " & " + filename])
        # should have a dirty copy
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 3 ", " & " + filename])

        self.admin.assert_icommand("irepl -a " + filename)                                # update all replicas

        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        # should have a clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 3 ", " & " + filename])

        self.admin.assert_icommand("irm -f " + filename)                                   # cleanup file
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
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput -R " + self.testresc + " " + filename)       # put file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand("irepl " + filename)               # replicate to default resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand(['irepl', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop default resource
        # should not have a replica 2
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        self.admin.assert_icommand(['irepl', '-R', self.testresc, filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop test resource
        # should not have a replica 2
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_third_replica__ticket_1705(self):
        # local setup
        filename = "thirdreplicatest.txt"
        filepath = lib.create_local_testfile(filename)
        hostname = lib.get_hostname()
        hostuser = getpass.getuser()
        # assertions
        self.admin.assert_icommand("iadmin mkresc thirdresc s3 %s:/%s/tmp/%s/thirdrescVault %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")  # create third resource


        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)                            # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)      # replicate to test resource
        self.admin.assert_icommand("irepl -R thirdresc " + filename)              # replicate to third resource
        self.admin.assert_icommand(['irepl', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop default resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand(['irepl', '-R', self.testresc, filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop test resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand(['irepl', '-R', 'thirdresc', filename], 'STDERR', 'SYS_NOT_ALLOWED') # replicate overtop third resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        # should not have a replica 3
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 3 ", " & " + filename])
        # should not have a replica 4
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 4 ", " & " + filename])
        self.admin.assert_icommand("irm -f " + filename)                          # cleanup file
        self.admin.assert_icommand("iadmin rmresc thirdresc")                   # remove third resource
        # local cleanup
        os.remove(filepath)

    def test_irepl_over_existing_bad_replica__ticket_1705(self):
        # local setup
        filename = "reploverwritebad.txt"
        filepath = lib.create_local_testfile(filename)
        doublefile = "doublefile.txt"
        lib.execute_command("cat %s %s > %s" % (filename, filename, doublefile), use_unsafe_shell=True)
        doublesize = str(os.stat(doublefile).st_size)
        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)                            # put file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)      # replicate to test resource
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', filename)          # for debugging
        # overwrite default repl with different data
        self.admin.assert_icommand("iput -f %s %s" % (doublefile, filename))
        # default resource should have clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " & " + filename])
        # default resource should have new double clean copy
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", " " + doublesize + " ", " & " + filename])
        # test resource should not have doublesize file
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE',
                                        [" 1 " + self.testresc, " " + doublesize + " ", "  " + filename])
        # replicate back onto test resource
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + filename)
        # test resource should have new clean doublesize file
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE',
                                   [" 1 " + self.testresc, " " + doublesize + " ", " & " + filename])
        # should not have a replica 2
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", " & " + filename])
        # local cleanup
        os.remove(filepath)
        os.remove(doublefile)

    # repl update ( repave old copies )
    # walk through command line switches

    def test_irepl_with_purgec(self):
        # local setup
        filename = "purgecreplfile.txt"
        filepath = os.path.abspath(filename)
        with open(filepath, 'wt') as f:
            print("TESTFILE -- [" + filepath + "]", file=f, end='')

        # assertions
        self.admin.assert_icommand("ils -L " + filename, 'STDERR_SINGLELINE', "does not exist")  # should not be listed
        self.admin.assert_icommand("iput " + filename)  # put file
        self.admin.assert_icommand("irepl -R " + self.testresc + " --purgec " + filename, 'STDOUT', 'Specifying a minimum number of replicas to keep is deprecated.')  # replicate to test resource
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 0 ", filename])  # should be trimmed
        self.admin.assert_icommand("ils -L " + filename, 'STDOUT_SINGLELINE', [" 1 ", filename])  # should be listed once
        self.admin.assert_icommand_fail("ils -L " + filename, 'STDOUT_SINGLELINE', [" 2 ", filename])  # should be listed only once

        # local cleanup
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_irepl_with_admin_mode(self):
        lib.touch("file.txt")
        for i in range(0, 100):
            self.user0.assert_icommand("iput file.txt " + str(i) + ".txt", "EMPTY")
        homepath = "/" + self.admin.zone_name + "/home/" + \
            self.user0.username + "/" + self.user0._session_id
        self.admin.assert_icommand("irepl -r -M -R " + self.testresc + " " + homepath, "EMPTY")  # creates replica

    ###################
    # irm
    ###################

    def test_irm_doesnotexist(self):
        self.admin.assert_icommand_fail("irm doesnotexist")  # does not exist

    def test_irm(self):
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.admin.assert_icommand("irm " + self.testfile)  # remove from grid
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDERR_SINGLELINE', self.testfile)  # should be deleted
        trashpath = "/" + self.admin.zone_name + "/trash/home/" + self.admin.username + \
            "/" + self.admin._session_id
        # should be in trash
        self.admin.assert_icommand("ils -L " + trashpath + "/" + self.testfile, 'STDOUT_SINGLELINE', self.testfile)

    def test_irm_force(self):
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.admin.assert_icommand("irm -f " + self.testfile)  # remove from grid
        self.admin.assert_icommand_fail("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be deleted
        trashpath = "/" + self.admin.zone_name + "/trash/home/" + self.admin.username + \
            "/" + self.admin._session_id
        # should not be in trash
        self.admin.assert_icommand_fail("ils -L " + trashpath + "/" + self.testfile, 'STDOUT_SINGLELINE', self.testfile)

    def test_irm_specific_replica(self):
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.admin.assert_icommand("irepl -R " + self.testresc + " " + self.testfile)  # creates replica
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed twice
        self.admin.assert_icommand("irm -n 0 " + self.testfile, 'STDOUT', 'deprecated')  # remove original from grid
        # replica 1 should be there
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', ["1 " + self.testresc, self.testfile])
        self.admin.assert_icommand_fail("ils -L " + self.testfile, 'STDOUT_SINGLELINE',
                                        ["0 " + self.admin.default_resource, self.testfile])  # replica 0 should be gone
        trashpath = "/" + self.admin.zone_name + "/trash/home/" + self.admin.username + \
            "/" + self.admin._session_id
        self.admin.assert_icommand_fail("ils -L " + trashpath + "/" + self.testfile, 'STDOUT_SINGLELINE',
                                        ["0 " + self.admin.default_resource, self.testfile])  # replica should not be in trash

    def test_irm_recursive_file(self):
        self.admin.assert_icommand("ils -L " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.admin.assert_icommand("irm -r " + self.testfile)  # should not fail, even though a collection

    def test_irm_recursive(self):
        self.admin.assert_icommand("icp -r " + self.testdir + " copydir")  # make a dir copy
        self.admin.assert_icommand("ils -L ", 'STDOUT_SINGLELINE', "copydir")  # should be listed
        self.admin.assert_icommand("irm -r copydir")  # should remove
        self.admin.assert_icommand_fail("ils -L ", 'STDOUT_SINGLELINE', "copydir")  # should not be listed

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing")
    def test_irm_with_read_permission(self):
        self.user0.assert_icommand("icd ../../public")  # switch to shared area
        self.user0.assert_icommand("ils -AL " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.user0.assert_icommand_fail("irm " + self.testfile)  # read perm should not be allowed to remove
        self.user0.assert_icommand("ils -AL " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should still be listed

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing")
    def test_irm_with_write_permission(self):
        self.user1.assert_icommand("icd ../../public")  # switch to shared area
        self.user1.assert_icommand("ils -AL " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.user1.assert_icommand_fail("irm " + self.testfile)  # write perm should not be allowed to remove
        self.user1.assert_icommand("ils -AL " + self.testfile, 'STDOUT_SINGLELINE', self.testfile)  # should still be listed

    def test_irm_repeated_many_times(self):
        # repeat count
        many_times = 50
        # create file
        filename = "originalfile.txt"
        filepath = os.path.abspath(filename)
        lib.make_file(filepath, 15)
        # define
        trashpath = "/" + self.admin.zone_name + "/trash/home/" + self.admin.username + \
            "/" + self.admin._session_id
        # loop
        for _ in range(many_times):
            self.admin.assert_icommand("iput " + filename, "EMPTY")  # put the file
            self.admin.assert_icommand("irm " + filename, "EMPTY")  # delete the file
            self.admin.assert_icommand("ils -L " + trashpath, 'STDOUT_SINGLELINE', filename)

    ###################
    # irmtrash
    ###################

    def test_irmtrash_admin(self):
        # assertions
        self.admin.assert_icommand("irm " + self.testfile)  # remove from grid
        self.admin.assert_icommand("ils -rL /" + self.admin.zone_name + "/trash/home/" +
                                   self.admin.username + "/", 'STDOUT_SINGLELINE', self.testfile)  # should be listed
        self.admin.assert_icommand("irmtrash")  # should be listed
        self.admin.assert_icommand_fail("ils -rL /" + self.admin.zone_name + "/trash/home/" +
                                        self.admin.username + "/", 'STDOUT_SINGLELINE', self.testfile)  # should be deleted

    ###################
    # itrim
    ###################

    def test_itrim_with_admin_mode(self):
        lib.touch("file.txt")
        for i in range(100):
            self.user0.assert_icommand("iput file.txt " + str(i) + ".txt", "EMPTY")

        filename1 = "itrimadminmode1.txt"
        filename2 = "itrimadminmode2.txt"
        filesize = int(pow(2, 20) + pow(10,5))

        filesizeMB = round(float(2 * filesize)/1048576, 3)
        lib.make_file(filename1, filesize)
        lib.make_file(filename2, filesize)

        self.user0.assert_icommand("iput {filename1}".format(**locals()), 'EMPTY')
        self.user0.assert_icommand("iput {filename2}".format(**locals()), 'EMPTY')

        homepath = self.user0.session_collection
        self.user0.assert_icommand("irepl -R " + self.testresc + " -r " + homepath, "EMPTY")  # creates replica
        self.admin.assert_icommand("itrim -M -N1 -r " + homepath, 'STDOUT_SINGLELINE', "Total size trimmed = " + str(filesizeMB) +" MB. Number of files trimmed = 102.")

        #local file cleanup
        os.unlink(os.path.abspath("file.txt"))
        os.unlink(os.path.abspath(filename1))
        os.unlink(os.path.abspath(filename2))

    def test_itrim_no_op(self):
        collection = self.admin.session_collection
        filename = self.testfile
        repl_resource = self.anotherresc

        # check that test file is there
        self.admin.assert_icommand("ils {filename}".format(**locals()), 'STDOUT_SINGLELINE', filename)

        # replicate test file
        self.admin.assert_icommand("irepl -R {repl_resource} {filename}".format(**locals()), 'EMPTY')

        # check replication
        self.admin.assert_icommand("ils -L {filename}".format(**locals()), 'STDOUT_SINGLELINE', repl_resource)

        # count replicas
        repl_count = self.admin.run_icommand('''iquest "%s" "SELECT count(DATA_ID) where COLL_NAME ='{collection}' and DATA_NAME ='{filename}'"'''.format(**locals()))[0]

        # try to trim down to repl_count
        self.admin.assert_icommand("itrim -N {repl_count} {filename}".format(**locals()), 'STDOUT_SINGLELINE', "Total size trimmed = 0.000 MB. Number of files trimmed = 0.")

    def test_itrim_displays_incorrect_count__ticket_3531(self):
        filename = "itrimcountwrong.txt"
        filesize = int(pow(2, 20) + pow(10,5))

        filesizeMB = round(float(filesize)/1048576, 3)

        lib.make_file(filename, filesize)
        filepath = os.path.abspath(filename)

        put_resource = self.testresc
        repl_resource = self.anotherresc

        # put file
        self.user0.assert_icommand("iput -R {put_resource} {filename}".format(**locals()), 'EMPTY')

        # check if file was added
        self.user0.assert_icommand("ils -L", 'STDOUT_SINGLELINE', filename)

        # replicate test file
        self.user0.assert_icommand("irepl -R {repl_resource} {filename}".format(**locals()), 'EMPTY')

        # check replication
        self.user0.assert_icommand("ils -L", 'STDOUT_MULTILINE', [put_resource, repl_resource])

        # trim the file
        self.user0.assert_icommand("itrim -N 1 -S {put_resource} {filename}".format(**locals()), 'STDOUT_SINGLELINE', "Total size trimmed = " + str(filesizeMB) +" MB. Number of files trimmed = 1.")

        # local cleanup
        if os.path.exists(filepath):
           os.unlink(filepath)

    @unittest.skip("skipping because this isn't really an s3 test and server changed")
    def test_itrim_returns_on_negative_status__ticket_3531(self):
        # local setup
        filename = "filetotesterror.txt"
        filepath = lib.create_local_testfile(filename)

        itrimReplResc = "itrimReplResc"
        self.admin.assert_icommand("iadmin mkresc {itrimReplResc} replication".format(**locals()), 'STDOUT_SINGLELINE', "replication")
        resc1 = self.testresc
        resc2 = self.anotherresc

        self.user0.assert_icommand("iput -R {resc1} {filename}".format(**locals()), 'EMPTY')

        # check if file was added
        self.user0.assert_icommand("ils -L", 'STDOUT_SINGLELINE', filename)

        # add resources to the replNode
        self.admin.assert_icommand("iadmin addchildtoresc {itrimReplResc} {resc1}".format(**locals()), 'EMPTY')
        self.admin.assert_icommand("iadmin addchildtoresc {itrimReplResc} {resc2}".format(**locals()), 'EMPTY')

        # rebalance
        self.admin.assert_icommand("iadmin modresc {itrimReplResc} rebalance".format(**locals()), 'EMPTY')

        # trim the file
        rc, _, _ = self.user0.assert_icommand("itrim -n0 {resc2} {filename}".format(**locals()), 'STDERR', 'status = -402000 USER_INCOMPATIBLE_PARAMS')
        self.assertNotEqual(rc, 0, 'itrim should have non-zero error code on trim failure')

        #local cleanup
        self.admin.assert_icommand("iadmin rmchildfromresc {itrimReplResc} {resc1}".format(**locals()), 'EMPTY')
        self.admin.assert_icommand("iadmin rmchildfromresc {itrimReplResc} {resc2}".format(**locals()), 'EMPTY')
        self.admin.assert_icommand("iadmin rmresc {itrimReplResc}".format(**locals()), 'EMPTY')
        self.user0.assert_icommand("irm -f {filename}".format(**locals()), 'EMPTY')
        if os.path.exists(filepath):
            os.unlink(filepath)


    # tests add for cacheless S3

    def test_iput_large_file_over_smaller(self):

        file1 = "f1"
        file2 = "f2"
        filename_get = "f3"

        file1_size = pow(2,20)
        file2_size = 512*pow(2,20)

        # create small and large files
        lib.make_file(file1, file1_size)
        lib.make_file(file2, file2_size)

        try:

            # put small file
            self.admin.assert_icommand("iput %s %s" % (file1, file2))  # iput

            # write over small file with big file
            self.admin.assert_icommand("iput -f %s" % file2)  # iput
            self.admin.assert_icommand("ils -L %s" % file2, 'STDOUT_SINGLELINE', str(file2_size))  # should be listed

            # get the file under a new name
            self.admin.assert_icommand("iget -f %s %s" % (file2, filename_get))

            # make sure the file that was put and got are the same
            self.admin.assert_icommand("diff %s %s " % (file2, filename_get), 'EMPTY')

        finally:

            # local cleanup
            self.admin.assert_icommand("irm -f " + file2, 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)
            if os.path.exists(filename_get):
                os.unlink(filename_get)


    def test_iput_small_file_over_larger(self):

        file1 = "f1"
        file2 = "f2"
        filename_get = "f3"

        file1_size = 512*pow(2,20)
        file2_size = pow(2,20)

        # create small and large files
        lib.make_file(file1, file1_size)
        lib.make_file(file2, file2_size)

        try:
            # put large file
            self.admin.assert_icommand("iput %s %s" % (file1, file2))  # iput

            # write over large file with small file
            self.admin.assert_icommand("iput -f %s" % file2)  # iput
            self.admin.assert_icommand("ils -L %s" % file2, 'STDOUT_SINGLELINE', str(file2_size))  # should be listed

            # get the file under a new name
            self.admin.assert_icommand("iget -f %s %s" % (file2, filename_get))

            # make sure the file that was put and got are the same
            self.admin.assert_icommand("diff %s %s " % (file2, filename_get), 'EMPTY')

        finally:

            # local cleanup
            self.admin.assert_icommand("irm -f " + file2, 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)
            if os.path.exists(filename_get):
                os.unlink(filename_get)


    @unittest.skip("simultaneous opens are no longer allowed")
    def test_simultaneous_open_writes(self):

        rule_file_path = 'test_simultaneous_open_writes.r'
        target_obj = '/'.join([self.user0.session_collection, 'file1.txt'])

        rule_str = '''
test_simultaneous_open_writes {{
            msiDataObjCreate("{target_obj}", "destRescName=demoResc", *fd)
            msiDataObjWrite(*fd, "abcd", *len)
            msiDataObjClose(*fd, *status)


            # multiple open simultaneously
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd1)
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd2)
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd3)

            msiDataObjLseek(*fd1, 4, "SEEK_SET", *status)
            msiDataObjLseek(*fd2, 14, "SEEK_SET", *status)
            msiDataObjLseek(*fd3, 24, "SEEK_SET", *status)

            msiDataObjWrite(*fd3, "CCCCCCCCCC", *len)
            msiDataObjWrite(*fd2, "BBBBBBBBBB", *len)
            msiDataObjWrite(*fd1, "AAAAAAAAAA", *len)

            msiDataObjClose(*fd1, *status)
            msiDataObjClose(*fd2, *status)
            msiDataObjClose(*fd3, *status)

            # open write close consecutively
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd1)
            msiDataObjLseek(*fd1, 34, "SEEK_SET", *status)
            msiDataObjWrite(*fd1, "XXXXXXXXXX", *len)
            msiDataObjClose(*fd1, *status)

            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd2)
            msiDataObjLseek(*fd2, 44, "SEEK_SET", *status)
            msiDataObjWrite(*fd2, "YYYYYYYYYY", *len)
            msiDataObjClose(*fd2, *status)

            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd3)
            msiDataObjLseek(*fd3, 54, "SEEK_SET", *status)
            msiDataObjWrite(*fd3, "ZZZZZZZZZZ", *len)
            msiDataObjClose(*fd3, *status)
        }}
INPUT null
OUTPUT ruleExecOut

        '''.format(**locals())

        try:
            with open(rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str)

            self.user0.assert_icommand("irule -F %s" % rule_file_path)
            self.user0.assert_icommand("iget {target_obj} -".format(**locals()), 'STDOUT_SINGLELINE', 'abcdAAAAAAAAAABBBBBBBBBBCCCCCCCCCCXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZ')
        finally:
            # local cleanup
            self.user0.assert_icommand("irm -f {target_obj}".format(**locals()))
            if os.path.exists(rule_file_path):
                os.unlink(rule_file_path)

    @unittest.skipIf(True, "skip until issue 5018 fixed")
    def test_read_write_zero_length_file_issue_1890(self):

        rule_file_path = 'test_issue_1890.r'
        target_obj = '/'.join([self.user0.session_collection, 'file1.txt'])

        rule_str = '''
test_open_write_close {{
    msiDataObjCreate("{target_obj}", "destRescName=demoResc", *fd)
    msiDataObjClose(*fd, *status)
    msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_RDWR", *fd1)
    msiDataObjRead(*fd1, 10, *buf)
    msiDataObjLseek(*fd1, 0, "SEEK_SET", *status)
    msiDataObjWrite(*fd1, "XXXXXXXXXX", *len)
    msiDataObjLseek(*fd1, 5, "SEEK_SET", *status)
    msiDataObjWrite(*fd1, "YYYY", *len)
    msiDataObjClose(*fd1, *status)

}}
INPUT null
OUTPUT ruleExecOut

        '''.format(**locals())

        try:
            with open(rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str)

            self.user0.assert_icommand("irule -F %s" % rule_file_path)
            self.user0.assert_icommand("iget {target_obj} -".format(**locals()), 'STDOUT_SINGLELINE', 'XXXXXYYYYX')
        finally:
            # local cleanup
            self.user0.assert_icommand("irm -f {target_obj}".format(**locals()))
            if os.path.exists(rule_file_path):
                os.unlink(rule_file_path)

    def test_seek_end(self):

        rule_file_path = 'test_seek_end.r'
        target_obj = '/'.join([self.user0.session_collection, 'file1.txt'])

        rule_str = '''
test_seek_end {{
            msiDataObjCreate("{target_obj}", "destRescName=demoResc", *fd)
            msiDataObjWrite(*fd, "abcdefg", *len)
            msiDataObjClose(*fd, *status)

            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd1)
            msiDataObjLseek(*fd1, 0, "SEEK_END", *status)
            msiDataObjWrite(*fd1, "hijk", *len)
            msiDataObjClose(*fd1, *status)
        }}
INPUT null
OUTPUT ruleExecOut

        '''.format(**locals())

        try:
            with open(rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str)

            self.user0.assert_icommand("irule -F %s" % rule_file_path)
            self.user0.assert_icommand("iget {target_obj} -".format(**locals()), 'STDOUT_SINGLELINE', 'abcdefghijk')
        finally:
            # local cleanup
            self.user0.assert_icommand("irm -f {target_obj}".format(**locals()))
            if os.path.exists(rule_file_path):
                os.unlink(rule_file_path)


    def test_seek_cur(self):

        rule_file_path = 'test_seek_cur.r'
        target_obj = '/'.join([self.user0.session_collection, 'file1.txt'])

        rule_str = '''
test_seek_end {{
            msiDataObjCreate("{target_obj}", "destRescName=demoResc", *fd)
            msiDataObjWrite(*fd, "abcdefg", *len)
            msiDataObjClose(*fd, *status)

            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd1)
            msiDataObjLseek(*fd1, 2, "SEEK_SET", *status)
            msiDataObjLseek(*fd1, 2, "SEEK_CUR", *status)
            msiDataObjWrite(*fd1, "hijk", *len)
            msiDataObjClose(*fd1, *status)
        }}
INPUT null
OUTPUT ruleExecOut

        '''.format(**locals())

        try:
            with open(rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str)

            self.user0.assert_icommand("irule -F %s" % rule_file_path)
            self.user0.assert_icommand("iget {target_obj} -".format(**locals()), 'STDOUT_SINGLELINE', 'abcdhijk')
        finally:
            # local cleanup
            self.user0.assert_icommand("irm -f {target_obj}".format(**locals()))
            if os.path.exists(rule_file_path):
                os.unlink(rule_file_path)


    def test_small_write_read_in_large_file(self):

        target_obj = '/'.join([self.user0.session_collection, 'f1'])
        rule_str_write = '''
test_small_write {{
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_WRONLY", *fd1)
            msiDataObjLseek(*fd1, 1024000, "SEEK_SET", *status)
            msiDataObjWrite(*fd1, "abcdef", *len)
            msiDataObjClose(*fd1, *status)
        }}
INPUT null
OUTPUT ruleExecOut

        '''.format(**locals())


        rule_str_read = '''
test_small_read {{
            msiDataObjOpen("objPath={target_obj}++++rescName=demoResc++++openFlags=O_RDONLY", *fd1)
            msiDataObjLseek(*fd1, 1024000, "SEEK_SET", *status)
            msiDataObjRead(*fd1, 6, *buf)
            msiDataObjClose(*fd1, *status)
            writeLine("stdout", "buf=*buf")
        }}
INPUT null
OUTPUT ruleExecOut
        '''.format(**locals())

        file1 = 'f1'
        write_rule_file_path = 'test_small_write.r'
        read_rule_file_path = 'test_small_read.r'

        try:
            file1_size = 512*pow(2,20)

            # create and put large file
            lib.make_file(file1, file1_size)
            self.user0.assert_icommand("iput {file1} {target_obj}".format(**locals()))  # iput

            with open(write_rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str_write)

            with open(read_rule_file_path, 'w') as rule_file:
                rule_file.write(rule_str_read)

            self.user0.assert_icommand("irule -F %s" % write_rule_file_path)

            before_read_time = time.time()
            self.user0.assert_icommand("irule -F %s" % read_rule_file_path, 'STDOUT_SINGLELINE', 'buf=abcdef')
            after_read_time = time.time()

            # small reads should not read entire file, make sure the read took less than three seconds
            # (should be much lower than that)
            self.assertLess(after_read_time - before_read_time, 3)


        finally:

            # local cleanup
            self.user0.assert_icommand("irm -f {target_obj}".format(**locals()), 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)

            if os.path.exists(write_rule_file_path):
                os.unlink(write_rule_file_path)

            if os.path.exists(read_rule_file_path):
                os.unlink(read_rule_file_path)

    @unittest.skipIf(True, "TODO")
    def test_detached_mode(self):

        # in non-topology HOSTNAME_3 is just local host so it really doesn't test detached mode
        # but in topology it will

        file1 = "f1"
        file2 = "f2"
        #resource_host = test.settings.HOSTNAME_3
        resource_host = "irods.org"

        # change demoResc to use detached mode

        s3_context_detached = "S3_DEFAULT_HOSTNAME=%s;S3_AUTH_FILE=%s;S3_REGIONNAME=%s;S3_RETRY_COUNT=2;S3_WAIT_TIME_SEC=3;S3_PROTO=%s;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_detached;S3_ENABLE_MD5=1;S3_ENABLE_MPU=%d" % (self.s3endPoint, self.keypairfile, self.s3region, self.proto, self.s3EnableMPU)
        s3_context_detached = 'S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        s3_context_detached += ';S3_AUTH_FILE=' + self.keypairfile
        s3_context_detached += ';S3_REGIONNAME=' + self.s3region
        s3_context_detached += ';S3_RETRY_COUNT=2'
        s3_context_detached += ';S3_WAIT_TIME_SEC=3'
        s3_context_detached += ';S3_PROTO=' + self.proto
        s3_context_detached += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        s3_context_detached += ';HOST_MODE=cacheless_detached'
        s3_context_detached += ';S3_ENABLE_MD5=1'
        s3_context_detached += ';S3_ENABLE_MPU=' + self.s3EnableMPU


        self.admin.assert_icommand("iadmin modresc demoResc context %s" % s3_context_detached , 'EMPTY')
        self.admin.assert_icommand("iadmin modresc demoResc host %s" % resource_host, 'EMPTY')

        # create file to put
        lib.make_file(file1, 100)

        try:

            # put small file
            self.admin.assert_icommand("iput %s" % file1)  # iput

            # get file
            self.admin.assert_icommand("iget %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.admin.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # local cleanup
            self.admin.assert_icommand("irm -f " + file1, 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)


    def test_attached_mode_invalid_resource_host(self):

        # in non-topology HOSTNAME_3 is just local host so it really doesn't test detached mode
        # but in topology it will

        file1 = "f1"
        #resource_host = test.settings.HOSTNAME_3
        resource_host = "irods.org"

        self.admin.assert_icommand("iadmin modresc demoResc host %s" % resource_host, 'EMPTY')

        # create file to put
        lib.make_file(file1, 100)

        try:

            # put small file
            self.admin.assert_icommand("iput %s" % file1, 'STDERR_SINGLELINE', 'USER_SOCK_CONNECT_ERR')  # iput

        finally:

            # local cleanup
            hostname = lib.get_hostname()
            self.admin.assert_icommand("iadmin modresc demoResc host %s" % hostname, 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)


    def recursive_register_from_s3_bucket(self):

        # create some files on s3
        if self.proto == 'HTTPS':
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key)
        else:
            s3_client = Minio(self.s3endPoint, access_key=self.aws_access_key_id, secret_key=self.aws_secret_access_key, secure=False)
        file_contents = b'random test data'
        f = io.BytesIO(file_contents)
        size = len(file_contents)

        s3_client.put_object(self.s3bucketname, 'basedir/f1', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/f2', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/f3', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir1/f1', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir1/f2', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir1/f3', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir2/f1', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir2/f2', f, size)
        f.seek(0)
        s3_client.put_object(self.s3bucketname, 'basedir/subdir2/f3', f, size)

        self.admin.assert_icommand("ireg -C /%s/basedir %s/basedir" % (self.s3bucketname, self.admin.session_collection))
        file_count = self.admin.run_icommand('''iquest "%s" "SELECT count(DATA_ID) where COLL_NAME like '%/basedir%'"''')[0]
        self.assertEquals(file_count, u'9\n')

    def test_copy_file_greater_than_chunk_size(self):

        try:

            file1 = "f1"
            file2 = "f2"
            lib.make_file(file1, 128*1024*1024, 'arbitrary')
            self.user0.assert_icommand("iput {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("imv {file1} {file2}".format(**locals()))  # imv

        finally:

            self.user0.assert_icommand("irm -f {file2}".format(**locals()))  # imv

            if os.path.exists(file1):
                os.unlink(file1)

    def test_put_get_small_file_in_repl_node(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            # create two s3 resources in repl node
            self.admin.assert_icommand("iadmin mkresc s3repl replication".format(**locals()), 'STDOUT_SINGLELINE', "replication")
            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/tmp/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc2 s3 %s:/%s/tmp/%s/s3resc2 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc3 s3 %s:/%s/tmp/%s/s3resc3 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc3", 'EMPTY')

            # create and put file
            file1 = "f1"
            lib.make_file(file1, 1024, 'arbitrary')
            file2 = "f1.get"
            self.user0.assert_icommand("iput -R s3repl {file1}".format(**locals()))  # iput

            # get file from first repl
            self.user0.assert_icommand("iget -n 0 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

            # get file from second repl
            self.user0.assert_icommand("iget -n 1 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

            # get file from second repl
            self.user0.assert_icommand("iget -n 2 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup
            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc3", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3repl", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc3", 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)

    def test_put_get_large_file_in_repl_node(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            # create two s3 resources in repl node
            self.admin.assert_icommand("iadmin mkresc s3repl replication".format(**locals()), 'STDOUT_SINGLELINE', "replication")
            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/tmp/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc2 s3 %s:/%s/tmp/%s/s3resc2 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc3 s3 %s:/%s/tmp/%s/s3resc3 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin addchildtoresc s3repl s3resc3", 'EMPTY')

            # create and put file
            file1 = "f1"
            file2 = "f1.get"
            lib.make_file(file1, 120*1024*1024)
            self.user0.assert_icommand("iput -R s3repl {file1}".format(**locals()))  # iput

            # get file from first repl
            self.user0.assert_icommand("iget -n 0 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

            # get file from second repl
            self.user0.assert_icommand("iget -n 1 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

            # get file from second repl
            self.user0.assert_icommand("iget -n 2 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup
            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc s3repl s3resc3", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3repl", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc3", 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)

    def test_put_get_file_between_parallel_buff_size_and_max_size_single_buffer(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            # create two s3 resources in repl node
            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/tmp/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            # create and put file
            file1 = "f1"
            file2 = "f1.get"
            lib.make_file(file1, 5*1024*1024)
            self.user0.assert_icommand("iput -R s3resc1 {file1}".format(**locals()))  # iput

            # get file from first repl
            self.user0.assert_icommand("iget -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup
            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            if os.path.exists(file1):
                os.unlink(file1)
            if os.path.exists(file2):
                os.unlink(file2)
