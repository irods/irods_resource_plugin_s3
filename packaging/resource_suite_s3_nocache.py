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
import psutil
import subprocess

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


if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from .. import test
from .. import lib
from . import s3plugin_lib
from ..configuration import IrodsConfig
from ..controller import IrodsController
from . import session

class Test_S3_NoCache_Base(session.make_sessions_mixin([('otherrods', 'rods')], [('alice', 'apass'), ('bobby', 'bpass')])):

    def __init__(self, *args, **kwargs):
        """Set up the cacheless test."""
        # if self.proto is defined use it else default to HTTPS
        if not hasattr(self, 'proto'):
            self.proto = 'HTTPS'

        # if self.archive_naming_policy is defined use it
        # else default to 'consistent'
        if not hasattr(self, 'archive_naming_policy'):
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
        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region = self.s3region,
                secure=(self.proto == 'HTTPS'))

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
            distro_str = ''.join(platform.linux_distribution()[:2]).replace(' ','').replace('.', '')
            self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d%H-%M-%S-%f-')
            self.s3bucketname += ''.join(random.choice(string.letters) for i in xrange(10))
            self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
            s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        self.testresc = "TestResc"
        self.testvault = "/" + self.testresc
        self.anotherresc = "AnotherResc"
        self.anothervault = "/" + self.anotherresc

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
        self.s3_context += ';S3_CACHE_DIR=/var/lib/irods'

        if hasattr(self, 's3DisableCopyObject'):
            self.s3DisableCopyObject = self.s3DisableCopyObject
            self.s3_context += ';S3_ENABLE_COPYOBJECT=0'

        if hasattr(self, 's3sse'):
            self.s3_context += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)

        self.s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy

        self.admin.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

        self.admin.assert_icommand("iadmin mkresc demoResc s3 " + hostname + ":/" + self.s3bucketname + "/demoResc " + self.s3_context, 'STDOUT_SINGLELINE', 's3')

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
        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        objects = s3_client.list_objects(self.s3bucketname, recursive=True)

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
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
        s3plugin_lib.remove_if_exists(localfile)

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
        s3plugin_lib.remove_if_exists(localfile)

    def test_local_iget_with_bad_option(self):
        # assertions
        self.admin.assert_icommand_fail("iget -z")  # run iget with bad option

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
        s3plugin_lib.remove_if_exists(filename)
        s3plugin_lib.remove_if_exists(updated_filename)
        s3plugin_lib.remove_if_exists(retrievedfile)

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
        s3plugin_lib.remove_if_exists(filepath)

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
        s3plugin_lib.remove_if_exists(filename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(restartfile)
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
        s3plugin_lib.remove_if_exists(restartfile)

    @unittest.skipIf(True, 'Enable once race conditions fixed, see #2634')
    def test_local_iput_interrupt_largefile(self):
        # local setup
        datafilename = 'bigfile'
        file_size = int(6 * pow(10, 8))
        lib.make_file(datafilename, file_size)
        restartfile = 'bigrestartfile'
        iputcmd = 'iput --lfrestart {0} {1}'.format(restartfile, datafilename)
        s3plugin_lib.remove_if_exists(restartfile)
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
        s3plugin_lib.remove_if_exists(datafilename)
        s3plugin_lib.remove_if_exists(restartfile)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(datafilename)

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
        s3plugin_lib.remove_if_exists(filepath)

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
        s3plugin_lib.remove_if_exists(filepath)

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
        self.admin.assert_icommand("iadmin mkresc thirdresc s3 %s:/%s/%s/thirdrescVault %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")   # create third resource

        self.admin.assert_icommand("iadmin mkresc fourthresc s3 %s:/%s/%s/fourthrescVault %s" %
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
        self.admin.assert_icommand("iadmin mkresc thirdresc unixfilesystem %s:/tmp/%s/thirdrescVault" %
                                   (hostname, hostuser), 'STDOUT_SINGLELINE', "Creating")  # create third resource
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
        s3plugin_lib.remove_if_exists(filepath)

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
        s3plugin_lib.remove_if_exists(filepath)

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
        s3plugin_lib.remove_if_exists(filepath)


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

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(filename_get)

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

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(filename_get)


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
            s3plugin_lib.remove_if_exists(rule_file_path)

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
            s3plugin_lib.remove_if_exists(rule_file_path)

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
            s3plugin_lib.remove_if_exists(rule_file_path)


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
            s3plugin_lib.remove_if_exists(rule_file_path)

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

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(write_rule_file_path)
            s3plugin_lib.remove_if_exists(read_rule_file_path)

    def test_detached_mode(self):

        try:
            file1 = "f1"
            file2 = "f2"
            resource_host = "irods.org"
            resource_name = 's3resc1'

            hostuser = getpass.getuser()

            s3_context = 'S3_DEFAULT_HOSTNAME=' + self.s3endPoint
            s3_context += ';S3_AUTH_FILE=' + self.keypairfile
            s3_context += ';S3_REGIONNAME=' + self.s3region
            s3_context += ';S3_RETRY_COUNT=2'
            s3_context += ';S3_WAIT_TIME_SEC=3'
            s3_context += ';S3_PROTO=' + self.proto
            s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
            s3_context += ';HOST_MODE=cacheless_detached'
            s3_context += ';S3_ENABLE_MD5=1'
            s3_context += ';S3_ENABLE_MPU=' + str(self.s3EnableMPU)
            s3_context += ';S3_CACHE_DIR=/var/lib/irods'

            if hasattr(self, 's3DisableCopyObject'):
                s3_context += ';S3_ENABLE_COPYOBJECT=0'

            self.admin.assert_icommand("iadmin mkresc %s s3 %s:/%s/%s/s3resc1 %s" %
                                   (resource_name, resource_host, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            # create file to put
            lib.make_file(file1, 100)

            # put small file
            self.admin.assert_icommand("iput -R %s %s" % (resource_name, file1))  # iput

            # get file
            self.admin.assert_icommand("iget %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.admin.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # local cleanup
            self.admin.assert_icommand("irm -f " + file1, 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

            # cleanup
            self.admin.assert_icommand("iadmin rmresc %s" % resource_name, 'EMPTY')


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

            s3plugin_lib.remove_if_exists(file1)

    def test_recursive_register_from_s3_bucket(self):

        # create some files on s3
        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

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

            s3plugin_lib.remove_if_exists(file1)

    def test_put_get_small_file_in_repl_node(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            # create two s3 resources in repl node
            self.admin.assert_icommand("iadmin mkresc s3repl replication".format(**locals()), 'STDOUT_SINGLELINE', "replication")
            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc2 s3 %s:/%s/%s/s3resc2 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc3 s3 %s:/%s/%s/s3resc3 %s" %
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

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    def test_put_get_large_file_in_repl_node(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            # create two s3 resources in repl node
            self.admin.assert_icommand("iadmin mkresc s3repl replication".format(**locals()), 'STDOUT_SINGLELINE', "replication")
            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc2 s3 %s:/%s/%s/s3resc2 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc s3resc3 s3 %s:/%s/%s/s3resc3 %s" %
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

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    def test_put_get_various_file_sizes(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")


            # Using defaults:
            #   S3_MPU_CHUNK = 5 MB
            #   CIRCULAR_BUFFER_SIZE = 2 (2 * 5MB = 10MB)
            #   maximum_size_for_single_buffer_in_megabytes = 32 MB

            file_sizes = [
                    10 * 1024 * 1024 - 1,  # Single thread, normal put (file size < circular buffer size)
                    10 * 1024 * 1024,      # Single thread, normal put (file size = circular buffer size)
                    10 * 1024 * 1024 + 1,  # Single thread, two parts per thread (file size > circular buffer size)
                    20 * 1024 * 1024 - 1,  # Single thread, two parts per thread (file size < 2*circular buffer size)
                    20 * 1024 * 1024,      # Single thread, three parts per thread (file size = 2*circular buffer size)
                    20 * 1024 * 1024 + 1,  # Single thread, three parts per thread (file size > 2*circular buffer size)
                    32 * 1024 * 1024 - 1,  # Single thread, multiple parts per thread
                    32 * 1024 * 1024,      # 16 threads, each same size, force cache due to size per thread < 5 MB
                    32 * 1024 * 1024 + 1,  # 16 threads, different sizes, force cache due to size per thread < 5 MB
                    80 * 1024 * 1024 - 1,  # 16 threads, different sizes, force cache due to size per thread < 5 MB
                    80 * 1024 * 1024,      # 16 threads, each same size, multipart upload, one part per thread
                    80 * 1024 * 1024 + 1,  # 16 threads, different sizes, multipart upload, one part per thread
                    160 * 1024 * 1024 - 1, # 16 threads, different sizes, multipart upload, one part per thread
                    160 * 1024 * 1024,     # 16 threads, each same size, multipart upload, one part per thread
                    160 * 1024 * 1024 + 1, # 16 threads, different sizes, multipart upload, two parts per thread
                    240 * 1024 * 1024 - 1, # 16 threads, different sizes, multipart upload, two parts per thread
                    240 * 1024 * 1024,     # 16 threads, each same size, multipart upload, two parts per thread
                    240 * 1024 * 1024 + 1, # 16 threads, different sizes, multipart upload, two parts per thread
                    320 * 1024 * 1024 - 1, # 16 threads, different sizes, multipart upload, two parts per thread
                    320 * 1024 * 1024,     # 16 threads, each same size, multipart upload, two parts per thread
                    320 * 1024 * 1024 + 1, # 16 threads, different sizes, multipart upload, three parts per thread
                    ];

            file1 = "f1"
            file2 = "f1.get"

            for file_size in file_sizes:

                # create and put file
                s3plugin_lib.make_arbitrary_file(file1, file_size)
                self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput

                # make sure file is right size
                self.user0.assert_icommand("ils -L %s" % file1, 'STDOUT_SINGLELINE', str(file_size))  # should be listed

                # get file from first repl
                self.user0.assert_icommand("iget -f %s %s" % (file1, file2))  # iput

                self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')

                # make sure the file that was put and got are the same
                self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    def test_rm_without_force(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file_size = 120*1024*1024;

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file_size)
            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput

            # remove file without force
            self.user0.assert_icommand("irm {file1}".format(**locals()))  # irm

            self.user0.assert_icommand("irmtrash", 'EMPTY')

        finally:

            # cleanup
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)

    def test_missing_keyfile(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = 'S3_DEFAULT_HOSTNAME=' + self.s3endPoint
            s3_context += ';S3_AUTH_FILE=doesnotexit.keypair'
            s3_context += ';S3_REGIONNAME=' + self.s3region
            s3_context += ';S3_RETRY_COUNT=2'
            s3_context += ';S3_WAIT_TIME_SEC=3'
            s3_context += ';S3_PROTO=' + self.proto
            s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
            s3_context += ';HOST_MODE=cacheless_attached'
            s3_context += ';S3_ENABLE_MD5=1'
            s3_context += ';S3_ENABLE_MPU=' + str(self.s3EnableMPU)
            s3_context += ';S3_CACHE_DIR=/var/lib/irods'

            if hasattr(self, 's3DisableCopyObject'):
                s3_context += ';S3_ENABLE_COPYOBJECT=0'

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            # do an ils to make sure server is up
            self.user0.assert_icommand("ils", 'STDOUT_SINGLELINE', 'tempZone')

        finally:

            # cleanup
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')


    def test_itouch_nonexistent_file__issue_6479(self):
        replica_number_in_s3 = 0
        filename = 'test_itouch_nonexistent_file__issue_6479'
        logical_path = os.path.join(self.user0.session_collection, filename)

        try:
            # Just itouch and ensure that the data object is created successfully.
            self.user0.assert_icommand(['itouch', logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # Ensure that the replica actually exists.
            self.user0.assert_icommand(['iget', logical_path, '-'])

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            self.admin.run_icommand([
                'iadmin', 'modrepl',
                'logical_path', logical_path,
                'replica_number', str(replica_number_in_s3),
                'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_istream_nonexistent_file__issue_6479(self):
        replica_number_in_s3 = 0
        filename = 'test_istream_nonexistent_file__issue_6479'
        logical_path = os.path.join(self.user0.session_collection, filename)
        content = 'streamin and screamin'

        try:
            # istream to a new data object and ensure that it is created successfully.
            self.user0.assert_icommand(['istream', 'write', logical_path], input=content)
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # Ensure that the replica actually contains the contents streamed into it.
            self.user0.assert_icommand(['iget', logical_path, '-'], 'STDOUT', content)

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            self.admin.run_icommand([
                'iadmin', 'modrepl',
                'logical_path', logical_path,
                'replica_number', str(replica_number_in_s3),
                'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_with_invalid_secret_key_and_overwrite__issue_6154(self):
        replica_number_in_s3 = 0
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

                # Put the physical file, which should fail, leaving a data object with a single stale replica. The main
                # purpose of this test is to ensure that the system is in a state from which it can recover.
                self.user0.assert_icommand(['iput', physical_path, logical_path], 'STDERR', 'S3_PUT_ERROR')
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Now overwrite the data object with wild success. This is here to ensure that things are back to normal.
            self.user0.assert_icommand(['iput', '-f', physical_path, logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            self.admin.run_icommand([
                'iadmin', 'modrepl',
                'logical_path', logical_path,
                'replica_number', str(replica_number_in_s3),
                'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_with_invalid_secret_key_and_remove__issue_6154(self):
        replica_number_in_s3 = 0
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

                # Put the physical file, which should fail, leaving a data object with a single stale replica. The main
                # purpose of this test is to ensure that the system is in a state from which it can recover.
                self.user0.assert_icommand(['iput', physical_path, logical_path], 'STDERR', 'S3_PUT_ERROR')
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
            self.admin.run_icommand([
                'iadmin', 'modrepl',
                'logical_path', logical_path,
                'replica_number', str(replica_number_in_s3),
                'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])


    def test_iput_and_replicate_with_invalid_secret_key__issue_6154(self):
        replica_number_in_s3 = 1
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
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # issue 2121 workaround
            time.sleep(2)

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Now replicate with a good set of S3 keys and watch for success.
            self.user0.assert_icommand(['irepl', logical_path])
            self.assertEqual(str(1), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

        finally:
            # Set the replica status here so that we can remove the object even if it is stuck in the locked status.
            for replica_number in [0, 1]:
                self.admin.run_icommand([
                    'iadmin', 'modrepl',
                    'logical_path', logical_path,
                    'replica_number', str(replica_number),
                    'DATA_REPL_STATUS', '0'])
            self.user0.run_icommand(['irm', '-f', logical_path])
            lib.remove_resource(test_resc, self.admin)


    def test_iput_and_icp_with_invalid_secret_key__issue_6154(self):
        replica_number_in_s3 = 0
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
                self.assertEqual(str(0), lib.get_replica_status(self.user0, filename, replica_number_in_s3))

            # debugging
            self.user0.assert_icommand(['ils', '-L', os.path.dirname(logical_path)], 'STDOUT', filename)

            # Now overwrite the data object with wild success. This is here to ensure that things are back to normal.
            self.user0.assert_icommand(['icp', '-f', original_logical_path, logical_path])
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

    # Replication to an S3 resource failed for file sizes between [4MB, 32MB] due to not knowing the correct
    # number of write threads being used.  Test the following conditions:
    # 1) files just inside this range
    # 2) files just outside the range
    # 3) a couple of files in the middle of the range (8MB and 16MB)
    # 4) a small file
    # 5) a file one byte less than 32MB just in case
    #
    # Added -k flag on iput to test issue 2122
    def test_s3_in_replication_node__issues_2102_2122(self):
        resc_prefix = 'issue_2101'
        repl_resource_name = '%s_repl' % resc_prefix
        s3_resc1_resource_name = '%s_s3_resc1' % resc_prefix
        s3_resc2_resource_name = '%s_s3_resc2' % resc_prefix
        s3_resc3_resource_name = '%s_s3_resc3' % resc_prefix
        filename = '%s_file' % resc_prefix
        hostname = lib.get_hostname()

        try:

            # set up resource tree
            self.admin.assert_icommand("iadmin mkresc %s replication" % repl_resource_name, 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc %s s3 %s:/%s/%s %s" %
                                   (s3_resc1_resource_name, hostname, self.s3bucketname, s3_resc1_resource_name, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc %s s3 %s:/%s/%s %s" %
                                   (s3_resc2_resource_name, hostname, self.s3bucketname, s3_resc2_resource_name, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin mkresc %s s3 %s:/%s/%s %s" %
                                   (s3_resc3_resource_name, hostname, self.s3bucketname, s3_resc3_resource_name, self.s3_context), 'STDOUT_SINGLELINE', "Creating")
            self.admin.assert_icommand("iadmin addchildtoresc %s %s" % (repl_resource_name, s3_resc1_resource_name))
            self.admin.assert_icommand("iadmin addchildtoresc %s %s" % (repl_resource_name, s3_resc2_resource_name))
            self.admin.assert_icommand("iadmin addchildtoresc %s %s" % (repl_resource_name, s3_resc3_resource_name))

            file_sizes = [
                    100,
                    4 * 1024 * 1024 - 1,
                    4 * 1024 * 1024,
                    8 * 1024 * 1024,
                    16 * 1024 * 1024,
                    32 * 1024 * 1024 - 1,
                    32 * 1024 * 1024,
                    32 * 1024 * 1024 + 1,
                    ];

            for file_size in file_sizes:
                # create the file
                lib.make_arbitrary_file(filename, file_size)

                # put the file to repl_resource
                self.user1.assert_icommand("iput -k -R %s %s" % (repl_resource_name, filename))

                # debug
                self.user1.assert_icommand("ils -l %s" % filename, 'STDOUT_SINGLELINE', filename)

                # get each replica
                self.user1.assert_icommand("iget -f -n 0 %s %s_get0" % (filename, filename))
                self.user1.assert_icommand("diff %s %s_get0" % (filename, filename), 'EMPTY')
                self.user1.assert_icommand("iget -f -n 1 %s %s_get1" % (filename, filename))
                self.user1.assert_icommand("diff %s %s_get1" % (filename, filename), 'EMPTY')
                self.user1.assert_icommand("iget -f -n 2 %s %s_get2" % (filename, filename))
                self.user1.assert_icommand("diff %s %s_get2" % (filename, filename), 'EMPTY')

                # cleanup before next loop
                s3plugin_lib.remove_if_exists(filename)
                s3plugin_lib.remove_if_exists("%s_get0" % filename)
                s3plugin_lib.remove_if_exists("%s_get1" % filename)
                s3plugin_lib.remove_if_exists("%s_get2" % filename)
                self.user1.assert_icommand("irm -f %s" % (filename))

        finally:
            # clean up the file that was put in case it wasn't cleaned up
            self.user1.assert_icommand("irm -f %s" % (filename))

            # clean up resource tree
            self.admin.assert_icommand("iadmin rmchildfromresc %s %s" % (repl_resource_name, s3_resc1_resource_name))
            self.admin.assert_icommand("iadmin rmchildfromresc %s %s" % (repl_resource_name, s3_resc2_resource_name))
            self.admin.assert_icommand("iadmin rmchildfromresc %s %s" % (repl_resource_name, s3_resc3_resource_name))
            self.admin.assert_icommand("iadmin rmresc %s" % repl_resource_name)
            self.admin.assert_icommand("iadmin rmresc %s" % s3_resc1_resource_name)
            self.admin.assert_icommand("iadmin rmresc %s" % s3_resc2_resource_name)
            self.admin.assert_icommand("iadmin rmresc %s" % s3_resc3_resource_name)

# The tests in this class take a long time and do not need to run in every different test suite
class Test_S3_NoCache_Large_File_Tests_Base(Test_S3_NoCache_Base):

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 2 * (4*1024*1024*1024 + 1), "not enough free space for two 4 GiB files")
    def test_put_get_file_greater_than_4GiB_one_thread(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file2 = "f1.get"

            file_size = 4*1024*1024*1024 + 1  # +1 - make sure one thread handles more than 4 GiB

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file_size)

            self.user0.assert_icommand("iput -N 1 -f -R s3resc1 {file1}".format(**locals()))  # iput

            # make sure file is right size
            self.user0.assert_icommand("ils -L %s" % file1, 'STDOUT_SINGLELINE', str(file_size))  # should be listed

            # get file from first repl
            self.user0.assert_icommand("iget -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup

            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    # issue 2024
    @unittest.skipIf(psutil.disk_usage('/').free < 2 * (8*1024*1024*1024 + 2), "not enough free space for two 8 GiB files")
    def test_put_get_file_greater_than_8GiB_two_threads(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file2 = "f1.get"

            file_size = 8*1024*1024*1024 + 2  # +2 - make sure each thread handles more than 4 GiB

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file_size)

            if self.s3EnableMPU == False:
                # iput will fail because file is too big for single part upload
                self.user0.assert_icommand("iput -N 2 -f -R s3resc1 {file1}".format(**locals()), 'STDERR_SINGLELINE', 'ERROR') # iput

            else:
                self.user0.assert_icommand("iput -N 2 -f -R s3resc1 {file1}".format(**locals()))  # iput

                # make sure file is right size
                self.user0.assert_icommand("ils -L %s" % file1, 'STDOUT_SINGLELINE', str(file_size))  # should be listed

                # get file from first repl
                self.user0.assert_icommand("iget -f %s %s" % (file1, file2))  # iput

                # make sure the file that was put and got are the same
                self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup

            if self.s3EnableMPU == False:
                # after failure data remains locked in 4.2.11, must unlock
                self.admin.assert_icommand("iadmin modrepl logical_path %s/%s replica_number 0 DATA_REPL_STATUS 0" % (self.user0.session_collection, file1))

            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    # issue 2024
    def test_large_file_put_repl_node(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            self.admin.assert_icommand("iadmin mkresc replresc replication", 'STDOUT_SINGLELINE', "Creating")

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            self.admin.assert_icommand("iadmin mkresc s3resc2 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, self.s3_context), 'STDOUT_SINGLELINE', "Creating")

            self.admin.assert_icommand("iadmin addchildtoresc replresc s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin addchildtoresc replresc s3resc2", 'EMPTY')

            file1 = "f1"
            file2 = "f1.get"

            file_size = 1024*1024*1024;


            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file_size)
            self.user0.assert_icommand("iput -f -R replresc {file1}".format(**locals()))  # iput

            # make sure file is right size
            self.user0.assert_icommand("ils -L %s" % file1, 'STDOUT_SINGLELINE', str(file_size))  # should be listed

            # get file from first repl
            self.user0.assert_icommand("iget -n0 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

            # get file from second repl
            self.user0.assert_icommand("iget -n1 -f %s %s" % (file1, file2))  # iput

            # make sure the file that was put and got are the same
            self.user0.assert_icommand("diff %s %s " % (file1, file2), 'EMPTY')

        finally:

            # cleanup
            self.user0.assert_icommand("irm -f %s" % file1, 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc replresc s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmchildfromresc replresc s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc2", 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc replresc", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)

    @unittest.skipIf(psutil.disk_usage('/').free < 2 * (11*1024*1024*1024 + 2), "not enough free space for two 11 GiB files")
    def test_upload_very_large_file_after_redirect_issues_2104_2114(self):

        # in non-topology HOSTNAME_3 is just local host so it really doesn't test detached mode
        # but in topology it will

        file1 = "f1"
        retrieved_file = "f1.get"
        resource_host = test.settings.HOSTNAME_3
        resource_name = "s3_resc_on_host3"

        # create an S3 resource
        # with S3_MPU_THREADS = 2 when testing in topology:
        # - There will be a redirect and cache will be used.
        # - There will only be 2 simultaneous part uploads but will require a third part to follow (issue 2114)
        # - Each part will be roughly 3.67 MB in size.  This is greater than the 2^31 limit with the old libs3 (issue 2104)
        s3_context = "%s;%s" % (self.s3_context, "S3_MPU_THREADS=2")
        self.admin.assert_icommand("iadmin mkresc %s s3 %s:/%s/%s %s" % (resource_name, resource_host, self.s3bucketname, resource_name, s3_context), 'STDOUT_SINGLELINE', 's3')

        try:
            # create an 11 GB file 
            lib.make_file(file1, 11*1024*1024*1024)

            # put the 11 GB file 
            self.user1.assert_icommand("iput %s" % file1)  # iput
            self.user1.assert_icommand("iget %s %s" % (file1, retrieved_file))

            self.assertTrue(filecmp.cmp(file1, retrieved_file))  # confirm retrieved is correct

        finally:

            # cleanup

            self.user1.assert_icommand("irm -f %s" % file1)  # irm
            self.admin.assert_icommand("iadmin rmresc %s" % resource_name)
            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(retrieved_file)

class Test_S3_NoCache_Glacier_Base(session.make_sessions_mixin([('otherrods', 'rods')], [('alice', 'apass'), ('bobby', 'bpass')])):

    def __init__(self, *args, **kwargs):
        """Set up the cacheless test."""
        # if self.proto is defined use it else default to HTTPS
        if not hasattr(self, 'proto'):
            self.proto = 'HTTPS'

        # if self.archive_naming_policy is defined use it
        # else default to 'consistent'
        if not hasattr(self, 'archive_naming_policy'):
            self.archive_naming_policy = 'consistent'

        super(Test_S3_NoCache_Glacier_Base, self).__init__(*args, **kwargs)

    def setUp(self):

        super(Test_S3_NoCache_Glacier_Base, self).setUp()
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
        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region = self.s3region,
                secure=(self.proto == 'HTTPS'))

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
            distro_str = ''.join(platform.linux_distribution()[:2]).replace(' ','').replace('.', '')
            self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d%H-%M-%S-%f-')
            self.s3bucketname += ''.join(random.choice(string.ascii_letters) for i in range(10))
            self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
            s3_client.make_bucket(self.s3bucketname, location=self.s3region)

        self.testresc = "TestResc"
        self.testvault = "/" + self.testresc
        self.anotherresc = "AnotherResc"
        self.anothervault = "/" + self.anotherresc

        self.s3_context = 'S3_DEFAULT_HOSTNAME=' + self.s3endPoint
        self.s3_context += ';S3_AUTH_FILE=' + self.keypairfile
        self.s3_context += ';S3_REGIONNAME=' + self.s3region
        self.s3_context += ';S3_RETRY_COUNT=2'
        self.s3_context += ';S3_WAIT_TIME_SECONDS=3'
        self.s3_context += ';S3_PROTO=' + self.proto
        self.s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy
        self.s3_context += ';HOST_MODE=cacheless_attached'
        self.s3_context += ';S3_ENABLE_MD5=1'
        self.s3_context += ';S3_ENABLE_MPU=' + str(self.s3EnableMPU)
        self.s3_context += ';S3_CACHE_DIR=/var/lib/irods'

        if hasattr(self, 's3DisableCopyObject'):
            self.s3DisableCopyObject = self.s3DisableCopyObject
            self.s3_context += ';S3_ENABLE_COPYOBJECT=0'

        if hasattr(self, 's3sse'):
            self.s3_context += ';S3_SERVER_ENCRYPT=' + str(self.s3sse)

        self.s3_context += ';ARCHIVE_NAMING_POLICY=' + self.archive_naming_policy

        self.admin.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

        self.admin.assert_icommand("iadmin mkresc demoResc s3 " + hostname + ":/" + self.s3bucketname + "/demoResc " + self.s3_context, 'STDOUT_SINGLELINE', 's3')

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

        super(Test_S3_NoCache_Glacier_Base, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand('irmtrash -M')
            admin_session.run_icommand(['iadmin', 'rmresc', self.testresc])
            admin_session.run_icommand(['iadmin', 'rmresc', self.anotherresc])
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            print("run_resource_teardown - END")

        # delete s3 bucket
        s3_client = Minio(self.s3endPoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region=self.s3region,
                secure=(self.proto == 'HTTPS'))

        objects = s3_client.list_objects(self.s3bucketname, recursive=True)

        if hasattr(self, 'static_bucket_name'):
            self.s3bucketname = self.static_bucket_name
        else:
            s3_client.remove_bucket(self.s3bucketname)

    def read_aws_keys(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            self.aws_access_key_id = f.readline().rstrip()
            self.aws_secret_access_key = f.readline().rstrip()

    def call_iget_get_status(self, rc1, rc2, file1, file2, file1_get, file2_get):
        _, _, rc1  = self.user0.run_icommand("iget -f {file1} {file1_get}".format(**locals()))
        _, _, rc2  = self.user0.run_icommand("iget -f {file2} {file2_get}".format(**locals()))
        return rc1 == 0 and rc2 == 0

    def test_put_get_glacier_expedited_retrieval(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = self.s3_context
            s3_context += ';S3_STORAGE_CLASS=Glacier;S3_RESTORATION_TIER=expedited'

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("iput -f -R s3resc1 {file2}".format(**locals()))  # iput

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

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)

    def test_put_get_glacier_standard_retrieval(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = self.s3_context
            s3_context += ';S3_STORAGE_CLASS=Glacier;S3_RESTORATION_TIER=standard'

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("iput -f -R s3resc1 {file2}".format(**locals()))  # iput

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

            # do not wait for retrieval as that takes 3-5 hours

        finally:

            # cleanup

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)

    def test_put_get_glacier_bulk_retrieval(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = self.s3_context
            s3_context += ';S3_STORAGE_CLASS=Glacier;S3_RESTORATION_TIER=bulk'

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("iput -f -R s3resc1 {file2}".format(**locals()))  # iput

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

            # do not wait for retrieval as that takes 5-12 hours

        finally:

            # cleanup

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)

    def test_put_get_deep_archive(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = self.s3_context
            s3_context += ';S3_STORAGE_CLASS=deep_archive;S3_RESTORATION_TIER=bulk'   # note restoration_tier is not applicable to deep archive and not used
                                                                                      # this will test that it is not attempted to be used

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("iput -f -R s3resc1 {file2}".format(**locals()))  # iput

            cmd = "iget -f {file1} {file1_get}".format(**locals())
            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in DEEP_ARCHIVE and has been queued for restoration', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and has been queued for restoration', stdout))

            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in DEEP_ARCHIVE and is currently being restored', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and is currently being restored', stdout))

            cmd = "iget -f {file2} {file2_get}".format(**locals())
            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in DEEP_ARCHIVE and has been queued for restoration', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and has been queued for restoration', stdout))

            stdout, stderr, rc  = self.user0.run_icommand(cmd)
            self.assertIn('REPLICA_IS_BEING_STAGED', stderr, '{0}: Expected stderr: "...{1}...", got: "{2}"'.format(cmd, 'REPLICA_IS_BEING_STAGED', stderr))
            self.assertIn('Object is in DEEP_ARCHIVE and is currently being restored', stdout, '{0}: Expected stdout: "...{1}...", got: "{2}"'.format(cmd, 'Object is in GLACIER and is currently being restored', stdout))

            # do not wait for retrieval

        finally:

            # cleanup

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)

    def test_put_get_glacier_ir(self):

        try:

            hostname = lib.get_hostname()
            hostuser = getpass.getuser()

            s3_context = self.s3_context
            s3_context += ';S3_STORAGE_CLASS=Glacier_IR;S3_RESTORATION_TIER=expedited' # note restoration_tier is not applicable for glacier_ir
                                                                                       # this will test that it is not attempted to be used

            self.admin.assert_icommand("iadmin mkresc s3resc1 s3 %s:/%s/%s/s3resc1 %s" %
                                   (hostname, self.s3bucketname, hostuser, s3_context), 'STDOUT_SINGLELINE', "Creating")

            file1 = "f1"
            file1_get = "f1.get"
            file2 = "f2"
            file2_get = "f2.get"

            file1_size = 8*1024*1024
            file2_size = 32*1024*1024 + 1

            # create and put file
            s3plugin_lib.make_arbitrary_file(file1, file1_size)
            s3plugin_lib.make_arbitrary_file(file2, file2_size)

            self.user0.assert_icommand("iput -f -R s3resc1 {file1}".format(**locals()))  # iput
            self.user0.assert_icommand("iput -f -R s3resc1 {file2}".format(**locals()))  # iput

            # glacier_ir does not require restoration
            self.user0.assert_icommand("iget -f {file1} {file1_get}".format(**locals()))  # iput
            self.user0.assert_icommand("iget -f {file2} {file2_get}".format(**locals()))  # iput

            # make sure the files that were put and got are the same
            self.user0.assert_icommand("diff {file1} {file1_get}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("diff {file2} {file2_get}".format(**locals()), 'EMPTY')

        finally:

            # cleanup

            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            self.user0.assert_icommand("irm -f {file2}".format(**locals()), 'EMPTY')
            self.admin.assert_icommand("iadmin rmresc s3resc1", 'EMPTY')

            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file2)
            s3plugin_lib.remove_if_exists(file1_get)
            s3plugin_lib.remove_if_exists(file2_get)

class Test_S3_NoCache_MPU_Disabled_Base(Test_S3_NoCache_Base):

    def test_iput_large_file_with_checksum_issue_2124(self):

        try:

            file1 = "issue_2124_file"
            file1_get = "issue_2124_file.get"

            # use a file size that is large enough for parallel transfers
            file1_size = 32*1024*1024 + 1

            # create and put file
            lib.make_arbitrary_file(file1, file1_size)

            self.user0.assert_icommand("iput -K " + file1)  # iput
            with open(file1, 'rb') as f:
                checksum = base64.b64encode(hashlib.sha256(f.read()).digest()).decode()
            self.user0.assert_icommand("ils -L", 'STDOUT_SINGLELINE', "sha2:" + checksum)  # check proper checksum

        finally:

            # cleanup
            self.user0.assert_icommand("irm -f {file1}".format(**locals()), 'EMPTY')
            s3plugin_lib.remove_if_exists(file1)
            s3plugin_lib.remove_if_exists(file1_get)
