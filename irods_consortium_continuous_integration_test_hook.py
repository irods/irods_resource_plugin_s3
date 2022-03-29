from __future__ import print_function

import glob
import optparse
import os
import random
import shutil
import stat
import string
import subprocess
import time

import irods_python_ci_utilities

def install_test_prerequisites():
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'boto3', '--upgrade'], check_rc=True)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'minio', '--upgrade'], check_rc=True)
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'python3', '-m', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

def download_and_start_minio_server():
    path_to_minio = os.path.abspath('minio')

    # Only download the executable if it's not already here
    if not os.path.isfile(path_to_minio):
        subprocess.check_output(['wget', '-q', 'https://dl.min.io/server/minio/release/linux-amd64/minio'])
        os.chmod(path_to_minio, stat.S_IXUSR)

    root_username = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))
    root_password = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))

    with open('/var/lib/irods/minio.keypair', 'w') as f:
        f.write('%s\n' % root_username)
        f.write('%s\n' % root_password)

    os.environ['MINIO_ROOT_USER'] = root_username
    os.environ['MINIO_ROOT_PASSWORD'] = root_password

    minio_region_name_key = 'MINIO_REGION_NAME'

    proc_infos = [
        {
            'address':              '9000',
            'console_address':      '9002',
            minio_region_name_key:  None
        },
        {
            'address':              '9001',
            'console_address':      '9003',
            minio_region_name_key:  'eu-central-1'
        }
    ]

    procs = list()

    for p in proc_infos:
        if p[minio_region_name_key] is not None:
            os.environ[minio_region_name_key] = p[minio_region_name_key]
        elif minio_region_name_key in os.environ:
            del os.environ[minio_region_name_key]

        procs.append(subprocess.Popen([path_to_minio, 'server',
                                       '--address', f':{p["address"]}',
                                       '--console-address', f':{p["console_address"]}',
                                       '/data']))

    return procs


def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--test', metavar='dotted name')
    parser.add_option('--skip-setup', action='store_false', dest='do_setup', default=True)
    parser.add_option('--teardown-minio', action='store_true', dest='do_teardown', default=False)
    options, _ = parser.parse_args()

    if not options.do_setup and options.do_teardown:
        # TODO: if minio-client can shut down the server, this will not be true
        print('--skip-setup and --teardown-minio are incompatible')
        exit(1)

    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    if options.do_setup:
        irods_python_ci_utilities.install_os_packages_from_files(
            glob.glob(os.path.join(os_specific_directory,
                      f'irods-resource-plugin-s3*.{package_suffix}')
            )
        )

        install_test_prerequisites()

        minio_processes = download_and_start_minio_server()

    test = options.test or 'test_irods_resource_plugin_s3_minio'

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c',
            f'python3 scripts/run_tests.py --xml_output --run_s {test} 2>&1 | tee {test_output_file}; exit $PIPESTATUS'],
            check_rc=True)

        if options.do_teardown:
            # TODO: investigate mc admin service stop
            # Get minio client
            #   wget https://dl.min.io/client/mc/release/linux-amd64/mc
            #   chmod +x mc
            #   sudo cp mc /usr/bin
            for m in minio_processes:
                m.terminate()

    finally:
        output_root_directory = options.output_root_directory
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
