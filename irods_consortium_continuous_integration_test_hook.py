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
import platform
import distro
import logging
import sys

import irods_python_ci_utilities

def get_package_type():
    log = logging.getLogger(__name__)
    distro_id = distro.id()
    log.debug('linux distribution detected: {0}'.format(distro_id))
    if distro_id in ['debian', 'ubuntu']:
        pt = 'deb'
    elif distro_id in ['rocky', 'almalinux', 'centos', 'rhel', 'scientific', 'opensuse', 'sles']:
        pt = 'rpm'
    else:
        if platform.mac_ver()[0] != '':
            pt = 'osxpkg'
        else:
            pt = 'not_detected'
    log.debug('package type detected: {0}'.format(pt))
    return pt

def install_test_prerequisites():
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', '--upgrade', 'pip>=20.3.4'], check_rc=True)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'boto3', '--upgrade'], check_rc=True)

    # Minio 7.1.17 imports the annontations module which only exists in Python 3.7 and beyond.
    # For OS which default to Python 3.6, we have to install the previous version of Minio to avoid
    # compatibility issues. The --upgrade flag is ignored if "minio_version" results in a non-empty string.
    minio_version = '==7.1.16' if sys.hexversion < 0x030700F0 else ''
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'minio' + minio_version, '--upgrade'], check_rc=True)

    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'python3', '-m', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

def download_and_start_minio_server():
    path_to_minio = os.path.abspath('minio')

    # Only download the executable if it's not already here
    if not os.path.isfile(path_to_minio):
        if get_package_type() == 'deb':
            minio_package_file = 'minio_20220526054841.0.0_amd64.deb'
            subprocess.check_output(['wget', '-q', '--no-check-certificate', 'https://dl.min.io/server/minio/release/linux-amd64/archive/{}'.format(minio_package_file)])
            subprocess.check_output(['dpkg', '-i', minio_package_file])
        elif get_package_type() == 'rpm':
            minio_package_file = 'minio-20220526054841.0.0.x86_64.rpm'
            subprocess.check_output(['wget', '-q', '--no-check-certificate', 'https://dl.min.io/server/minio/release/linux-amd64/archive/{}'.format(minio_package_file)])
            subprocess.check_output(['rpm', '--force', '-i', minio_package_file])
        else:
            raise Exception('Unknown or invalid OS type.  Can not install minio.')

        path_to_minio = 'minio'

    root_username = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))
    root_password = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))
    keypair_path = '/var/lib/irods/minio.keypair'

    with open(keypair_path, 'w') as f:
        f.write('%s\n' % root_username)
        f.write('%s\n' % root_password)

    shutil.chown(keypair_path, user='irods', group='irods')

    os.environ['MINIO_ROOT_USER'] = root_username
    os.environ['MINIO_ROOT_PASSWORD'] = root_password
    os.environ['MINIO_CI_CD'] = '1'

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
                                       '--address', ':' + p["address"],
                                       '--console-address', ':' + p["console_address"],
                                       '/data_%s' % p[minio_region_name_key]]))

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
