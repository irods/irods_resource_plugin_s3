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
import zipfile

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
    distro_major_version = int(distro.major_version())
    ub24_or_later = (distro.id() == "ubuntu" and distro_major_version >= 24)
    deb13_or_later = (distro.id() == 'debian' and distro_major_version >= 13)
    if not any([ub24_or_later, deb13_or_later]):
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', '--upgrade', 'pip>=20.3.4'], check_rc=True)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'boto3', '--upgrade'], check_rc=True)

    # Minio 7.1.17 imports the annotations module which only exists in Python 3.7 and beyond.
    # For OS which default to Python 3.6, we have to install the previous version of Minio to avoid
    # compatibility issues. The --upgrade flag is ignored if "minio_version" results in a non-empty string.
    minio_version = '==7.1.16' if sys.hexversion < 0x030700F0 else ''
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'python3', '-m', 'pip', 'install', 'minio' + minio_version, '--upgrade'], check_rc=True)

    # install PRC
    irods_python_ci_utilities.subprocess_get_output(['python3', '-m', 'pip', 'install', 'python-irodsclient'], check_rc=True)


def wait_for_rustfs_servers_or_raise(procs, proc_infos, timeout_seconds=30):
    import socket
    import time as time_module

    deadline = time_module.time() + timeout_seconds
    remaining = list(zip(procs, proc_infos))

    while remaining:
        still_waiting = []
        for proc, info in remaining:
            exit_code = proc.poll()
            if exit_code is not None:
                _dump_rustfs_log_and_raise(info, 'exited early with code {0}'.format(exit_code))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                if sock.connect_ex(('127.0.0.1', int(info['address']))) == 0:
                    print('RustFS server on port {0} is accepting connections.'.format(info['address']))
                    continue

            still_waiting.append((proc, info))

        remaining = still_waiting
        if remaining and time_module.time() >= deadline:
            for _, info in remaining:
                _dump_rustfs_log_and_raise(info, 'never became reachable on port {0} within {1}s'.format(info['address'], timeout_seconds))
        if remaining:
            time_module.sleep(1)


def _dump_rustfs_log_and_raise(info, reason):
    log_path = info['log_path']
    print('RustFS server (port {0}) {1}. Log ({2}):'.format(info['address'], reason, log_path))
    try:
        with open(log_path) as f:
            print(f.read())
    except OSError as e:
        print('  (could not read log: {0})'.format(e))
    raise RuntimeError('RustFS server on port {0} {1}'.format(info['address'], reason))


def download_and_start_rustfs_server():
    rustfs_version = '1.0.0'

    path_to_rustfs = '/rustfs'
    rustfs_zip_path = '/tmp/rustfs.zip'

    # Use the musl build, not gnu: the gnu build is dynamically linked against a glibc
    # newer than what ships on several supported distros (e.g. glibc 2.38+ vs. the 2.34-2.36
    # available on RockyLinux 9 / Ubuntu 22.04 / Debian 12), so it fails to even start there.
    # The musl build is statically linked and has no such dependency.
    subprocess.check_output(['wget', '-q', '--no-check-certificate', '-O', rustfs_zip_path,
                             'https://github.com/rustfs/rustfs/releases/download/{0}/rustfs-linux-x86_64-musl-v{0}.zip'
                                .format(rustfs_version)])

    with zipfile.ZipFile(rustfs_zip_path) as zip_file:
        zip_file.extract('rustfs', path='/tmp/rustfs_extracted')
    shutil.move('/tmp/rustfs_extracted/rustfs', path_to_rustfs)
    os.chmod(path_to_rustfs, os.stat(path_to_rustfs).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    root_username = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))
    root_password = ''.join(random.choice(string.ascii_letters) for i in list(range(10)))

    for keypair_path in ('/var/lib/irods/rustfs.keypair', '/var/lib/irods/minio.keypair'):
        with open(keypair_path, 'w') as f:
            f.write('%s\n' % root_username)
            f.write('%s\n' % root_password)
        shutil.chown(keypair_path, user='irods', group='irods')

    os.environ['RUSTFS_ACCESS_KEY'] = root_username
    os.environ['RUSTFS_SECRET_KEY'] = root_password

    rustfs_region_name_key = 'RUSTFS_REGION'

    proc_infos = [
        {
            'address':              '9000',
            'console_address':      '9002',
            rustfs_region_name_key: None
        },
        {
            'address':              '9001',
            'console_address':      '9003',
            rustfs_region_name_key: 'eu-central-1'
        }
    ]

    procs = list()

    for p in proc_infos:
        if p[rustfs_region_name_key] is not None:
            os.environ[rustfs_region_name_key] = p[rustfs_region_name_key]
        elif rustfs_region_name_key in os.environ:
            del os.environ[rustfs_region_name_key]

        data_dir = '/data_rustfs_%s' % p[rustfs_region_name_key]
        os.makedirs(data_dir, exist_ok=True)

        log_path = '/tmp/rustfs_%s.log' % p['address']
        p['log_path'] = log_path
        log_file = open(log_path, 'wb')

        procs.append(subprocess.Popen([path_to_rustfs, 'server',
                                       '--address', ':' + p["address"],
                                       '--console-address', ':' + p["console_address"],
                                       data_dir], stdout=log_file, stderr=subprocess.STDOUT))

    wait_for_rustfs_servers_or_raise(procs, proc_infos)

    return procs


def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--test', metavar='dotted name')
    parser.add_option('--skip-setup', action='store_false', dest='do_setup', default=True)
    parser.add_option('--teardown-rustfs', action='store_true', dest='do_teardown', default=False)
    options, _ = parser.parse_args()

    if not options.do_setup and options.do_teardown:
        # TODO(#2327): if a client can shut down the server, this will not be true
        print('--skip-setup and --teardown-rustfs are incompatible')
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

        s3_server_processes = download_and_start_rustfs_server()

    test = options.test or 'test_irods_resource_plugin_s3_rustfs'

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c',
            f'python3 scripts/run_tests.py --xml_output --run_s {test} 2>&1 | tee {test_output_file}; exit $PIPESTATUS'],
            check_rc=True)

        if options.do_teardown:
            for p in s3_server_processes:
                p.terminate()

    finally:
        output_root_directory = options.output_root_directory
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
