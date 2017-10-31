from __future__ import print_function

import multiprocessing
import optparse
import os
import sys
import tempfile

import irods_python_ci_utilities


def install_cmake_and_add_to_front_of_path():
    irods_python_ci_utilities.install_os_packages(['irods-externals-cmake3.5.2-0'])
    os.environ['PATH'] = '/opt/irods-externals/cmake3.5.2-0/bin' + os.pathsep + os.environ['PATH']

def get_build_prerequisites_all():
    return ['irods-externals-clang3.8-0',
            'irods-externals-cppzmq4.1-0',
            'irods-externals-libarchive3.1.2-0',
            'irods-externals-avro1.7.7-0',
            'irods-externals-clang-runtime3.8-0',
            'irods-externals-boost1.60.0-0',
            'irods-externals-jansson2.7-0',
            'irods-externals-zeromq4-14.1.3-0',
            'irods-externals-libs3a30e55e8-0']

def get_build_prerequisites_apt():
    return ['make', 'libssl-dev', 'libxml2-dev', 'libcurl4-gnutls-dev', 'gcc'] + get_build_prerequisites_all()

def get_build_prerequisites_yum():
    return ['openssl-devel', 'libxml2-devel', 'curl-devel'] + get_build_prerequisites_all()

def get_build_prerequisites():
    dispatch_map = {
        'Ubuntu': get_build_prerequisites_apt,
        'Centos': get_build_prerequisites_yum,
        'Centos linux': get_build_prerequisites_yum,
        'Opensuse ':  get_build_prerequisites_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def install_build_prerequisites_apt():
    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())

def install_build_prerequisites_yum():
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())

def install_build_prerequisites():
    dispatch_map = {
        'Ubuntu': install_build_prerequisites_apt,
        'Centos': install_build_prerequisites_yum,
        'Centos linux': install_build_prerequisites_yum,
        'Opensuse ':  install_build_prerequisites_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def install_irods_dev_and_runtime_packages(irods_packages_root_directory):
    irods_packages_directory = irods_python_ci_utilities.append_os_specific_directory(irods_packages_root_directory)
    dev_package_basename = filter(lambda x:'irods-dev' in x, os.listdir(irods_packages_directory))[0]
    dev_package = os.path.join(irods_packages_directory, dev_package_basename)
    irods_python_ci_utilities.install_os_packages_from_files([dev_package])
    runtime_package_basename = filter(lambda x:'irods-runtime' in x, os.listdir(irods_packages_directory))[0]
    runtime_package = os.path.join(irods_packages_directory, runtime_package_basename)
    irods_python_ci_utilities.install_os_packages_from_files([runtime_package])

def copy_output_packages(build_directory, output_root_directory):
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        build_directory,
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s:s.endswith(irods_python_ci_utilities.get_package_suffix()))

def main(output_root_directory, irods_packages_root_directory):
    irods_python_ci_utilities.install_irods_core_dev_repository()
    install_cmake_and_add_to_front_of_path()
    install_build_prerequisites()
    if irods_packages_root_directory:
        install_irods_dev_and_runtime_packages(irods_packages_root_directory)
    build_directory = tempfile.mkdtemp(prefix='irods_s3_plugin_build_directory')
    irods_python_ci_utilities.subprocess_get_output(['cmake', os.path.dirname(os.path.realpath(__file__))], check_rc=True, cwd=build_directory)
    irods_python_ci_utilities.subprocess_get_output(['make', '-j', str(multiprocessing.cpu_count()), 'package'], check_rc=True, cwd=build_directory)
    if output_root_directory:
        copy_output_packages(build_directory, output_root_directory)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--irods_packages_root_directory')
    options, _ = parser.parse_args()

    main(options.output_root_directory, options.irods_packages_root_directory)
