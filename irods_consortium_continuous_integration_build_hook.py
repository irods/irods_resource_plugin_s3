from __future__ import print_function

import glob
import multiprocessing
import optparse
import os
import sys
import tempfile

import irods_python_ci_utilities


def add_cmake_to_front_of_path():
    cmake_path = '/opt/irods-externals/cmake3.11.4-0/bin'
    os.environ['PATH'] = os.pathsep.join([cmake_path, os.environ['PATH']])

def install_building_dependencies(externals_directory):
    externals_list = ['irods-externals-cmake3.11.4-0',
                      'irods-externals-avro1.9.0-0',
                      'irods-externals-boost1.67.0-0',
                      'irods-externals-clang-runtime6.0-0',
                      'irods-externals-clang6.0-0',
                      'irods-externals-cppzmq4.2.3-0',
                      'irods-externals-json3.7.3-0',
                      'irods-externals-libarchive3.3.2-1',
                      'irods-externals-libs3e4197a5e-0',
                      'irods-externals-zeromq4-14.1.6-0']
    if externals_directory == 'None' or externals_directory is None:
        irods_python_ci_utilities.install_irods_core_dev_repository()
        irods_python_ci_utilities.install_os_packages(externals_list)
    else:
        package_suffix = irods_python_ci_utilities.get_package_suffix()
        os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(externals_directory)
        externals = []
        for irods_externals in externals_list:
            externals.append(glob.glob(os.path.join(os_specific_directory, irods_externals + '*.{0}'.format(package_suffix)))[0])
        irods_python_ci_utilities.install_os_packages_from_files(externals)
    add_cmake_to_front_of_path()
    install_os_specific_dependencies()

def install_os_specific_dependencies_apt():
    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'update'], check_rc=True)
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])
    irods_python_ci_utilities.install_os_packages(['make', 'libssl-dev', 'libxml2-dev', 'libcurl4-gnutls-dev', 'gcc'])

def install_os_specific_dependencies_yum():
    irods_python_ci_utilities.install_os_packages(['openssl-devel', 'libxml2-devel', 'curl-devel'])

def install_os_specific_dependencies():
    dispatch_map = {
        'Ubuntu': install_os_specific_dependencies_apt,
        'Centos': install_os_specific_dependencies_yum,
        'Centos linux': install_os_specific_dependencies_yum,
        'Opensuse ':  install_os_specific_dependencies_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def copy_output_packages(build_directory, output_root_directory):
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        build_directory,
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s:s.endswith(irods_python_ci_utilities.get_package_suffix()))

def main(build_directory, output_root_directory, irods_packages_root_directory, externals_directory):
    install_building_dependencies(externals_directory)
    if irods_packages_root_directory:
        irods_python_ci_utilities.install_irods_dev_and_runtime_packages(irods_packages_root_directory)
    build_directory = os.path.abspath(build_directory or tempfile.mkdtemp(prefix='irods_s3_plugin_build_directory'))
    irods_python_ci_utilities.subprocess_get_output(['cmake', os.path.dirname(os.path.realpath(__file__))], check_rc=True, cwd=build_directory)
    irods_python_ci_utilities.subprocess_get_output(['make', '-j', str(multiprocessing.cpu_count()), 'package'], check_rc=True, cwd=build_directory)
    if output_root_directory:
        copy_output_packages(build_directory, output_root_directory)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--build_directory')
    parser.add_option('--output_root_directory')
    parser.add_option('--irods_packages_root_directory')
    parser.add_option('--externals_packages_directory')
    options, _ = parser.parse_args()

    main(options.build_directory,
         options.output_root_directory,
         options.irods_packages_root_directory,
         options.externals_packages_directory)
