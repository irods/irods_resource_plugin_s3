cmake_minimum_required(VERSION 3.12.0 FATAL_ERROR)
# CURL::libcurl and LibXml2::LibXml2 targets

set(IRODS_MINIMUM_VERSION "4.3.4")
find_package(IRODS "${IRODS_MINIMUM_VERSION}" REQUIRED)
set(IRODS_PLUGIN_REVISION "1")
set(IRODS_PLUGIN_VERSION "${IRODS_MINIMUM_VERSION}.${IRODS_PLUGIN_REVISION}")

set(IRODS_PACKAGE_REVISION "0")

include(IrodsCXXCompiler)
set(CMAKE_CXX_STANDARD ${IRODS_CXX_STANDARD})
set(CMAKE_EXE_LINKER_FLAGS_INIT "-Wl,--export-dynamic -Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
set(CMAKE_MODULE_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
include(IrodsRunpathDefaults)

project(irods_resource_plugin-s3
  VERSION "${IRODS_PLUGIN_VERSION}"
  LANGUAGES C CXX)

include("${IRODS_TARGETS_PATH}")

include(GNUInstallDirs)

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

if (CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  set(IRODS_BUILD_WITH_WERROR_DEFAULT ON)
else()
  set(IRODS_BUILD_WITH_WERROR_DEFAULT OFF)
endif()

set(IRODS_BUILD_WITH_WERROR ${IRODS_BUILD_WITH_WERROR_DEFAULT} CACHE BOOL "Choose whether to compile with -Werror.")

include(UseLibCXX)

if (IRODS_BUILD_WITH_WERROR)
  add_compile_options(-Werror)
endif()

add_compile_options(-Wall -Wextra)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fpermissive>)
endif()

# Variable length arrays can crash clang (#2156), so let's turn on warnings for them.
# -Wvla is not enabled by -Wall or -Wextra, so we have to do it manually.
check_c_compiler_flag(-Wvla can_use_wvla_c)
check_cxx_compiler_flag(-Wvla can_use_wvla_cxx)
if (can_use_wvla_c)
  add_compile_options($<$<COMPILE_LANGUAGE:C>:-Wvla>)
endif()
if (can_use_wvla_cxx)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wvla>)
endif()

if (NOT DEFINED THREADS_PREFER_PTHREAD_FLAG)
  set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif()
find_package(Threads REQUIRED)
find_package(nlohmann_json "3.6.1" REQUIRED)
find_package(fmt "8.1.1"
  HINTS "${IRODS_EXTERNALS_FULLPATH_FMT}")
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibXml2 REQUIRED)
include(ObjectTargetHelpers)

add_subdirectory(libs3)
add_subdirectory(s3_transport)
add_subdirectory(s3_resource)
add_subdirectory(unit_tests)

add_library(irods_s3_plugin MODULE)
target_link_objects(
  irods_s3_plugin
  PRIVATE
  libs3_obj
  s3_transport_obj
  s3_resource_obj
)
set_property(TARGET irods_s3_plugin PROPERTY OUTPUT_NAME s3)

install(
  TARGETS
  irods_s3_plugin
  LIBRARY
  DESTINATION "${IRODS_PLUGINS_DIRECTORY}/resources"
)

install(
  FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_minio.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_ceph.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_gcs.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_fujifilm.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/resource_suite_s3_nocache.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/resource_suite_s3_cache.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/s3plugin_lib.py"
  "${CMAKE_CURRENT_SOURCE_DIR}/packaging/s3_prc_upload_issues_2260_2261.py"
  DESTINATION "${IRODS_HOME_DIRECTORY}/scripts/irods/test"
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)

if (NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR ${IRODS_CPACK_GENERATOR} CACHE STRING "CPack generator to use, e.g. {DEB, RPM, TGZ}." FORCE)
  message(STATUS "Setting unspecified CPACK_GENERATOR to ${CPACK_GENERATOR}. This is the correct setting for normal builds.")
endif()

include(IrodsCPackCommon)

list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_PLUGINS_DIRECTORY}")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_PLUGINS_DIRECTORY}/resources")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods/test")

set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENTS_GROUPING IGNORE)
set(CPACK_PACKAGE_VERSION ${IRODS_PLUGIN_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The integrated Rule-Oriented Data System")

set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_SOURCE_DIR}/packaging/postinst;")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

set(CPACK_RPM_COMPONENT_INSTALL OFF)
set(CPACK_RPM_PACKAGE_LICENSE "LGPL-3.0-or-later OR GPL-2.0-or-later WITH cryptsetup-OpenSSL-exception")
set(CPACK_RPM_PACKAGE_AUTOREQ 0)
set(CPACK_RPM_PACKAGE_AUTOPROV 0)
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/packaging/postinst")
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)

set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

set(CPACK_DEBIAN_PACKAGE_NAME "irods-resource-plugin-s3")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "irods-runtime (= ${IRODS_VERSION}), libxml2, libc6")

get_filename_component(CURL_LIBRARY_REALPATH ${CURL_LIBRARY} REALPATH)
get_filename_component(CURL_LIBRARY_REALNAME ${CURL_LIBRARY_REALPATH} NAME_WE)
if (CURL_LIBRARY_REALNAME STREQUAL "libcurl-gnutls")
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl3-gnutls")
elseif (CURL_LIBRARY_REALNAME STREQUAL "libcurl-nss")
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl3-nss")
elseif (CURL_LIBRARY_REALNAME STREQUAL "libcurl")
  if (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "ubuntu")
    if (IRODS_LINUX_DISTRIBUTION_VERSION_MAJOR GREATER_EQUAL "18")
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl4")
    else ()
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl3")
    endif ()
  elseif (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "debian")
    if (IRODS_LINUX_DISTRIBUTION_VERSION_MAJOR GREATER_EQUAL "10")
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl4")
    else ()
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libcurl3")
    endif ()
  endif ()
endif ()

set(CPACK_RPM_PACKAGE_NAME "irods-resource-plugin-s3")
if (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "opensuse")
  set(CPACK_RPM_PACKAGE_REQUIRES "irods-runtime = ${IRODS_VERSION}, libcurl, libopenssl1_0_0")
else()
  set(CPACK_RPM_PACKAGE_REQUIRES "irods-runtime = ${IRODS_VERSION}, libcurl, libxml2")
endif()

include(CPack)
