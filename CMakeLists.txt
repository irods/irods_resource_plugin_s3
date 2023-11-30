cmake_minimum_required(VERSION 3.12.0 FATAL_ERROR)
# CURL::libcurl and LibXml2::LibXml2 targets

find_package(IRODS 4.3.1 EXACT REQUIRED)
set(IRODS_PLUGIN_REVISION "0")
set(IRODS_PLUGIN_VERSION "${IRODS_VERSION}.${IRODS_PLUGIN_REVISION}")

set(IRODS_PACKAGE_REVISION "0")

include(IrodsCXXCompiler)
set(CMAKE_CXX_STANDARD ${IRODS_CXX_STANDARD})
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_MODULE_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
set(CMAKE_SHARED_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
include(IrodsRunpathDefaults)

project(irods_resource_plugin-s3
  VERSION "${IRODS_PLUGIN_VERSION}"
  LANGUAGES C CXX)

include(${IRODS_TARGETS_PATH})

include(GNUInstallDirs)

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

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

if (NOT DEFINED THREADS_PREFER_PTHREAD_FLAG)
  set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif()
find_package(Threads REQUIRED)
find_package(nlohmann_json "3.6.1" REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibXml2 REQUIRED)

include(ObjectTargetHelpers)

add_library(
  libs3_obj
  OBJECT
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/bucket.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/bucket_metadata.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/error_parser.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/general.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/multipart.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/object.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/request.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/request_context.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/response_headers_handler.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/service.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/service_access_logging.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/simplexml.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/libs3/src/util.c"
)
target_link_libraries(
  libs3_obj
  PUBLIC
  CURL::libcurl
  LibXml2::LibXml2
  OpenSSL::Crypto
  Threads::Threads
)
target_include_directories(
  libs3_obj
  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libs3/include>"
)
target_compile_definitions(
  libs3_obj
  PRIVATE
  LIBS3_VER_MAJOR="4"
  LIBS3_VER_MINOR="1"
  __STRICT_ANSI__
  _ISOC99_SOURCE
  _POSIX_C_SOURCE=200112L
  OPENSSL_API_COMPAT=10100
)
target_compile_options(libs3_obj PRIVATE -Wshadow)
set_target_properties(libs3_obj PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(libs3_obj PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(libs3_obj PROPERTIES EXCLUDE_FROM_ALL TRUE)

add_library(
  irods_s3_plugin
  MODULE
  ${CMAKE_SOURCE_DIR}/s3/libirods_s3.cpp
  ${CMAKE_SOURCE_DIR}/s3/s3_operations.cpp
  ${CMAKE_SOURCE_DIR}/s3/s3_transport/src/s3_transport.cpp
  )
target_link_objects(irods_s3_plugin PRIVATE libs3_obj)
target_link_libraries(
  irods_s3_plugin
  PRIVATE
  irods_server
  irods_common
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
  ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so
  Threads::Threads
  nlohmann_json::nlohmann_json
  rt
  )
target_include_directories(
  irods_s3_plugin
  PRIVATE
  ${IRODS_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/s3/s3_transport/include
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
  ${IRODS_EXTERNALS_FULLPATH_FMT}/include
  ${IRODS_EXTERNALS_FULLPATH_S3}/include
  ${IRODS_EXTERNALS_FULLPATH_SPDLOG}/include
  )
target_compile_definitions(
  irods_s3_plugin
  PRIVATE
  IRODS_QUERY_ENABLE_SERVER_SIDE_API
  IRODS_ENABLE_SYSLOG
  ${IRODS_COMPILE_DEFINITIONS}
  ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
  BOOST_SYSTEM_NO_DEPRECATED
  )
set_property(TARGET irods_s3_plugin PROPERTY OUTPUT_NAME s3)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(irods_s3_plugin PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-fpermissive>)
endif()

target_compile_options(irods_s3_plugin PRIVATE -Wno-unused-parameter)

# due to OPENSSL_NO_DEPRECATED_1_1_0
target_compile_options(irods_s3_plugin PRIVATE -Wno-unused-function)

include(CheckCXXCompilerFlag)

# due to boost headers
check_cxx_compiler_flag(-Wno-error=deprecated-copy CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
if (CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
  target_compile_options(irods_s3_plugin PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Wno-error=deprecated-copy>)
endif()

install(
  TARGETS
  irods_s3_plugin
  LIBRARY
  DESTINATION ${IRODS_PLUGINS_DIRECTORY}/resources
  )

install(
  FILES
  ${CMAKE_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3.py
  ${CMAKE_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_minio.py
  ${CMAKE_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_ceph.py
  ${CMAKE_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_gcs.py
  ${CMAKE_SOURCE_DIR}/packaging/test_irods_resource_plugin_s3_fujifilm.py
  ${CMAKE_SOURCE_DIR}/packaging/resource_suite_s3_nocache.py
  ${CMAKE_SOURCE_DIR}/packaging/resource_suite_s3_cache.py
  ${CMAKE_SOURCE_DIR}/packaging/s3plugin_lib.py
  DESTINATION ${IRODS_HOME_DIRECTORY}/scripts/irods/test
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

if (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "ubuntu" OR IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "debian")
  if (NOT IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME)
    execute_process(
      COMMAND "lsb_release" "-s" "-c"
      RESULT_VARIABLE IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME
      OUTPUT_VARIABLE IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME
      )
    string(STRIP ${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME} IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME)
    if (NOT ${IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME} STREQUAL "0")
      message(FATAL_ERROR "Linux lsb_release shortname detection failed\n${IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME}")
    endif()
    set(IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME ${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME} CACHE STRING "Linux distribution version codename, e.g. {precise, wheezy, trusty, jessie, ...}." FORCE)
    message(STATUS "Setting unspecified IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME to '${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME}'")
  endif()
endif()

if (NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR ${IRODS_CPACK_GENERATOR} CACHE STRING "CPack generator to use, e.g. {DEB, RPM, TGZ}." FORCE)
  message(STATUS "Setting unspecified CPACK_GENERATOR to ${CPACK_GENERATOR}. This is the correct setting for normal builds.")
endif()

include(IrodsCPackCommon)

list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}")
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
  set(CPACK_RPM_PACKAGE_REQUIRES "irods-runtime = ${IRODS_VERSION}, curl-devel, libopenssl1_0_0")
else()
  set(CPACK_RPM_PACKAGE_REQUIRES "irods-runtime = ${IRODS_VERSION}, curl-devel, libxml2")
endif()

include(CPack)
