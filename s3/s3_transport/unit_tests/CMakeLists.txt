cmake_minimum_required(VERSION ${CMAKE_VERSION})
project(unit_tests LANGUAGES C CXX)

find_package(IRODS 4.3.0 EXACT REQUIRED)

set(IRODS_EXTERNALS_FULLPATH_S3 "/opt/irods-externals/libs3e4197a5e-0")    # without verbosity

message(STATUS "CMAKE_BINARY_DIR=${CMAKE_BINARY_DIR}")
message(STATUS "IRODS_EXTERNALS_FULLPATH_S3=${IRODS_EXTERNALS_FULLPATH_S3}")
message(STATUS "IRODS_CXX_STANDARD=${IRODS_CXX_STANDARD}")

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'. This is the correct setting for normal builds.")
endif()

if (NOT IRODS_EXTERNALS_PACKAGE_ROOT)
  set(IRODS_EXTERNALS_PACKAGE_ROOT "/opt/irods-externals" CACHE STRING "Choose the location of iRODS external packages." FORCE)
  message(STATUS "Setting unspecified IRODS_EXTERNALS_PACKAGE_ROOT to '${IRODS_EXTERNALS_PACKAGE_ROOT}'. This is the correct setting for normal builds.")
endif()

macro(IRODS_MACRO_CHECK_DEPENDENCY_SET_FULLPATH DEPENDENCY_NAME DEPENDENCY_SUBDIRECTORY)
  if (IS_DIRECTORY ${IRODS_EXTERNALS_FULLPATH_${DEPENDENCY_NAME}})
    message(STATUS "Using user-specified value for IRODS_EXTERNALS_FULLPATH_${DEPENDENCY_NAME}: ${IRODS_EXTERNALS_FULLPATH_${DEPENDENCY_NAME}}")
  else()
    if (NOT IS_DIRECTORY ${IRODS_EXTERNALS_PACKAGE_ROOT}/${DEPENDENCY_SUBDIRECTORY})
      message(FATAL_ERROR "${DEPENDENCY_NAME} not found at ${IRODS_EXTERNALS_PACKAGE_ROOT}/${DEPENDENCY_SUBDIRECTORY}")
    endif()
    set(IRODS_EXTERNALS_FULLPATH_${DEPENDENCY_NAME} ${IRODS_EXTERNALS_PACKAGE_ROOT}/${DEPENDENCY_SUBDIRECTORY})
  endif()
endmacro()

macro(IRODS_MACRO_CHECK_DEPENDENCY_SET_FULLPATH_ADD_TO_IRODS_PACKAGE_DEPENDENCIES_LIST DEPENDENCY_NAME DEPENDENCY_SUBDIRECTORY)
  IRODS_MACRO_CHECK_DEPENDENCY_SET_FULLPATH(${DEPENDENCY_NAME} ${DEPENDENCY_SUBDIRECTORY})
  list(APPEND IRODS_PACKAGE_DEPENDENCIES_LIST irods-externals-${DEPENDENCY_SUBDIRECTORY})
endmacro()

set(BUILD_UNIT_TESTS YES CACHE BOOL "Build unit tests")
set(UNIT_TESTS_RUN_AFTER_BUILD NO CACHE BOOL "Run after building unit tests")
set(UNIT_TESTS_REPORTING_STYLE "junit" CACHE STRING "The style of output used for unit test reporting [console, compact, junit, xml]")
set(UNIT_TESTS_REPORT_FILENAME "report.xml" CACHE STRING "The filename of the unit test report")

if (NOT BUILD_UNIT_TESTS)
    message(STATUS "NOT BUILD UNIT TESTS")
    return()
endif()

IRODS_MACRO_CHECK_DEPENDENCY_SET_FULLPATH_ADD_TO_IRODS_PACKAGE_DEPENDENCIES_LIST(CATCH2 catch22.3.0-0)
string(REPLACE ";" ", " IRODS_PACKAGE_DEPENDENCIES_STRING "${IRODS_PACKAGE_DEPENDENCIES_LIST}")

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)
set(CMAKE_EXE_LINKER_FLAGS_INIT "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++ -Wl,-z,defs")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++ -Wl,-z,defs")

add_compile_options(-nostdinc++ -Wall -Wextra -Werror)
add_compile_options(-Wno-unused-parameter) # Until fix of irods_re_plugin.hpp
add_compile_options(-Wno-unneeded-internal-declaration) # Until fix of https://github.com/irods/irods/issues/3396
add_compile_options(-DIRODS_ENABLE_SYSLOG)

add_compile_definitions(PRIVATE
                        IRODS_ENABLE_SYSLOG
                        SPDLOG_FMT_EXTERNAL)

link_libraries(c++abi
               curl
               xml2
               crypto
               pthread
               ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so)

include_directories(${IRODS_INCLUDE_DIRS}
                    ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
                    ${IRODS_EXTERNALS_FULLPATH_JSON}/include
                    ${IRODS_EXTERNALS_FULLPATH_SPDLOG}/include
                    ${IRODS_EXTERNALS_FULLPATH_FMT}/include)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

# Enable CTest support.
enable_testing()

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

# Update the CMake module path so that the test compilation variables
# can be found.  Prepends the new path to the beginning of the list.
list(INSERT CMAKE_MODULE_PATH 0 ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Include helper functions and other utilities.

if (UNIT_TESTS_RUN_AFTER_BUILD)
    set(TEST_RUNNER_ARGS -r ${UNIT_TESTS_REPORTING_STYLE}
                         -o ${UNIT_TESTS_REPORT_FILENAME})
endif()

# List of cmake files defined under ./cmake/test_config.
# Each file in the ./cmake/test_config directory defines variables for a specific test.
# New tests should be added to this list.
set(TEST_INCLUDE_LIST test_config/irods_s3_transport
   )

foreach(IRODS_TEST_CONFIG ${TEST_INCLUDE_LIST})
    #unset_irods_test_variables()

    include(${IRODS_TEST_CONFIG})
    add_executable(${IRODS_TEST_TARGET} ${IRODS_TEST_SOURCE_FILES})
    set_property(TARGET ${IRODS_TEST_TARGET} PROPERTY CXX_STANDARD ${IRODS_CXX_STANDARD})
    target_include_directories(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_INCLUDE_PATH})
    target_link_libraries(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_LINK_LIBRARIES})

    # =======================
    # Post Build Instructions
    # =======================

    if (UNIT_TESTS_RUN_AFTER_BUILD)
        # Run the test immediately following successful compilation.
        add_custom_command(TARGET ${IRODS_TEST_TARGET}
                           POST_BUILD
                           COMMAND ${IRODS_TEST_TARGET} ${TEST_RUNNER_ARGS})
    endif()

    # Make the new test available to CTest.
    add_test(NAME ${IRODS_TEST_TARGET} COMMAND ${IRODS_TEST_TARGET} ${TEST_RUNNER_ARGS})
endforeach()

