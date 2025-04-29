set(IRODS_UNIT_TESTS_BUILD NO CACHE BOOL "Build unit tests")
set(IRODS_UNIT_TESTS_REPORTING_STYLE "junit" CACHE STRING "The style of output used for unit test reporting [console, compact, junit, xml]")
set(IRODS_UNIT_TESTS_REPORT_DIR "${CMAKE_CURRENT_BINARY_DIR}/reports" CACHE STRING "The directory of the unit test reports")
set(IRODS_UNIT_TESTS_REPORT_FILENAME_PREFIX "" CACHE STRING "The filename prefix of the unit test report")
set(IRODS_UNIT_TESTS_REPORT_FILENAME_SUFFIX ".xml" CACHE STRING "The filename suffix of the unit test report")

if (DEFINED IRODS_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH_INIT)
  set(DEFAULT_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH ${IRODS_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH_INIT})
else()
  set(DEFAULT_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH NO)
endif()
set(IRODS_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH "${DEFAULT_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH}" CACHE BOOL "Build unit tests with install RPATH/RUNPATH")

if (NOT IRODS_UNIT_TESTS_BUILD)
  return()
endif()

find_package(Catch2 3.4)
if (NOT Catch2_FOUND)
  find_package(Catch2 2.13.2 REQUIRED)
endif()

include(Catch)

# create phony target to build all unit tests
add_custom_target(all-unit_tests)

# Enable CTest support.
enable_testing()

# Include helper functions and other utilities.
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/utils.cmake")

# Each file in the ./cmake/test_config directory defines variables for a specific test.
# New tests should be added to this list.
set(
  IRODS_PLUGIN_UNIT_TESTS
  s3_transport
)

foreach(test IN LISTS IRODS_PLUGIN_UNIT_TESTS)
  unset_irods_test_variables()

  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/test_config/irods_${test}.cmake")
  add_executable(
    ${IRODS_TEST_TARGET}
    ${IRODS_TEST_SOURCE_FILES}
  )
  if (Catch2_VERSION VERSION_LESS "3.0.0")
    target_sources(
      ${IRODS_TEST_TARGET}
      PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/src/catch2_compat_include/catch2/catch_all.hpp"
    )
    target_link_libraries(
      ${IRODS_TEST_TARGET}
      PRIVATE
      Catch2::Catch2
    )
    target_include_directories(
      ${IRODS_TEST_TARGET}
      PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/src/catch2_compat_include"
    )
    if (NOT IRODS_TEST_PROVIDES_MAIN)
      target_sources(
        ${IRODS_TEST_TARGET}
        PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
      )
    endif()
  else()
    if (NOT IRODS_TEST_PROVIDES_MAIN)
      target_link_libraries(
        ${IRODS_TEST_TARGET}
        PRIVATE
        Catch2::Catch2WithMain
      )
    else()
      target_link_libraries(
        ${IRODS_TEST_TARGET}
        PRIVATE
        Catch2::Catch2
      )
    endif()
  endif()
  target_compile_definitions(${IRODS_TEST_TARGET} PRIVATE ${IRODS_COMPILE_DEFINITIONS_PRIVATE})
  if (DEFINED IRODS_TEST_LINK_OBJLIBRARIES)
    target_link_objects(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_LINK_OBJLIBRARIES})
  endif()
  target_link_libraries(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_LINK_LIBRARIES})
  if (DEFINED IRODS_TEST_INCLUDE_PATH)
    target_include_directories(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_INCLUDE_PATH})
  endif()
  set_property(TARGET ${IRODS_TEST_TARGET} PROPERTY BUILD_WITH_INSTALL_RPATH ${IRODS_UNIT_TESTS_BUILD_WITH_INSTALL_RPATH})
  if (IRODS_UNIT_TESTS_ENABLE_ALL)
    target_compile_definitions(${IRODS_TEST_TARGET} PRIVATE IRODS_ENABLE_ALL_UNIT_TESTS)
  endif()

  add_dependencies(all-unit_tests ${IRODS_TEST_TARGET})
  install(
    TARGETS ${IRODS_TEST_TARGET}
    RUNTIME
    DESTINATION "${IRODS_HOME_DIRECTORY}/unit_tests"
  )

  catch_discover_tests(
    ${IRODS_TEST_TARGET}
    REPORTER "${IRODS_UNIT_TESTS_REPORTING_STYLE}"
    OUTPUT_DIR "${IRODS_UNIT_TESTS_REPORT_DIR}"
    OUTPUT_PREFIX "${IRODS_UNIT_TESTS_REPORT_FILENAME_PREFIX}"
    OUTPUT_SUFFIX "${IRODS_UNIT_TESTS_REPORT_FILENAME_SUFFIX}"
  )
endforeach()
