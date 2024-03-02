include(ObjectTargetHelpers)

add_library(
  s3_resource_obj
  OBJECT
  "${CMAKE_CURRENT_SOURCE_DIR}/src/s3_resource.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/s3_operations.cpp"
)
target_link_objects(
  s3_resource_obj
  PUBLIC
  libs3_obj
  s3_transport_obj
)
target_link_libraries(
  s3_resource_obj
  PUBLIC
  irods_common
  irods_server
  fmt::fmt
  Threads::Threads
  PRIVATE
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so"
)
target_include_directories(
  s3_resource_obj
  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  PRIVATE
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
)
target_compile_definitions(
  s3_resource_obj
  PRIVATE
  IRODS_QUERY_ENABLE_SERVER_SIDE_API
  IRODS_ENABLE_SYSLOG
  ${IRODS_COMPILE_DEFINITIONS}
  ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
  BOOST_SYSTEM_NO_DEPRECATED
)

include(CheckCXXCompilerFlag)

# due to boost headers
check_cxx_compiler_flag(-Wno-error=deprecated-copy CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
if (CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
  target_compile_options(s3_resource_obj PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Wno-error=deprecated-copy>)
endif()

set_target_properties(s3_resource_obj PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(s3_resource_obj PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)

set_target_properties(s3_resource_obj PROPERTIES EXCLUDE_FROM_ALL TRUE)
