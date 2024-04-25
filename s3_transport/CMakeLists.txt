include(CheckLibraryExists)
include(ObjectTargetHelpers)

check_library_exists("rt" "shm_open" "" LIBRT_HAS_SHM_OPEN)

add_library(
  s3_transport_obj
  OBJECT
  "${CMAKE_CURRENT_SOURCE_DIR}/src/s3_transport.cpp"
)
target_link_objects(
  s3_transport_obj
  PUBLIC
  libs3_obj
)
target_link_libraries(
  s3_transport_obj
  PUBLIC
  irods_common
  irods_server
  nlohmann_json::nlohmann_json
  fmt::fmt
  PRIVATE
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so"
)
if (LIBRT_HAS_SHM_OPEN)
  target_link_libraries(
    s3_transport_obj
    PUBLIC
    Threads::Threads
    rt
  )
endif()
target_include_directories(
  s3_transport_obj
  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  PRIVATE
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
)
target_compile_definitions(
  s3_transport_obj
  PRIVATE
  IRODS_ENABLE_SYSLOG
  ${IRODS_COMPILE_DEFINITIONS}
  ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
  BOOST_SYSTEM_NO_DEPRECATED
)

target_compile_options(s3_transport_obj PUBLIC -Wno-unused-parameter)

include(CheckCXXCompilerFlag)

# due to boost headers
check_cxx_compiler_flag(-Wno-error=deprecated-copy CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
if (CMAKE_CXX_COMPILER_W_DEPRECATED_COPY)
  target_compile_options(s3_transport_obj PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Wno-error=deprecated-copy>)
endif()

set_target_properties(s3_transport_obj PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(s3_transport_obj PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)

set_target_properties(s3_transport_obj PROPERTIES EXCLUDE_FROM_ALL TRUE)
