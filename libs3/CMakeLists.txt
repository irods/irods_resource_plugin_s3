add_library(
  libs3_obj
  OBJECT
  "${CMAKE_CURRENT_SOURCE_DIR}/src/bucket.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/bucket_metadata.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/error_parser.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/general.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/multipart.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/object.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/request.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/request_context.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/response_headers_handler.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/service.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/service_access_logging.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/simplexml.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/util.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/object_chunked.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/request_chunked.c"
)
target_link_libraries(
  libs3_obj
  PUBLIC
  CURL::libcurl
  LibXml2::LibXml2
  OpenSSL::Crypto
)
target_include_directories(
  libs3_obj
  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
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
target_compile_options(
  libs3_obj
  PRIVATE
  -Wshadow
  -Wno-unused-function # due to OPENSSL_NO_DEPRECATED_1_1_0
)
set_target_properties(libs3_obj PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(libs3_obj PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)

set_target_properties(libs3_obj PROPERTIES EXCLUDE_FROM_ALL TRUE)
