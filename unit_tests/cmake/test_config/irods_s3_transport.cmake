set(IRODS_TEST_TARGET irods_s3_transport)

set(IRODS_TEST_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
                            "${CMAKE_CURRENT_SOURCE_DIR}/src/test_s3_transport.cpp")

set(IRODS_TEST_LINK_OBJLIBRARIES s3_transport_obj)

set(IRODS_TEST_INCLUDE_PATH ${IRODS_EXTERNALS_FULLPATH_BOOST}/include)

set(IRODS_TEST_LINK_LIBRARIES fmt::fmt
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so)
