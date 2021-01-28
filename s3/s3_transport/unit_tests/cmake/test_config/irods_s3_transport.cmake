set(IRODS_TEST_TARGET irods_s3_transport)

set(IRODS_TEST_SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
                            ${CMAKE_CURRENT_SOURCE_DIR}/src/test_s3_transport.cpp
                            ${CMAKE_CURRENT_SOURCE_DIR}/../src/s3_transport.cpp)

                        set(IRODS_TEST_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../include
                            ${IRODS_EXTERNALS_FULLPATH_CATCH2}/include
                            ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
                            ${IRODS_EXTERNALS_FULLPATH_JSON}/include
                            ${IRODS_EXTERNALS_FULLPATH_S3}/include
                            /usr/include/libxml2
                            ${IRODS_INCLUDE_DIRS}
                            )

set(IRODS_TEST_LINK_LIBRARIES irods_common
                              irods_client
                              c++abi
                              rt
                              ${IRODS_EXTERNALS_FULLPATH_S3}/lib/libs3.so
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
                              ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so
                              )
