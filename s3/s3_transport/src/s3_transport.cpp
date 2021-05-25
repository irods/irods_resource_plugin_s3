#include "circular_buffer.hpp"

// iRODS includes
#include <transport/transport.hpp>

// misc includes
#include "json.hpp"
#include <libs3.h>

// stdlib and misc includes
#include <string>
#include <thread>
#include <vector>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>
#include <ctime>

// boost includes
#include <boost/algorithm/string/predicate.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/list.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

// local includes
#include "s3_multipart_shared_data.hpp"
#include "s3_transport.hpp"


namespace irods::experimental::io::s3_transport
{
    int S3_status_is_retryable(S3Status status) {
        return ::S3_status_is_retryable(status) || S3StatusErrorQuotaExceeded == status || S3StatusErrorSlowDown == status || 128 == status;
    }


    void print_bucket_context(const libs3_types::bucket_context& bucket_context)
    {
        rodsLog(LOG_DEBUG, "BucketContext: [hostName=%s] [bucketName=%s][protocol=%d]"
               "[uriStyle=%d][accessKeyId=%s][secretAccessKey=%s]"
               "[securityToken=%s][stsDate=%d][region=%s]\n",
               bucket_context.hostName == nullptr ? "" : bucket_context.hostName,
               bucket_context.bucketName == nullptr ? "" : bucket_context.bucketName,
               bucket_context.protocol,
               bucket_context.uriStyle,
               bucket_context.accessKeyId == nullptr ? "" : bucket_context.accessKeyId,
               bucket_context.secretAccessKey == nullptr ? "" : bucket_context.secretAccessKey,
               bucket_context.securityToken == nullptr ? "" : bucket_context.securityToken,
               bucket_context.stsDate,
               bucket_context.authRegion);
    }

    void store_and_log_status( libs3_types::status status,
                               const libs3_types::error_details *error,
                               const std::string& function,
                               const libs3_types::bucket_context& saved_bucket_context,
                               libs3_types::status& pStatus,
                               unsigned long thread_id )
    {

        int log_level = LOG_DEBUG;

        if (thread_id == 0) {
            thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        }

        pStatus = status;
        if(status != libs3_types::status_ok ) {
            log_level = LOG_ERROR;
        }

        rodsLog(log_level,  "%s:%d [%s] [[%lu]]  libs3_types::status: [%s] - %d\n",
                __FILE__, __LINE__, __FUNCTION__, thread_id, S3_get_status_name( status ), static_cast<int>(status) );
        if (saved_bucket_context.hostName) {
            rodsLog(log_level,  "%s:%d [%s] [[%lu]]  S3Host: %s\n",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, saved_bucket_context.hostName );
        }

        rodsLog(log_level,  "%s:%d [%s] [[%lu]]  Function: %s\n",
                __FILE__, __LINE__, __FUNCTION__, thread_id, function.c_str() );

        if (error) {

            if (error->message) {
                rodsLog(log_level,  "%s:%d [%s] [[%lu]]  Message: %s\n",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, error->message);
            }
            if (error->resource) {
                rodsLog(log_level,  "%s:%d [%s] [[%lu]]  Resource: %s\n",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, error->resource);
            }
            if (error->furtherDetails) {
                rodsLog(log_level,  "%s:%d [%s] [[%lu]]  Further Details: %s\n",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, error->furtherDetails);
            }
            if (error->extraDetailsCount) {
                rodsLog(log_level,  "%s:%d [%s] [[%lu]]%s",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, "  Extra Details:\n");

                for (int i = 0; i < error->extraDetailsCount; i++) {
                    rodsLog(log_level,  "%s:%d [%s] [[%lu]]    %s: %s\n",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->extraDetails[i].name,
                            error->extraDetails[i].value);
                }
            }
        }
    }  // end store_and_log_status

    // Returns timestamp in usec for delta-t comparisons
    // uint64_t provides plenty of headroom
    uint64_t get_time_in_microseconds()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (tv.tv_sec) * 1000000LL + tv.tv_usec;
    } // end get_time_in_microseconds

    // Sleep between _s/2 to _s. 
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(
        int _s) {
        // We're the only user of libc rand(), so if we mutex around calls we can
        // use the thread-unsafe rand() safely and randomly...if this is changed
        // in the future, need to use rand_r and init a static seed in this function
        static std::mutex rand_mutex;
        rand_mutex.lock();
        int random = rand();
        rand_mutex.unlock();
        int sleep_time = (int)((((double)random / (double)RAND_MAX) + 1) * .5 * _s); // Sleep between _s/2 and _s.
        std::this_thread::sleep_for (std::chrono::seconds (sleep_time));
    }

    namespace s3_head_object_callback
    {
        libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                    void *callback_data)
        {
            data_for_head_callback *data = (data_for_head_callback*)callback_data;
            data->content_length = properties->contentLength;
            return libs3_types::status_ok;
        }

        void on_response_complete (libs3_types::status status,
                                   const libs3_types::error_details *error,
                                   void *callback_data)
        {
            data_for_head_callback *data = (data_for_head_callback*)callback_data;
            store_and_log_status( status, error, "s3_head_object_callback::on_response_complete", data->bucket_context,
                    data->status );
        }


    }

    namespace s3_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                std::string& shmem_key = manager->shmem_key;

                // upload upload_id in shared memory
                named_shared_memory_object shm_obj{shmem_key,
                    manager->shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return libs3_types::status_ok;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return libs3_types::status_ok;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_upload::on_response_complete", data->saved_bucket_context,
                        data->status );
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                long ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > static_cast<int64_t>(buffer_size)) ?
                                  static_cast<int64_t>(buffer_size) : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_upload::on_response_completion", data->saved_bucket_context,
                        data->status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback


        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = libs3_types::status_ok;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, "cancel_callback::on_response_completion", *g_response_completion_saved_bucket_context,
                        g_response_completion_status);
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_upload

    namespace s3_multipart_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                std::string& shmem_key = manager->shmem_key;

                named_shared_memory_object shm_obj{shmem_key,
                    manager->shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return libs3_types::status_ok;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return libs3_types::status_ok;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_multipart_upload::on_response_complete", data->saved_bucket_context,
                        data->status);
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                long ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > static_cast<int64_t>(buffer_size)) ?
                                  static_cast<int64_t>(buffer_size) : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_multipart_upload::on_response_completion", data->saved_bucket_context,
                        data->status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback


        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = libs3_types::status_ok;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, "cancel_callback::on_response_completion", *g_response_completion_saved_bucket_context,
                        g_response_completion_status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_multipart_upload


}

