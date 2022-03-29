#ifndef S3_TRANSPORT_UTIL_HPP
#define S3_TRANSPORT_UTIL_HPP

#include "circular_buffer.hpp"

// iRODS includes
#include <irods/rcMisc.h>
#include <irods/transport/transport.hpp>

// misc includes
#include <nlohmann/json.hpp>
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
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

// local includes
#include "s3_multipart_shared_data.hpp"

#include "s3_transport_types.hpp"

namespace irods::experimental::io::s3_transport
{

    struct constants
    {

        static const std::int64_t            MAXIMUM_NUMBER_ETAGS_PER_UPLOAD{10000};
        static const std::int64_t            BYTES_PER_ETAG{112};  // 80 bytes for every string added, 32 bytes for the vector size,
                                                              // determined by testing
        static const std::int64_t            UPLOAD_ID_SIZE{128};
        static const std::int64_t            MAX_S3_SHMEM_SIZE{sizeof(shared_data::multipart_shared_data) +
                                                          MAXIMUM_NUMBER_ETAGS_PER_UPLOAD * (BYTES_PER_ETAG + 1) +
                                                          UPLOAD_ID_SIZE + 1};

        static const int                DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS{900};
        inline static const std::string SHARED_MEMORY_KEY_PREFIX{"irods_s3-shm-"};
    };

    void print_bucket_context( const libs3_types::bucket_context& bucket_context );

    void store_and_log_status( libs3_types::status status,
                               const libs3_types::error_details *error,
                               const std::string& function,
                               const libs3_types::bucket_context& saved_bucket_context,
                               libs3_types::status& pStatus,
                               std::uint64_t thread_id = 0);
    // Sleep between _s / 2 and _s seconds.
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep( int _s );

    // Returns timestamp in usec for delta-t comparisons
    auto get_time_in_microseconds() -> std::uint64_t;

    struct upload_manager
    {
        explicit upload_manager(libs3_types::bucket_context& _saved_bucket_context)
            : saved_bucket_context{_saved_bucket_context}
            , xml{""}
            , remaining{0}
            , offset{0}
            , shared_memory_timeout_in_seconds{60}
        {
        }

        libs3_types::bucket_context& saved_bucket_context;             /* To enable more detailed error messages */

        /* Below used for the upload completion command, need to send in XML */
        std::string              xml;

        std::int64_t                  remaining;
        std::int64_t                  offset;
        libs3_types::status      status;            /* status returned by libs3 */
        std::string              object_key;
        std::string              shmem_key;
        time_t                   shared_memory_timeout_in_seconds;
    };

    struct data_for_write_callback
    {
        data_for_write_callback(libs3_types::bucket_context& _saved_bucket_context,
                                irods::experimental::circular_buffer<libs3_types::char_type>& _circular_buffer)
            : offset{0}
            , circular_buffer{_circular_buffer}
            , content_length{0}
            , bytes_written{0}
            , saved_bucket_context{_saved_bucket_context}
            , thread_identifier{0}
        {}

        libs3_types::char_type *buffer;
        std::int64_t                offset;

        irods::experimental::circular_buffer<libs3_types::char_type>&
                               circular_buffer;

        std::int64_t                content_length;
        std::int64_t                bytes_written;
        libs3_types::status    status;

        libs3_types::bucket_context&
                               saved_bucket_context;   // To enable more detailed error messages
        std::uint64_t               thread_identifier;
    };

    struct data_for_head_callback
    {
        explicit data_for_head_callback(libs3_types::bucket_context& _bucket_context)
            : last_modified{0}
            , content_length{0}
            , x_amz_storage_class{}   // for glacier
            , x_amz_restore{}         // for glacier
            , status{libs3_types::status_ok}
            , bucket_context{_bucket_context}
        {}

        time_t                             last_modified;
        std::int64_t                            content_length;
        std::string                        x_amz_storage_class;
        std::string                        x_amz_restore;
        libs3_types::status                status;
        libs3_types::bucket_context&       bucket_context;
    };

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_UTIL_HPP
