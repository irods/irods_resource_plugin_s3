#ifndef IRODS_S3_TRANSPORT_MULTIPART_SHARED_DATA_HPP
#define IRODS_S3_TRANSPORT_MULTIPART_SHARED_DATA_HPP

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/list.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

#include "irods/private/s3_transport/types.hpp"

namespace irods::experimental::io::s3_transport::shared_data
{

    namespace interprocess_types
    {

        namespace bi = boost::interprocess;

        using segment_manager       = bi::managed_shared_memory::segment_manager;
        using void_allocator        = boost::container::scoped_allocator_adaptor
                                      <bi::allocator<void, segment_manager> >;
        using int_allocator         = bi::allocator<int, segment_manager>;
        using char_allocator        = bi::allocator<char, segment_manager>;
        using shm_int_vector        = bi::vector<int, int_allocator>;
        using shm_char_string       = bi::basic_string<char, std::char_traits<char>,
                                      char_allocator>;
        using char_string_allocator = bi::allocator<shm_char_string, segment_manager>;
        using shm_string_vector     = bi::vector<shm_char_string, char_string_allocator>;
    }

    // data that needs to be shared among different processes
    struct multipart_shared_data
    {
        using interprocess_recursive_mutex = boost::interprocess::interprocess_recursive_mutex;
        using error_codes = irods::experimental::io::s3_transport::error_codes;

        explicit multipart_shared_data(const interprocess_types::void_allocator &allocator)
            : threads_remaining_to_close{0}
            , done_initiate_multipart{false}
            , upload_id{allocator}
            , etags{allocator}
            , last_error_code{error_codes::SUCCESS}
            , cache_file_download_progress{cache_file_download_status::NOT_STARTED}
            , ref_count{0}
            , existing_object_size{-1}
            , circular_buffer_read_timeout{false}
            , file_open_counter{0}
            , cache_file_flushed{false}
            , know_number_of_threads{true}
            , first_open_has_trunc_flag{false}
            , total_parts_expected{0}
            , multipart_upload_completion_started{false}
            , multipart_upload_completion_finished{false}
            , multipart_upload_completion_result{error_codes::SUCCESS}
        {}

        bool can_delete() {
            return know_number_of_threads
                   ? threads_remaining_to_close == 0
                   : file_open_counter == 0;
        }

        int                                   threads_remaining_to_close;
        bool                                  done_initiate_multipart;
        interprocess_types::shm_char_string   upload_id;
        interprocess_types::shm_string_vector etags;
        error_codes                           last_error_code;
        cache_file_download_status            cache_file_download_progress;
        int                                   ref_count;
        std::int64_t                          existing_object_size;
        bool                                  circular_buffer_read_timeout;
        int                                   file_open_counter;
        bool                                  cache_file_flushed;
        bool                                  know_number_of_threads;

        // this is set so that multiple processes that are used to write to the file don't download the file
        // to cache if the trunc flag is not set.
        bool                                  first_open_has_trunc_flag;

        // Issue 2319: This is the true expected part count for this multipart upload, computed once
        // by whichever thread/process initiates it.
        std::int64_t                          total_parts_expected;

        // Set atomically (alongside the etag write that makes every expected part's etag present - see
        // on_response_properties in callbacks.hpp) by whichever part-upload worker thread/process writes
        // the last part's etag; that thread then calls complete_multipart_upload() itself, directly, rather
        // than a separate close()-side wait discovering readiness later and making the call itself. Only
        // ever transitions false -> true once, so exactly one thread ever performs the completion call.
        bool                                   multipart_upload_completion_started;

        // Set true by whichever thread claimed multipart_upload_completion_started, after its
        // complete_multipart_upload() call has actually returned (result in
        // multipart_upload_completion_result below). This, not etag completeness, is what
        // wait_for_multipart_upload_completion() blocks on.
        bool                                   multipart_upload_completion_finished;
        error_codes                            multipart_upload_completion_result;

        // Dedicated lock/condvar pair for wait_for_multipart_upload_completion().
        boost::interprocess::interprocess_mutex     etags_mutex;
        boost::interprocess::interprocess_condition etags_cv;
    };

}



#endif // IRODS_S3_TRANSPORT_MULTIPART_SHARED_DATA_HPP
