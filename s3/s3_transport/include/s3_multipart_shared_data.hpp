#ifndef S3_MULTIPART_TEST_SHARED_DATA
#define S3_MULTIPART_TEST_SHARED_DATA

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

#include "s3_transport_types.hpp"

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
        {}

        void reset_fields()
        {
            threads_remaining_to_close = 0;
            done_initiate_multipart = false;
            upload_id = "";
            etags.clear();
            last_error_code = error_codes::SUCCESS;
            cache_file_download_progress = cache_file_download_status::NOT_STARTED;
            ref_count = 1;   // current object has reference so ref_count = 1
            circular_buffer_read_timeout = false;
            file_open_counter = 0;
            cache_file_flushed = false;
            know_number_of_threads = true;
        }

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
    };

}



#endif // S3_MULTIPART_TEST_SHARED_DATA
