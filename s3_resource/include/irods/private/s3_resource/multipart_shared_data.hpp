#ifndef IRODS_S3_RESOURCE_MULTIPART_SHARED_DATA_HPP
#define IRODS_S3_RESOURCE_MULTIPART_SHARED_DATA_HPP

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

#include <iostream>

namespace irods_s3 
{
    namespace interprocess_types
    {
        namespace bi          = boost::interprocess;
        using segment_manager = bi::managed_shared_memory::segment_manager;
        using void_allocator  = boost::container::scoped_allocator_adaptor<bi::allocator<void, segment_manager> >;
    }

    // data that needs to be shared among different processes
    struct multipart_shared_data
    {
        explicit multipart_shared_data(const interprocess_types::void_allocator &allocator)
            : threads_remaining_to_close{0}
            , number_of_threads{0}
            , ref_count{0}
        {}

        bool can_delete() {
            return threads_remaining_to_close == 0;
        }

        int threads_remaining_to_close;
        int number_of_threads;
        int ref_count;
    }; // struct multipart_shared_data
} // namespace irods_s3

#endif // IRODS_S3_RESOURCE_MULTIPART_SHARED_DATA_HPP
