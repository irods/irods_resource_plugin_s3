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
#include <chrono>
#include <utility>

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
#include "managed_shared_memory_object.hpp"
#include "s3_transport_util.hpp"
#include "s3_transport_types.hpp"
#include "s3_transport_callbacks.hpp"

int main(int argc, char **argv) {

    using named_shared_memory_object =
        irods::experimental::interprocess::shared_memory::named_shared_memory_object
        <irods::experimental::io::s3_transport::shared_data::multipart_shared_data>;

    if (argc != 2) {
        fprintf(stderr, "Need shmem key\n");
        return 0;
    }

    const char *key = argv[1];

    named_shared_memory_object shm_obj{key, 60,
        irods::experimental::io::s3_transport::constants::MAX_S3_SHMEM_SIZE};

    shm_obj.atomic_exec([](auto& data)
    {
        printf("[threads_remaining_to_close=%d][upload_id=%s][last_error_code=%d][cache_file_download_progress=%d]"
                "[ref_count=%d]\n", data.threads_remaining_to_close, data.upload_id.c_str(), data.last_error_code,
                data.cache_file_download_progress, data.ref_count);
    });

    return 0;
}

