#ifndef S3_TRANSPORT_MANAGED_SHARED_MEMORY_OBJECT_HPP
#define S3_TRANSPORT_MANAGED_SHARED_MEMORY_OBJECT_HPP

#include <irods/rodsLog.h>

#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <utility>

#include "irods/private/s3_transport/logging_category.hpp"

namespace irods::experimental::interprocess
{

    namespace shared_memory
    {
        namespace bi = boost::interprocess;
        namespace bc = boost::container;

        namespace log  = irods::experimental::log;
        using logger = log::logger<s3_transport_logging_category>;

        using segment_manager       = bi::managed_shared_memory::segment_manager;
        using void_allocator        = bc::scoped_allocator_adaptor<bi::allocator<void, segment_manager>>;

        template <typename T>
        class named_shared_memory_object
        {

        private:

            struct ipc_object
            {
                template <typename... Args>
                ipc_object(void_allocator&& alloc_inst, time_t access_time,
                        Args&& ... args)
                    : thing(alloc_inst, std::forward<Args>(args)...)
                    , last_access_time_in_seconds(access_time)
                {}

                // T must have ref_count and can_delete()
                T thing;

                time_t last_access_time_in_seconds;
                bi::interprocess_recursive_mutex access_mutex;

            };

        public:

            named_shared_memory_object(const named_shared_memory_object&) = delete;
            auto operator=(const named_shared_memory_object&) ->
                named_shared_memory_object& = delete;

            template <typename... Args>
            named_shared_memory_object(const std::string& shm_name,
                    time_t shared_memory_timeout_in_seconds,
                    std::uint64_t shm_size,
                    Args&& ...args)

                : shm_name_{shm_name}
                , shm_size_{shm_size}
                , shm_{bi::open_or_create, shm_name_.c_str(), shm_size_}
                , alloc_inst_{shm_.get_segment_manager()}

            {
                const time_t now = time(0);

                bi::named_mutex create_delete_reset_mutex(bi::open_or_create, shm_name_.c_str());
                bi::scoped_lock lk{create_delete_reset_mutex};

                object_ = shm_.find_or_construct<ipc_object>(SHARED_DATA_NAME.c_str())
                    (  static_cast<void_allocator>(shm_.get_segment_manager()), now,
                       std::forward<Args>(args)...);

                (object_->thing.ref_count)++;

                const bool shmem_has_expired = now -
                    object_->last_access_time_in_seconds
                    > shared_memory_timeout_in_seconds;

                if (shmem_has_expired) {

                    logger::debug("{}:{} ({}) SHMEM_HAS_EXPIRED", __FILE__, __LINE__, __func__);

                    // rebuild shmem object
                    shm_.destroy<ipc_object>(SHARED_DATA_NAME.c_str());
                    object_ = shm_.find_or_construct<ipc_object>(SHARED_DATA_NAME.c_str())
                        (  static_cast<void_allocator>(shm_.get_segment_manager()), now,
                           std::forward<Args>(args)...);

                    object_->thing.ref_count = 1;
                }
                object_->last_access_time_in_seconds = now;
            }

            ~named_shared_memory_object()
            {
                {
                    bi::named_mutex create_delete_reset_mutex(bi::open_or_create, shm_name_.c_str());
                    bi::scoped_lock lk{create_delete_reset_mutex};

                    (object_->thing.ref_count)--;

                    bool can_delete = object_->thing.can_delete();

                    if (object_->thing.ref_count == 0 && can_delete) {

                        object_->thing.~T();
                        object_ = nullptr;
                        if (!bi::shared_memory_object::remove(shm_name_.c_str())) {
                            logger::error("{}:{} ({}) removal of shared memory object [{}] failed", __FILE__, __LINE__, __func__, shm_name_);
                        }
                        if (!bi::named_mutex::remove(shm_name_.c_str())) {
                            logger::error("{}:{} ({}) removal of mutex for shared memory object [{}] failed", __FILE__, __LINE__, __func__, shm_name_);
                        }
                    }
                }
            }

            template <typename Function>
            auto atomic_exec(Function _func) const
            {
                bi::scoped_lock lk{object_->access_mutex};
                object_->last_access_time_in_seconds = time(0);
                return _func(object_->thing);
            }

            template <typename Function>
            auto exec(Function _func) const
            {
                object_->last_access_time_in_seconds = time(0);
                return _func(object_->thing);
            }

            void_allocator& get_allocator() {
                return alloc_inst_;
            }

            auto get_free_memory() {
                return shm_.get_free_memory();
            }

        private:

            const std::string shm_name_;
            const std::uint64_t shm_size_;
            bi::managed_shared_memory shm_;
            void_allocator alloc_inst_;

            ipc_object* object_;

            const std::string SHARED_DATA_NAME{"SharedData"};

        }; // class shared_memory_object

    } // namespace shared_memory
} // namespace irods::experimental::ipc

#endif // S3_TRANSPORT_MANAGED_SHARED_MEMORY_OBJECT_HPP
