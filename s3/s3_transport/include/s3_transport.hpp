#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "circular_buffer.hpp"

// iRODS includes
#include <irods/transport/transport.hpp>
#include <irods/thread_pool.hpp>
#include <irods/rcMisc.h>
#include <irods/rodsErrorTable.h>
#include <irods/irods_error.hpp>

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
#include <chrono>
#include <utility>
#include <fmt/format.h>

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
#include "s3_transport_util.hpp"
#include "s3_transport_callbacks.hpp"

extern const unsigned int S3_DEFAULT_NON_DATA_TRANSFER_TIMEOUT_SECONDS;

namespace irods::experimental::io::s3_transport
{
    extern const int          S3_DEFAULT_CIRCULAR_BUFFER_SIZE;
    extern const std::string  S3_DEFAULT_RESTORATION_TIER;
    extern const unsigned int S3_DEFAULT_RESTORATION_DAYS;

    const int64_t DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE = 5L * 1024 * 1024 * 1024;

    enum class object_s3_status { DOES_NOT_EXIST, IN_S3, IN_GLACIER, IN_GLACIER_RESTORE_IN_PROGRESS };

    irods::error get_object_s3_status(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            int64_t& object_size,
            object_s3_status& object_status);

    irods::error handle_glacier_status(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            const unsigned int restoration_days,
            const std::string& restoration_tier,
            object_s3_status object_status);

    irods::error restore_s3_object(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            const unsigned int restoration_days,
            const std::string& restoration_tier);

    int S3_status_is_retryable(S3Status status);

    struct config
    {

        config()
            : object_size{UNKNOWN_OBJECT_SIZE}
            , number_of_cache_transfer_threads{1}  // this is the number of transfer threads when transferring from cache
            , number_of_client_transfer_threads{0}  // this is the number of transfer threads defined by iRODS for PUTs, GETs
            , bytes_this_thread{1000}
            , retry_count_limit{3}
            , retry_wait_seconds{3}
            , max_retry_wait_seconds{30}
            , hostname{"s3.amazonaws.com"}
            , region_name{"us-east-1"}
            , bucket_name{""}
            , access_key{""}
            , secret_access_key{""}
            , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
            , enable_md5_flag{false}
            , server_encrypt_flag{false}
            , s3_protocol_str{"http"}
            , s3_sts_date_str{"amz"}
            , cache_directory{"/tmp"}
            , circular_buffer_size{DEFAULT_MINIMUM_PART_SIZE * S3_DEFAULT_CIRCULAR_BUFFER_SIZE}
            , circular_buffer_timeout_seconds{120}
            , s3_uri_request_style{""}
            , minimum_part_size{DEFAULT_MINIMUM_PART_SIZE}
            , multipart_enabled{true}
            , put_repl_flag{false}
            , resource_name{""}
            , restoration_days{S3_DEFAULT_RESTORATION_DAYS}
            , restoration_tier{S3_DEFAULT_RESTORATION_TIER}
            , max_single_part_upload_size{DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE}
            , non_data_transfer_timeout_seconds{S3_DEFAULT_NON_DATA_TRANSFER_TIMEOUT_SECONDS}

        {}

        int64_t      object_size;
        unsigned int number_of_cache_transfer_threads;  // only used when doing full file upload/download via cache
        unsigned int number_of_client_transfer_threads; // controlled by iRODS
        int64_t      bytes_this_thread;                  // only used when doing a multipart upload
        unsigned int retry_count_limit;
        int          retry_wait_seconds;
        int          max_retry_wait_seconds;
        std::string  hostname;
        std::string  region_name;
        std::string  bucket_name;
        std::string  access_key;
        std::string  secret_access_key;
        time_t       shared_memory_timeout_in_seconds;
        bool         enable_md5_flag;                  // this flag currently has no effect but this behavior
                                                       // may be implemented later
        bool         server_encrypt_flag;
        std::string  s3_protocol_str;
        std::string  s3_sts_date_str;
        std::string  cache_directory;
        uint64_t     circular_buffer_size;
        int          circular_buffer_timeout_seconds;
        std::string  s3_uri_request_style;
        int64_t      minimum_part_size;
        bool         multipart_enabled;
        static const int64_t  UNKNOWN_OBJECT_SIZE = -1;
        static const uint64_t DEFAULT_MINIMUM_PART_SIZE = 5*1024*1024;
        int          developer_messages_log_level = LOG_DEBUG;

        // If the put_repl_flag is true, this is a promise that all writes will be performed in a
        // manner similar to iput.  This means:
        //   - If single threaded, the bytes will be written sequentially from the beginning of
        //     the file to the end of the file.
        //   - If multiple threaded, each thread will write its bytes sequentially from the beginning
        //     of its part to the end of its part.  The parts will be broken up in the same manner as parallel
        //     iput.  (This is required to determine the S3 part number from the offset.)
        //
        // Note:  If the put_repl_flag is set to true but the above rules are not followed, the behavior
        //        is undefined.
        bool         put_repl_flag;

        std::string  resource_name;
        unsigned int restoration_days;
        std::string  restoration_tier;

        int64_t max_single_part_upload_size;
        unsigned int non_data_transfer_timeout_seconds;
    };


    template <typename CharT>
    class s3_transport : public transport<CharT>
    {
    public:

        // clang-format off
        using char_type   = typename transport<CharT>::char_type;
        using buffer_type = std::vector<char_type>;
        using traits_type = typename transport<CharT>::traits_type;
        using int_type    = typename traits_type::int_type;
        using pos_type    = typename traits_type::pos_type;
        using off_type    = typename traits_type::off_type;
        // clang-format on

    private:

        using named_shared_memory_object =
            irods::experimental::interprocess::shared_memory::named_shared_memory_object
            <shared_data::multipart_shared_data>;

        // clang-format off
        const static int uninitialized_file_descriptor = -1;
        const static int minimum_valid_file_descriptor = 3;

        // Errors
        inline static constexpr auto translation_error             = -1;
        inline static const     auto seek_error                    = pos_type{off_type{-1}};

        // clang-format on

    public:

        explicit s3_transport(const config& _config)

            : transport<CharT>{}
            , root_resc_name_{}
            , leaf_resc_name_{}
            , replica_number_{}
            , replica_token_{}
            , config_{_config}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
            , call_s3_upload_part_flag_{true}
            , call_s3_download_part_flag_{true}
            , begin_part_upload_thread_ptr_{nullptr}
            , circular_buffer_{_config.circular_buffer_size, _config.circular_buffer_timeout_seconds}
            , mode_{0}
            , file_offset_{0}
            , existing_object_size_{config::UNKNOWN_OBJECT_SIZE}
            , download_to_cache_{true}
            , use_cache_{true}
            , object_must_exist_{false}
            , bucket_context_{}
            , upload_manager_{bucket_context_}
            , last_file_to_close_{false}
            , error_{SUCCESS()}
        {

            upload_manager_.shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;

            bucket_context_.hostName        = config_.hostname.c_str();
            bucket_context_.bucketName      = config_.bucket_name.c_str();
            bucket_context_.accessKeyId     = config_.access_key.c_str();
            bucket_context_.secretAccessKey = config_.secret_access_key.c_str();
            bucket_context_.authRegion      = config_.region_name.c_str();

            if (boost::iequals(_config.s3_protocol_str, "http")) {
                bucket_context_.protocol    = S3ProtocolHTTP;
            } else {
                bucket_context_.protocol    = S3ProtocolHTTPS;
            }

            if (boost::iequals(_config.s3_sts_date_str, "amz")) {
                bucket_context_.stsDate     = S3STSAmzOnly;
            } else if (boost::iequals(_config.s3_sts_date_str, "both")) {
                bucket_context_.stsDate     = S3STSAmzAndDate;
            } else {
                bucket_context_.stsDate     = S3STSDateOnly;
            }

            if (boost::iequals(_config.s3_uri_request_style, "virtual") ||
                    boost::iequals(_config.s3_uri_request_style, "host") ||
                    boost::iequals(_config.s3_uri_request_style, "virtualhost")) {

                bucket_context_.uriStyle    = S3UriStyleVirtualHost;
            } else {
                bucket_context_.uriStyle    = S3UriStylePath;
            }
        }

        ~s3_transport()
        {

            if (begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_ -> join();
                begin_part_upload_thread_ptr_ = nullptr;
            }

            // if using cache, go ahead and close the fstream
            if (use_cache_) {
                if (cache_fstream_ && cache_fstream_.is_open()) {
                    cache_fstream_.close();
                }
            }

            // each process must initialize and deinitiatize.
            {
                std::lock_guard<std::mutex> lock(s3_initialized_counter_mutex_);
                if (--s3_initialized_counter_ == 0) {
                    S3_deinitialize();
                }
            }

        }

        off_t get_offset() {
            if (use_cache_) {
                return cache_fstream_.tellg();
            } else {
                return get_file_offset();
            }

        }

        bool open(const irods::experimental::filesystem::path& _p,
                  std::ios_base::openmode _mode) override
        {
            return !is_open()
                ? open_impl(_p, _mode, [](auto&) {})
                : false;
        }

        bool open(const irods::experimental::filesystem::path& _p,
                  const root_resource_name& _root_resource_name,
                  std::ios_base::openmode _mode) override
        {
            if (is_open()) {
                return false;
            }

            return open_impl(_p, _mode, [&_root_resource_name](auto& _input) {
                addKeyVal(&_input.condInput, RESC_NAME_KW, _root_resource_name.value.c_str());
            });
        }

        bool open(const irods::experimental::filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode) override
        {
            // This is when the client knows exactly where the replica should reside.
            return open_impl(_path, _mode, [&_leaf_resource_name](auto& _input) {
                addKeyVal(&_input.condInput, LEAF_RESOURCE_NAME_KW, _leaf_resource_name.value.c_str());
            });
        }

        bool open(const irods::experimental::filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [_replica_number](auto& _input) {
                const auto replica = std::to_string(_replica_number.value);
                addKeyVal(&_input.condInput, REPL_NUM_KW, replica.c_str());

                // Providing a replica number implies that the replica already exists.
                // This constructor does not support creation of new replicas.
                _input.openFlags &= ~O_CREAT;
            });
        }


        bool open(const replica_token& _replica_token,
                  const irods::experimental::filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [_replica_token, _replica_number](auto& _input) {
                const auto replica = std::to_string(_replica_number.value);
                addKeyVal(&_input.condInput, REPLICA_TOKEN_KW, _replica_token.value.data());
                addKeyVal(&_input.condInput, REPL_NUM_KW, replica.c_str());

                // Providing a replica number implies that the replica already exists.
                // This constructor does not support creation of new replicas.
                _input.openFlags &= ~O_CREAT;
            });
        }

        bool open(const replica_token& _replica_token,
                  const irods::experimental::filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [_replica_token, &_leaf_resource_name](auto& _input) {
                addKeyVal(&_input.condInput, REPLICA_TOKEN_KW, _replica_token.value.data());
                addKeyVal(&_input.condInput, LEAF_RESOURCE_NAME_KW, _leaf_resource_name.value.c_str());
            });
        }

        bool close(const on_close_success* _on_close_success = nullptr) override
        {
            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] fd_=%d, is_open=%d use_cache_=%d\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), fd_, is_open(), use_cache_);

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            bool return_value = true;

            if (!is_open()) {
                return false;
            }

            fd_ = uninitialized_file_descriptor;

            // If the size == 0 and we were not using cache, the call to send() did not
            // pass through transport.  Call send here
            if ((mode_ & std::ios_base::out) && !use_cache_ && config_.object_size == 0) {
                send("", 0);
            }

            // wait for the upload thread to complete
            if (begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_->join();
                begin_part_upload_thread_ptr_ = nullptr;
            }

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            enum struct additional_processing_enum {
                CONTINUE,
                DO_FLUSH_CACHE_FILE
            };

            // only allow one open/close to happen at a time
            auto result = shm_obj.atomic_exec([this, &return_value](auto& data) -> additional_processing_enum
            {
                additional_processing_enum rv = additional_processing_enum::CONTINUE;

                // do close processing if # files open = 0
                //  - for multipart upload send the complete message
                //  - if using a cache file flush the cache and delete cache file
                if (data.threads_remaining_to_close > 0) {
                    data.threads_remaining_to_close -= 1;
                }

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] close BEFORE decrement file_open_counter = %d\n",
                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), data.file_open_counter);

                if (data.file_open_counter > 0) {
                    data.file_open_counter -= 1;
                }

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] close AFTER decrement file_open_counter = %d\n",
                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), data.file_open_counter);

                // determine if this is the last file to close
                // for now on redirect cache is forced and we do not know the # threads
                // so use the file_open_counter
                last_file_to_close_ =
                    ( data.know_number_of_threads && data.threads_remaining_to_close == 0 )  ||
                    ( !(data.know_number_of_threads) && data.file_open_counter == 0 && !data.cache_file_flushed );

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] [last_file_to_close=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(),
                        last_file_to_close_);

                // if a critical error occurred - do not flush cache file or complete multipart upload

                if (!this->error_.ok()) {

                    return_value = false;

                } else if (last_file_to_close_) {

                    if (this->use_cache_) {

                        rv = additional_processing_enum::DO_FLUSH_CACHE_FILE;
                        data.cache_file_flushed = true;

                    } else {


                        if ( this->use_streaming_multipart()  ) {

                            if (error_codes::SUCCESS != complete_multipart_upload()) {
                                return_value = false;
                            }
                        }

                        return_value = true;

                    }

                } else if (this->use_cache_) {

                    // not last file to close and using cache - close cache stream
                    if (use_cache_) {
                        rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] closing cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());
                        cache_fstream_.close();
                    }
                }

                return rv;

            }); // end close lock

            if (result == additional_processing_enum::DO_FLUSH_CACHE_FILE) {

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] closing cache file\n",
                        __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());

                cache_fstream_.close();

                if (error_codes::SUCCESS != flush_cache_file(shm_obj)) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] flush_cache_file returned error\n",
                            __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());
                    this->set_error(ERROR(S3_PUT_ERROR, "flush_cache_file returned error"));
                    return_value = false;
                }
            }


            return return_value;
        }


        std::streamsize receive(char_type* _buffer,
                                std::streamsize _buffer_size) override
        {
            if (use_cache_) {
                auto position_before_read = cache_fstream_.tellg();
                cache_fstream_.read(_buffer, _buffer_size);
                return cache_fstream_.tellg() - position_before_read;
            }

            // Not using cache.
            // just get what is asked for
            std::streamsize length = s3_download_part_worker_routine(_buffer, _buffer_size);

            // if we are not using cache file, update the read/write pointer
            if (!use_cache_) {
                this->seekpos(static_cast<off_type>(length), std::ios_base::cur);
            }

            return length;
        }

        std::streamsize send(const char_type* _buffer,
                             std::streamsize _buffer_size) override
        {
            thread_local std::ofstream tmp;

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            if (use_cache_) {

                named_shared_memory_object shm_obj{shmem_key_,
                    config_.shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                return shm_obj.atomic_exec([this, _buffer, _buffer_size](auto& data) {

                    auto position_before_write = this->cache_fstream_.tellp();
                    this->cache_fstream_.write(_buffer, _buffer_size);
                    this->cache_fstream_.flush();

                    // calculate the new size of the file
                    auto current_position = this->cache_fstream_.tellp();

                    const auto msg = fmt::format("send() position={} size={} position_after_write=", position_before_write, _buffer_size, current_position);
                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), msg.c_str());

                    // return bytes written
                    std::streamsize bytes_written = current_position - position_before_write;
                    return bytes_written;
                });
            }

            // Not using cache.

            // if this is a multipart upload and we have not yet initiated it, do so
            bool return_value = true;
            shm_obj.atomic_exec([this, &shm_obj, &return_value](auto& data) {

                if ( this->use_streaming_multipart() && !data.done_initiate_multipart ) {

                    bool multipart_upload_success = this->begin_multipart_upload(shm_obj);
                    if (!multipart_upload_success) {
                        rodsLog(LOG_ERROR, "Initiate multipart failed.");
                        return_value = false;
                    } else {
                        data.done_initiate_multipart = true;
                    }
                }
            });

            // could not initiate multipart, return error
            if (return_value == false) {
                this->set_error(ERROR(S3_PUT_ERROR, "Initiate multipart failed."));
                return 0;
            }

            // if config_.bytes_this_thread is 0 then bail
            if (config_.number_of_client_transfer_threads > 1 && 0 == get_bytes_this_thread()) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] part size is zero\n", __FILE__, __LINE__,
                        __FUNCTION__, get_thread_identifier());
                this->set_error(ERROR(S3_PUT_ERROR, "Part size was set to zero"));
                return 0;
            }

            // if we haven't already started an upload thread, start it
            if (!begin_part_upload_thread_ptr_) {

                // use multipart if we have multiple client transfer threads or if the object size is > 2 * minimum part size
                if ( use_streaming_multipart() ) {
                    try {
                        begin_part_upload_thread_ptr_ = std::make_unique<std::thread>(
                                &s3_transport::s3_upload_part_worker_routine, this, false, 0, 0, get_file_offset());
                    } catch (const std::bad_alloc& ba) {
                        const auto error_msg = fmt::format("Allocation error when creating upload part thread. [{}]", ba.what());
                        this->set_error(ERROR(S3_PUT_ERROR, error_msg.c_str()));
                        return 0;
                    } catch (const std::system_error& se) {
                        const auto error_msg = fmt::format("System error when creating upload part thread. [{}]", se.what());
                        this->set_error(ERROR(S3_PUT_ERROR, error_msg.c_str()));
                        return 0;
                    }
                } else {
                    try {
                        begin_part_upload_thread_ptr_ = std::make_unique<std::thread>(
                            &s3_transport::s3_upload_file, this, false);
                    } catch (const std::bad_alloc& ba) {
                        const auto error_msg = fmt::format("Allocation error when creating upload file thread. [{}]", ba.what());
                        this->set_error(ERROR(S3_PUT_ERROR, error_msg.c_str()));
                        return 0;
                    } catch (const std::system_error& se) {
                        const auto error_msg = fmt::format("System error when creating upload filethread. [{}]", se.what());
                        this->set_error(ERROR(S3_PUT_ERROR, error_msg.c_str()));
                        return 0;
                    }
                }
            }

            // Push the current buffer onto the circular_buffer.  The push may be partial so keep
            // pushing until all bytes are pushed
            int64_t offset = 0;
            while (offset < _buffer_size) {

                try {
                    offset += circular_buffer_.push_back(&_buffer[offset], &_buffer[_buffer_size]);
                } catch (timeout_exception& e) {

                    // timeout trying to push onto circular buffer
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] "
                            "Unexpected timeout when pushing onto circular buffer.  "
                            "Thread writing to S3 may have died.  Returning 0 bytes processed.",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                    set_error(ERROR(S3_PUT_ERROR, "Unexpected timeout when pushing onto circular buffer."));
                    return 0;
                }

            }

            try {
                circular_buffer_.push_back(&_buffer[offset], &_buffer[_buffer_size]);
            } catch (timeout_exception& e) {

                // timeout trying to push onto circular buffer
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] "
                        "Unexpected timeout when pushing onto circular buffer.  "
                        "Thread writing to S3 may have died.  Returning 0 bytes processed.",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                set_error(ERROR(S3_PUT_ERROR, "Unexpected timeout when pushing onto circular buffer."));
                return 0;
            }

            return _buffer_size;

        }

        pos_type seekpos(off_type _offset,
                         std::ios_base::seekdir _dir) override
        {
            if (!is_open()) {
                return seek_error;
            }

            if (use_cache_) {
                // we are using a cache file so just seek on it,
                cache_fstream_.seekg(_offset, _dir);
                return cache_fstream_.tellg();

            } else {

                int64_t existing_object_size = config_.object_size;
                switch (_dir) {
                    case std::ios_base::beg:
                        set_file_offset(_offset);
                        break;

                    case std::ios_base::cur:
                        set_file_offset(get_file_offset() + _offset);
                        break;

                    case std::ios_base::end:

                        if (existing_object_size == config::UNKNOWN_OBJECT_SIZE) {
                            // do a stat to get object size
                            object_s3_status object_status = object_s3_status::DOES_NOT_EXIST;
                            irods::error ret = get_object_s3_status(object_key_, bucket_context_, existing_object_size, object_status);
                            if (!ret.ok() || object_status == object_s3_status::DOES_NOT_EXIST) {
                                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] seek failed because object size is unknown and HEAD failed",
                                         __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                                return seek_error;
                            }
                        }

                        set_file_offset(existing_object_size + _offset);
                        break;

                    default:
                        return seek_error;
                }
                return get_file_offset();
            }

        }

        bool is_open() const noexcept override
        {
            if (use_cache_) {
                return cache_fstream_ && cache_fstream_.is_open();
            } else {
                return fd_ >= minimum_valid_file_descriptor;
            }
        }

        int file_descriptor() const noexcept override
        {
            return fd_;
        }

        const root_resource_name& root_resource_name() const override
        {
            return root_resc_name_;
        }

        const leaf_resource_name& leaf_resource_name() const override
        {
            return leaf_resc_name_;
        }

        const replica_number& replica_number() const override
        {
            return replica_number_;
        }

        const replica_token& replica_token() const override
        {
            return replica_token_;
        }

        bool is_last_file_to_close() {
            return last_file_to_close_;
        }

        // used for unit testing
        bool get_use_cache() {
            return use_cache_;
        }

        void set_error(const irods::error& e) {
            std::lock_guard<std::mutex> lock(error_mutex_);
            error_ = e;
        }

        irods::error get_error() {
            std::lock_guard<std::mutex> lock(error_mutex_);
            return error_;
        }

        void set_bytes_this_thread(int64_t bytes_this_thread) {
            std::lock_guard<std::mutex> lock(bytes_this_thread_mutex_);
            config_.bytes_this_thread = bytes_this_thread;
        }

        auto get_bytes_this_thread() {
            std::lock_guard<std::mutex> lock(bytes_this_thread_mutex_);
            return config_.bytes_this_thread;
        }


        // this function uses the starting offset provided to the transport
        // and the number of bytes in this thread to determine the start and end
        // part number for this thread
        static void determine_start_and_end_part_from_offset_and_bytes_this_thread(
                int64_t bytes_this_thread,
                int64_t file_offset,
                int64_t circular_buffer_size,
                unsigned int& start_part_number,
                unsigned int& end_part_number,
                std::vector<int64_t>& part_sizes)  {

            assert(bytes_this_thread > 0);

            // determine thread number, if this is the last thread, the bytes_this_thread
            // may be larger so the thread number must be adjusted
            unsigned int thread_number = (file_offset / bytes_this_thread) +
                (file_offset % bytes_this_thread == 0 ? 0 : 1);

            // Determine the number of bytes for all threads used to determine out start part number.
            // At this point we don't care about the size of the last thread.
            int64_t bytes_all_threads_except_last =
                thread_number == 0
                ? bytes_this_thread
                : file_offset / thread_number;

            // Determine number of parts per thread.  If parts is not divisible by circular buffer
            // size then we need one additional part. The last thread is treated specially because
            // it may have additional bytes.
            unsigned int parts_per_thread = bytes_all_threads_except_last / circular_buffer_size
                + ( bytes_all_threads_except_last % circular_buffer_size == 0 ? 0 : 1 );

            start_part_number = thread_number * parts_per_thread + 1;
            if (bytes_this_thread == bytes_all_threads_except_last) {
                end_part_number = start_part_number + parts_per_thread - 1;
            } else {
                unsigned int parts_last_thread = bytes_this_thread / circular_buffer_size
                    + (bytes_this_thread % circular_buffer_size == 0 ? 0 : 1);
                end_part_number = start_part_number + parts_last_thread - 1;
            }

            // put the part sizes on the vector, splitting remaining bytes among first few parts
            int64_t part_size = bytes_this_thread / ( end_part_number - start_part_number + 1 );
            int64_t remaining_bytes = bytes_this_thread % ( end_part_number - start_part_number + 1 );
            [[maybe_unused]] int64_t total_bytes = 0;
            for (unsigned int part_cntr = 0; part_cntr <= end_part_number - start_part_number; ++part_cntr) {
                int64_t bytes_this_part = part_size +
                        ( remaining_bytes > part_cntr ? 1 : 0 );
                total_bytes += bytes_this_part;
                assert(bytes_this_part <= circular_buffer_size);
                part_sizes.push_back(bytes_this_part);
            }
            assert(total_bytes == bytes_this_thread);
        }

        int64_t get_existing_object_size() {
            return existing_object_size_;
        }

    private:

        void set_file_offset(int64_t file_offset) {
            std::lock_guard<std::mutex> lock(file_offset_mutex_);
            file_offset_ = file_offset;
        }

        auto get_file_offset() {
            std::lock_guard<std::mutex> lock(file_offset_mutex_);
            return file_offset_;
        }

        uint64_t get_thread_identifier() const {
            return std::hash<std::thread::id>{}(std::this_thread::get_id());
        }

        auto get_cache_file_size() -> int64_t
        {
            std::fstream fs(cache_file_path_);
            if (!fs || !fs.is_open()) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] could not open cache file to get size\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                return 0;
            }

            fs.seekp(0, std::ios_base::end);
            auto cache_file_size = fs.tellp();
            return static_cast<int64_t>(cache_file_size) < 0 ? 0 : static_cast<int64_t>(cache_file_size);
        }

        bool begin_multipart_upload(named_shared_memory_object& shm_obj)
        {
            auto last_error_code = shm_obj.atomic_exec([](auto& data) {
                    return data.last_error_code;
            });

            // first one in initiates the multipart (everyone has same shared_memory_lock)
            if (last_error_code == error_codes::SUCCESS) {

                // send initiate message to S3
                error_codes ret = shm_obj.atomic_exec([this](auto& data) {
                    return this->initiate_multipart_upload();
                });

                if (error_codes::SUCCESS != ret) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] open returning false [last_error_code=%d]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), ret);

                    // update the last error
                    shm_obj.atomic_exec([ret](auto& data) {
                        data.last_error_code = ret;
                    });

                    this->set_error(ERROR(S3_PUT_ERROR, "Initiate multipart failed"));
                    return false;
                }
            } else {
                if (error_codes::SUCCESS != last_error_code) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] open returning false [last_error_code=%d]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            last_error_code);
                    this->set_error(ERROR(S3_PUT_ERROR, "Initiate multipart failed"));
                    return false;
                }
            }

            return true;
        }

        cache_file_download_status download_object_to_cache(
                named_shared_memory_object& shm_obj,
                int64_t s3_object_size)
        {

            // shmem is already locked here

            namespace bf = boost::filesystem;

            bf::path cache_file =  bf::path(config_.cache_directory) / bf::path(object_key_ + "-cache");
            bf::path parent_path = cache_file.parent_path();
            try {
                boost::filesystem::create_directories(parent_path);
            } catch (boost::filesystem::filesystem_error& e) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Could not download file to cache.  %s\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), e.what());
                return cache_file_download_status::FAILED;
            }
            cache_file_path_ = cache_file.string();

            bool start_download = shm_obj.atomic_exec([](auto& data) {
                bool start_download = data.cache_file_download_progress ==
                    cache_file_download_status::NOT_STARTED ||
                    data.cache_file_download_progress == cache_file_download_status::FAILED;
                if (start_download) {
                    data.cache_file_download_progress = cache_file_download_status::STARTED;
                }
                return start_download;
            });

            // first thread/process will spawn multiple threads to download object to cache
            if (start_download) {

                // download the object to a cache file

                int64_t disk_space_available = bf::space(config_.cache_directory).available;

                if (s3_object_size > disk_space_available)
                {

                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Not enough disk space to download object to cache.\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());

                    return shm_obj.atomic_exec([](auto& data) {
                        return data.cache_file_download_progress = cache_file_download_status::FAILED;
                    });
                }

                int64_t bytes_downloaded = 0;
                std::mutex bytes_downloaded_mutex;

                // determine number of download threads.
                //  max = config_.number_of_cache_transfer_threads
                //  start at 1 and add one per 1M
                int64_t cutoff_per_thread = 1024*1024;
                int64_t number_of_cache_transfer_threads = s3_object_size / cutoff_per_thread + 1;
                number_of_cache_transfer_threads = number_of_cache_transfer_threads > config_.number_of_cache_transfer_threads ? config_.number_of_cache_transfer_threads : number_of_cache_transfer_threads;

                int64_t part_size = s3_object_size / number_of_cache_transfer_threads;

                irods::thread_pool threads{static_cast<int>(number_of_cache_transfer_threads)};

                for (unsigned int thr_id= 0; thr_id < number_of_cache_transfer_threads; ++thr_id) {

                    irods::thread_pool::post(threads, [this, thr_id, part_size, s3_object_size, number_of_cache_transfer_threads,
                            &bytes_downloaded, &bytes_downloaded_mutex] () {

                            off_t this_part_offset = part_size * thr_id;
                            int64_t this_part_size;

                            if (thr_id == number_of_cache_transfer_threads - 1) {
                                this_part_size = part_size + (s3_object_size -
                                        part_size * number_of_cache_transfer_threads);
                            } else {
                                this_part_size = part_size;
                            }

                            int64_t this_bytes_downloaded =
                                this->s3_download_part_worker_routine(nullptr, this_part_size, this_part_offset, true);

                            {
                                std::lock_guard<std::mutex> lock(bytes_downloaded_mutex);
                                bytes_downloaded += this_bytes_downloaded;
                            }
                    });
                }
                threads.join();

                if (bytes_downloaded != s3_object_size) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Failed downloading to cache - bytes_downloaded (%lu) != s3_object_size (%lu).\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), bytes_downloaded, s3_object_size);
                    fflush(stderr);
                    return shm_obj.atomic_exec([](auto& data) {
                        return data.cache_file_download_progress = cache_file_download_status::FAILED;
                    });
                }

                return shm_obj.atomic_exec([](auto& data) {


                    data.cache_file_download_progress = cache_file_download_status::SUCCESS;
                    return data.cache_file_download_progress;
                });

            }

            // check the download status and return
            return shm_obj.atomic_exec([](auto& data) { return data.cache_file_download_progress; });

        }

        error_codes flush_cache_file(named_shared_memory_object& shm_obj) {

            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] Flushing cache file.\n",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());

            error_codes return_value = error_codes::SUCCESS;

            namespace bf = boost::filesystem;

            // Flush the cache file to S3.

            bf::path cache_file =  bf::path(config_.cache_directory) / bf::path(object_key_ + "-cache");
            cache_file_path_ = cache_file.string();

            // calculate the part size
            std::ifstream ifs;
            ifs.open(cache_file_path_.c_str(), std::ios::out);
            if (!ifs || !ifs.is_open()) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Failed to open cache file.\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                return error_codes::UPLOAD_FILE_ERROR;
            }

            ifs.seekg(0, std::ios_base::end);
            int64_t cache_file_size = static_cast<int64_t>(ifs.tellg());
            ifs.close();

            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] cache_file_size is %ld\n",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_size);
            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] number_of_cache_transfer_threads is %d\n",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), config_.number_of_cache_transfer_threads);

            if (config_.number_of_cache_transfer_threads == 0) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] number_of_cache_transfer_threads set to an invalid value (0).\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                return error_codes::UPLOAD_FILE_ERROR;
            }

            if (config_.max_single_part_upload_size == 0) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] max_single_part_upload_size set to an invalid value (0).\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
                return error_codes::UPLOAD_FILE_ERROR;
            }

            // each part must be at least 5MB in size so adjust number_of_cache_transfer_threads accordingly
            int64_t minimum_part_size = config_.minimum_part_size;
            config_.number_of_cache_transfer_threads
                = minimum_part_size * config_.number_of_cache_transfer_threads < cache_file_size
                ? config_.number_of_cache_transfer_threads
                : cache_file_size / minimum_part_size == 0 ? 1 : cache_file_size / minimum_part_size;

            // Calculate the number of parts.  This is usually a one-to-one mapping to number of threads but if
            // parts per thread > config_.max_single_part_upload_size then need to break it into more parts
            unsigned int number_of_parts = config_.number_of_cache_transfer_threads;
            if (cache_file_size > number_of_parts * config_.max_single_part_upload_size) {
                number_of_parts
                  = cache_file_size % config_.max_single_part_upload_size == 0
                  ? cache_file_size / config_.max_single_part_upload_size
                  : cache_file_size / config_.max_single_part_upload_size + 1;
            }

            if (config_.multipart_enabled && number_of_parts > 1) {

                initiate_multipart_upload();

                irods::thread_pool cache_flush_threads{static_cast<int>(config_.number_of_cache_transfer_threads)};

                unsigned int part_number = 1;

                int64_t part_size_all_but_last_part = cache_file_size / number_of_parts;
                while (part_number <= number_of_parts) {

                    // run number_of_cache_transfer_threads simultaneously
                    for (unsigned int i = 0; i < config_.number_of_cache_transfer_threads; ++i) {

                        if (part_number > number_of_parts) {
                            break;
                        }

                        int64_t part_size = part_size_all_but_last_part;

                        // give extra bytes to last part
                        if (part_number == number_of_parts) {
                            part_size += cache_file_size % number_of_parts;
                        }

                        off_t offset = (part_number - 1) * part_size_all_but_last_part;

                        irods::thread_pool::post(cache_flush_threads, [this, part_number, part_size, offset] () {
                                    // upload part and read your part from cache file
                                    s3_upload_part_worker_routine(true, part_number, part_size, offset);
                        });

                        ++part_number;
                    }
                    cache_flush_threads.join();
                }

                return_value = complete_multipart_upload();

            } else {
                return_value = s3_upload_file(true);
            }

            // remove cache file
            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] removing cache file %s\n",
                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), cache_file_path_.c_str());
            std::remove(cache_file_path_.c_str());

            // set cache file download flag to NOT_STARTED
            // already locked so just exec()
            shm_obj.atomic_exec([](auto& data) {
                    data.cache_file_download_progress = cache_file_download_status::NOT_STARTED;
                    return data.cache_file_download_progress;
            });

            return return_value;

        }

        bool is_full_upload() {
            //return config_.put_repl_flag;
            using std::ios_base;
            const auto m = mode_ & ~(ios_base::ate | ios_base::binary);
            return (ios_base::out == m && config_.number_of_client_transfer_threads > 1) ||
                (ios_base::out | ios_base::trunc) == m;
            //return ( (O_CREAT | O_WRONLY | O_TRUNC) == open_flags );
        }

        // This populates the following flags based on the open mode (mode_).
        //   - use_cache_         - Always set to true in the following circumstances
        //                          * The object was not opened in read only mode and one of the following are true:
        //                            * the number of transfer threads > 1 and MPU is disabled
        //                            * the part sizes would be less than the minimum number allowed
        //                            * the put_repl_flag is false indicating no predictable full file write
        //                            * do not know the object size
        //                         - Otherwise set to false
        //
        //   - download_to_cache_ - Set to true unless one of the following is true:
        //                             * the object is opened in read only mode
        //                             * the trunc flag is set
        //   - object_must_exist_ - See table "Action if file does not exist" in the table at the
        //                          following link: https://en.cppreference.com/w/cpp/io/basic_filebuf/open
        void populate_open_mode_flags() noexcept
        {

            using std::ios_base;

            const auto m = mode_ & ~(ios_base::ate | ios_base::binary);

            // read only, do not use cache
            if (ios_base::in == m) {
                download_to_cache_ = false;
                use_cache_ = false;
                object_must_exist_ = true;
            }
            // put_repl_flag is a contract that says the full file will be written in a similar
            // manner as iput.
            else if (config_.put_repl_flag) {
                download_to_cache_ = false;
                use_cache_ = false;
                object_must_exist_ = false;

                // override for cases where we must use cache
                //   1. If we don't know the file size.
                //   2. If we don't know the # of threads.
                //   3. If we have > 1 thread and multipart is disabled
                //   4. If doing multipart upload file size < #threads * minimum part size
                if ( config_.object_size == 0 || config_.object_size == config::UNKNOWN_OBJECT_SIZE ||
                        ( config_.number_of_client_transfer_threads == 0 ) ||
                        ( config_.number_of_client_transfer_threads > 1 && !config_.multipart_enabled ) ||
                        ( config_.number_of_client_transfer_threads > 1 &&
                          config_.object_size < static_cast<int64_t>(config_.number_of_client_transfer_threads) *
                          static_cast<int64_t>(config_.minimum_part_size))) {
                    use_cache_ = true;
                }
            }
            // config_.put_repl_flag not set.  This means we may have random access.  Must
            // use cache.
            else {

                download_to_cache_ = true;
                use_cache_ = true;

                if (ios_base::out == m) {
                    object_must_exist_ = false;
                }
                else if ((ios_base::out | ios_base::trunc) == m) {
                    download_to_cache_ = false;
                    object_must_exist_ = false;
                }
                else if (ios_base::app == m || (ios_base::out | ios_base::app) == m) {
                    object_must_exist_ = false;
                }
                else if ((ios_base::out | ios_base::in) == m) {
                    object_must_exist_ = true;
                }
                else if ((ios_base::out | ios_base::in | ios_base::trunc) == m) {
                    download_to_cache_ = false;
                    object_must_exist_ = false;
                }
                else if ((ios_base::out | ios_base::in | ios_base::app) == m ||
                         (ios_base::in | ios_base::app) == m)
                {
                    object_must_exist_ = false;
                }

            }

        }  // end populate_open_mode_flags

        bool seek_to_end_if_required(std::ios_base::openmode _mode)
        {
            if (std::ios_base::ate & _mode) {
                if (seek_error == seekpos(0, std::ios_base::end)) {
                    this->set_error(ERROR(UNIX_FILE_LSEEK_ERR, "Failed to seek on S3 cache file"));
                    return false;
                }
            }

            return true;
        }  // end seek_to_end_if_required

        template <typename Function>
        bool open_impl(const filesystem::path& _p,
                       std::ios_base::openmode _mode,
                       Function _func)
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;
            namespace bf = boost::filesystem;

            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] [_mode & in = %d][_mode & out = %d]"
                "[_mode & trunc = %d][_mode & app = %d][_mode & ate = %d]"
                "[_mode & binary = %d]\n",
                __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                (_mode & std::ios_base::in) == std::ios_base::in,
                (_mode & std::ios_base::out) == std::ios_base::out,
                (_mode & std::ios_base::trunc) == std::ios_base::trunc,
                (_mode & std::ios_base::app) == std::ios_base::out,
                (_mode & std::ios_base::ate) == std::ios_base::ate,
                (_mode & std::ios_base::binary) == std::ios_base::binary);

            object_key_ = _p.string();
            shmem_key_ = constants::SHARED_MEMORY_KEY_PREFIX +
                std::to_string(std::hash<std::string>{}(config_.resource_name + "/" + object_key_));

            upload_manager_.object_key = object_key_;
            upload_manager_.shmem_key = shmem_key_;

            mode_ = _mode;

            populate_open_mode_flags();

            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] [object_key_ = %s][use_cache_ = %d]"
                "[download_to_cache_ = %d]\n",
                __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                object_key_.c_str(),
                use_cache_,
                download_to_cache_);

            // if using cache and mpu is disabled and object size > maximum part size, then fail as we can't process this file
            if (!config_.multipart_enabled && config_.object_size > config_.max_single_part_upload_size) {
                this->set_error(ERROR(UNIX_FILE_OPEN_ERR, "File can't be uploaded because MPU is disabled and "
                    "file size is greater than maximum part size"));
                return false;
            }


            // each process must intitialize S3
            {
                std::lock_guard<std::mutex> lock(s3_initialized_counter_mutex_);
                if (s3_initialized_counter_ == 0) {

                    int flags = S3_INIT_ALL;

                    int status = S3_initialize( "s3", flags, bucket_context_.hostName );
                    if (status != libs3_types::status_ok) {
                        rodsLog(LOG_ERROR, "S3_initialize returned error\n");
                        this->set_error(ERROR(S3_INIT_ERROR, "S3_initialize returned error"));
                        return false;
                    }
                }
                ++s3_initialized_counter_;
            }

            // only allow open/close to run one at a time for this object
            bool return_value = true;
            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            shm_obj.atomic_exec([this, &return_value, &shm_obj](auto& data) {

                object_s3_status object_status = object_s3_status::DOES_NOT_EXIST;

                int64_t s3_object_size = 0;

                data.file_open_counter += 1;
                if (this->config_.number_of_client_transfer_threads == 0) {
                    data.know_number_of_threads = false;
                }

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] open file_open_counter = %d\n",
                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), data.file_open_counter);

                if (object_must_exist_ || download_to_cache_) {

                    // do a HEAD to get the object size, if a previous thread has already done one then
                    // just read the object size from shmem
                    if (data.cache_file_download_progress == cache_file_download_status::SUCCESS) {
                        object_status = object_s3_status::IN_S3;
                    } else {
                        irods::error ret = get_object_s3_status(object_key_, bucket_context_, s3_object_size, object_status);
                        if (!ret.ok()) {
                            return_value = false;
                            this->set_error(ret);
                        }
                        data.existing_object_size = s3_object_size;
                    }

                    // save the size of the existing object as we may need it later
                    existing_object_size_ = data.existing_object_size;

                }

                // restore object from glacier if necessary
                if (object_must_exist_) {

                    irods::error ret = handle_glacier_status(object_key_, bucket_context_, config_.restoration_days, config_.restoration_tier, object_status);
                    if (!ret.ok()) {
                        this->set_error(ret);
                        return_value = false;
                        return;
                    }
                }

                if (object_status == object_s3_status::IN_S3 && this->download_to_cache_) {

                    cache_file_download_status download_status = this->download_object_to_cache(shm_obj, s3_object_size);

                    if (cache_file_download_status::SUCCESS != download_status) {
                            rodsLog(LOG_ERROR, "failed to download file to cache, download_status =%d\n", download_status);
                        return_value = false;
                        return;
                    }
                }

                if (this->use_cache_) {

                    // using cache, open the cache file for subsequent reads/writes
                    // use the mode that was passed in

                    if (!this->cache_fstream_ || !this->cache_fstream_.is_open()) {

                        bf::path cache_file =  bf::path(this->config_.cache_directory) / bf::path(object_key_ + "-cache");
                        bf::path parent_path = cache_file.parent_path();

                        try {
                            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] Creating parent_path  %s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), parent_path.string().c_str());
                            boost::filesystem::create_directories(parent_path);
                        } catch (boost::filesystem::filesystem_error& e) {
                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Could not create parent directories for cache file.  %s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), e.what());
                            return_value = false;
                            return;
                        }

                        cache_file_path_ = cache_file.string();

                        // first open use open mode given to s3_transport for others
                        // turn off trunc flag
                        std::ios_base::openmode mode;
                        bool trunc_flag = false;
                        if (data.threads_remaining_to_close == 0) {
                            trunc_flag = true;
                            mode = mode_;
                        } else {
                            mode = mode_ & ~std::ios_base::trunc;
                        }

                        // try opening for read and write, if it fails create then open for read/write
                        cache_fstream_.open(cache_file_path_.c_str(), mode | std::ios_base::in | std::ios_base::out);
                        if (!cache_fstream_.is_open()) {
                            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] opened cache file %s with create [trunc_flag=%d]\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_path_.c_str(), trunc_flag);
                            // file may not exist, open with std::ios_base::out to create then with in/out
                            cache_fstream_.open(cache_file_path_.c_str(), std::ios_base::out);
                            cache_fstream_.close();
                            cache_fstream_.open(cache_file_path_.c_str(), mode | std::ios_base::in | std::ios_base::out);
                        } else {
                            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] opened cache file %s [trunc_flag=%d]\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_path_.c_str(), trunc_flag);
                        }

                        if (!cache_fstream_ || !cache_fstream_.is_open()) {
                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Failed to open cache file %s, error=%s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), cache_file_path_.c_str(), strerror(errno));
                            this->set_error(ERROR(UNIX_FILE_OPEN_ERR, "Failed to open S3 cache file"));
                            return_value = false;
                            return;
                        }

                        if (!this->seek_to_end_if_required(this->mode_)) {
                            this->set_error(ERROR(UNIX_FILE_LSEEK_ERR, "Failed to seek on cache file"));
                            return_value = false;
                            return;
                        }
                    }

                } else {

                    // not using cache, just create our own fd

                    const auto fd = this->file_descriptor_counter_++;

                    if (fd < minimum_valid_file_descriptor) {
                        this->set_error(ERROR(SYS_FILE_DESC_OUT_OF_RANGE, "S3 file descriptor was out of range"));
                        return_value = false;
                        return;
                    }

                    this->fd_ = fd;

                    if (!this->seek_to_end_if_required(mode_)) {
                        this->set_error(ERROR(UNIX_FILE_LSEEK_ERR, "Failed to seek in s3_transport"));
                        return_value = false;
                        return;
                    }
                }

                // set the number of threads to close to the number of client transfer threads
                // this will count down as closes happen
                if (data.threads_remaining_to_close == 0) {
                    data.threads_remaining_to_close = this->config_.number_of_client_transfer_threads;
                }


            });  // end atomic exec

            return return_value;

        }  // end open_impl

        error_codes initiate_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            unsigned int retry_cnt    = 0;

            S3PutProperties put_props{};
            put_props.useServerSideEncryption = config_.server_encrypt_flag;

            put_props.md5 = nullptr;
            put_props.expires = -1;

            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;
            upload_manager_.xml = "";

            data_for_write_callback data{bucket_context_, circular_buffer_};
            data.thread_identifier = get_thread_identifier();

            // read shared memory entry for this key

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            return shm_obj.atomic_exec([this, &put_props, &retry_cnt](auto& data) {

                int retry_wait_seconds = this->config_.retry_wait_seconds;

                retry_cnt = 0;

                // These expect a upload_manager* as cbdata
                S3MultipartInitialHandler mpu_initial_handler
                    = { { s3_multipart_upload::initialization_callback::on_response_properties,
                          s3_multipart_upload::initialization_callback::on_response_complete },
                        s3_multipart_upload::initialization_callback::on_response };

                do {
                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] call S3_initiate_multipart [object_key=%s]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), object_key_.c_str());

                    S3_initiate_multipart(&bucket_context_, object_key_.c_str(),
                            &put_props, &mpu_initial_handler, nullptr, 0, &upload_manager_);

                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] done call S3_initiate_multipart [object_key=%s]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), object_key_.c_str());

                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] [manager.status=%s]\n", __FILE__, __LINE__,
                            __FUNCTION__, get_thread_identifier(), S3_get_status_name(upload_manager_.status));

                    if (upload_manager_.status != libs3_types::status_ok) {
                        s3_sleep( retry_wait_seconds );
                        retry_wait_seconds *= 2;
                        if (retry_wait_seconds > config_.max_retry_wait_seconds) {
                            retry_wait_seconds = config_.max_retry_wait_seconds;
                        }
                    }

                } while ( (upload_manager_.status != libs3_types::status_ok)
                        && irods::experimental::io::s3_transport::S3_status_is_retryable(upload_manager_.status)
                        && ( ++retry_cnt <= config_.retry_count_limit));

                if ("" == data.upload_id || upload_manager_.status != libs3_types::status_ok) {
                    return error_codes::INITIATE_MULTIPART_UPLOAD_ERROR;
                }

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] S3_initiate_multipart returned.  Upload ID = %s\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        data.upload_id.c_str());

                upload_manager_.remaining = 0;
                upload_manager_.offset  = 0;

                return error_codes::SUCCESS;

            });

        } // end initiate_multipart_upload

        void mpu_cancel()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            // read shared memory entry for this key

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // read upload_id from shared_memory
            std::string upload_id = shm_obj.atomic_exec([](auto& data) {
                return data.upload_id.c_str();
            });

            S3AbortMultipartUploadHandler abort_handler
                = { { s3_multipart_upload::cancel_callback::on_response_properties,
                      s3_multipart_upload::cancel_callback::on_response_completion } };

            libs3_types::status status;

            const auto msg = fmt::format("Cancelling multipart upload: key=\"{}\", upload_id=\"{}\"", object_key_, upload_id);
            rodsLog(config_.developer_messages_log_level,  "%s\n", msg.c_str() );

            s3_multipart_upload::cancel_callback::g_response_completion_status = libs3_types::status_ok;
            s3_multipart_upload::cancel_callback::g_response_completion_saved_bucket_context = &bucket_context_;
            S3_abort_multipart_upload(&bucket_context_, object_key_.c_str(),
                    upload_id.c_str(), 0, &abort_handler);
            status = s3_multipart_upload::cancel_callback::g_response_completion_status;
            if (status != libs3_types::status_ok) {
                auto msg = fmt::format("{} - Error cancelling the multipart upload of S3 object: \"{}\"",
                        __FUNCTION__,
                        object_key_);
                if (status >= 0) {
                    msg += fmt::format(" - \"{}\"", S3_get_status_name(status));
                }
                rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.c_str() );
            }
        } // end mpu_cancel


        error_codes complete_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            error_codes result = shm_obj.atomic_exec([this](auto& data) {

                int retry_wait_seconds = this->config_.retry_wait_seconds;

                std::string upload_id  = data.upload_id.c_str();

                if ("" == upload_id) {
                    this->set_error(ERROR(S3_PUT_ERROR, "null upload_id in complete_multipart_upload"));
                    return error_codes::COMPLETE_MULTIPART_UPLOAD_ERROR;
                }

                if (error_codes::SUCCESS == data.last_error_code) { // If someone aborted, don't complete...

                    const auto msg = fmt::format("Multipart:  Completing key \"{}\" Upload ID \"{}\"", object_key_, upload_id);
                    rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            msg.c_str() );

                    uint64_t i;
                    auto xml = fmt::format("<CompleteMultipartUpload>\n");
                    for ( i = 0; i < data.etags.size() && !data.etags[i].empty(); i++ ) {
                        xml += fmt::format("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>\n", i + 1, data.etags[i]);
                    }
                    xml += fmt::format("</CompleteMultipartUpload>\n");

                    rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] [key=%s] Request: %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            object_key_.c_str(), xml.c_str() );

                    int manager_remaining = xml.size();
                    upload_manager_.offset = 0;
                    unsigned int retry_cnt = 0;
                    S3MultipartCommitHandler commit_handler
                        = { {s3_multipart_upload::commit_callback::on_response_properties,
                             s3_multipart_upload::commit_callback::on_response_completion },
                            s3_multipart_upload::commit_callback::on_response, nullptr };
                    do {
                        // On partial error, need to restart XML send from the beginning
                        upload_manager_.remaining = manager_remaining;
                        upload_manager_.xml = xml.c_str();

                        upload_manager_.offset = 0;
                        S3_complete_multipart_upload(&bucket_context_,
                                object_key_.c_str(),
                                &commit_handler,
                                upload_id.c_str(),
                                upload_manager_.remaining,
                                nullptr,
                                config_.non_data_transfer_timeout_seconds * 1000,   // timeout (ms)
                                &upload_manager_);

                        rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] [key=%s][manager.status=%s]\n", __FILE__, __LINE__,
                                __FUNCTION__, get_thread_identifier(), object_key_.c_str(), S3_get_status_name(upload_manager_.status));

                        retry_cnt++;

                        // Treating a timeout as a success here and below because under load we sometimes get a timeout
                        // but the multipart completes later.  A head/stat will detect this later.
                        if (upload_manager_.status != libs3_types::status_ok &&
                                upload_manager_.status != libs3_types::status_request_timeout &&
                                retry_cnt <= config_.retry_count_limit) {

                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] S3_complete_multipart_upload returned error [status=%s][object_key=%s][attempt=%d][retry_count_limit=%d].  Sleeping for %d seconds\n",
                                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                                    S3_get_status_name(upload_manager_.status), object_key_.c_str(), retry_cnt, config_.retry_count_limit, retry_wait_seconds);
                            s3_sleep( retry_wait_seconds );
                            retry_wait_seconds *= 2;
                            if (retry_wait_seconds > config_.max_retry_wait_seconds) {
                                retry_wait_seconds = config_.max_retry_wait_seconds;
                            }
                        }

                    } while (upload_manager_.status != libs3_types::status_ok &&
                            upload_manager_.status != libs3_types::status_request_timeout  &&
                            irods::experimental::io::s3_transport::S3_status_is_retryable(upload_manager_.status) &&
                            ( retry_cnt <= config_.retry_count_limit ));

                    if (upload_manager_.status != libs3_types::status_ok && upload_manager_.status != libs3_types::status_request_timeout) {
                        auto msg  = fmt::format("{}  - Error putting the S3 object: \"{}\"",
                                __FUNCTION__,
                                object_key_);
                        if(upload_manager_.status >= 0) {
                            msg += fmt::format(" - \"{}\"", S3_get_status_name( upload_manager_.status ));
                        }
                        this->set_error(ERROR(S3_PUT_ERROR, msg.c_str()));
                        return error_codes::COMPLETE_MULTIPART_UPLOAD_ERROR;
                    }
                }

                if (error_codes::SUCCESS != data.last_error_code && "" != data.upload_id ) {

                    // Someone aborted after we started, delete the partial object on S3
                    rodsLog(config_.developer_messages_log_level, "Cancelling multipart upload\n");
                    mpu_cancel();

                    // Return the error
                    return data.last_error_code;
                }

                return error_codes::SUCCESS;

            });

            return result;
        } // end complete_multipart_upload


        // download the part from the S3 object
        //   input:
        //     buffer               - If not null the downloaded part is written to buffer.  Buffer must have
        //                            length reserved.  If buffer is null the download is written to the cache file.
        //     length               - The length to be downloaded.
        //     offset               - If provided this is the offset of the object that is being downloaded.  If not
        //                            provided the current offset (file_offset_) is used.
        //     shmem_already_locked - If provided and true then no locking is done in shmem.
        //                            The default is false (with shmem locking).
        //
        //         Note:  The mutex is recursive but when reading from cache this is called by newly created
        //                threads and thus we need the flag if shmem is already locked.
        //
        std::streamsize s3_download_part_worker_routine(char_type *buffer,
                int64_t length,
                off_t offset = -1,
                bool shmem_already_locked = false)
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            unsigned int retry_cnt = 0;

            if (0 > offset) {
                offset = get_file_offset();
            }

            std::shared_ptr<callback_for_read_from_s3_base> read_callback;

            S3GetObjectHandler get_object_handler = {
                {
                    callback_for_read_from_s3_base::on_response_properties,
                    callback_for_read_from_s3_base::on_response_completion
                },
                callback_for_read_from_s3_base::invoke_callback
            };

            if (buffer == nullptr) {
                // Download to cache
                read_callback.reset(new callback_for_read_from_s3_to_cache
                        (bucket_context_));
                static_cast<callback_for_read_from_s3_to_cache*>
                    (read_callback.get())->set_and_open_cache_file(cache_file_path_);
            } else {
                // Download to buffer

                // test if beyond file
                if (existing_object_size_ != config_.UNKNOWN_OBJECT_SIZE) {
                    if (offset < 0 || static_cast<int64_t>(offset) >= existing_object_size_) {
                        return 0;
                    }

                    if (static_cast<int64_t>(offset + length) > existing_object_size_) {
                        length = existing_object_size_ - offset;
                    }

                    if (length == 0) {
                        return 0;
                    }
                }

                read_callback.reset(new callback_for_read_from_s3_to_buffer(bucket_context_));
                static_cast<callback_for_read_from_s3_to_buffer*>(read_callback.get())
                    ->set_output_buffer(buffer);
                static_cast<callback_for_read_from_s3_to_buffer*>(read_callback.get())
                    ->set_output_buffer_size(length);

            }
            read_callback->content_length = length;
            read_callback->thread_identifier = get_thread_identifier();
            read_callback->shmem_key = shmem_key_;
            read_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;

            int retry_wait_seconds = config_.retry_wait_seconds;

            do {

                // if we are reading into cache, write to cache file at offset
                // if reading into buffer, write at beginning of buffer
                if (buffer == nullptr) {
                    read_callback->offset = offset;
                    read_callback->bytes_read_from_s3 = 0;
                } else {
                    read_callback->offset = 0;
                    read_callback->bytes_read_from_s3 = 0;
                }

                auto msg = fmt::format("Multirange:  Start range key \"{}\", offset {}, len {}",
                        object_key_,
                        static_cast<int64_t>(offset),
                        static_cast<int>(read_callback->content_length));
                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.c_str());

                uint64_t start_microseconds = get_time_in_microseconds();

                S3_get_object( &bucket_context_, object_key_.c_str(), NULL,
                        offset, read_callback->content_length, 0, 0,
                        &get_object_handler, read_callback.get() );

                uint64_t end_microseconds = get_time_in_microseconds();
                double bw = (read_callback->content_length / (1024.0*1024.0)) /
                    ( (end_microseconds - start_microseconds) / 1000000.0 );
                msg = fmt::format(" -- END -- BW={} MB/s", bw);
                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        get_thread_identifier(), msg.c_str());

                if (read_callback->status != libs3_types::status_ok) {
                    s3_sleep( retry_wait_seconds );
                    retry_wait_seconds *= 2;
                    if (retry_wait_seconds > config_.max_retry_wait_seconds) {
                        retry_wait_seconds = config_.max_retry_wait_seconds;
                    }
                }

            } while ((read_callback->status != libs3_types::status_ok)
                    && irods::experimental::io::s3_transport::S3_status_is_retryable(read_callback->status)
                    && (++retry_cnt <= config_.retry_count_limit));

            if (read_callback->status != libs3_types::status_ok) {
                auto msg = fmt::format(" - Error getting the S3 object: \"{}\"", object_key_);
                if (read_callback->status >= 0) {
                    msg += fmt::format(" - \"{}\"", S3_get_status_name( read_callback->status));
                }
                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        get_thread_identifier(), msg.c_str());

                this->set_error(ERROR(S3_GET_ERROR, msg.c_str()));

                // update the last error in shmem

                named_shared_memory_object shm_obj{shmem_key_,
                    config_.shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                if (shmem_already_locked) {
                    shm_obj.atomic_exec([](auto& data) {
                        data.last_error_code = error_codes::DOWNLOAD_FILE_ERROR;
                    });
                } else {
                    shm_obj.atomic_exec([](auto& data) {
                        data.last_error_code = error_codes::DOWNLOAD_FILE_ERROR;
                    });
                }

            }
            return static_cast<std::streamsize>(read_callback->bytes_read_from_s3);

        } // end s3_download_part_worker_routine

        void s3_upload_part_worker_routine(bool read_from_cache = false,
                                           unsigned int part_number = 1,       // one based part number for cache only
                                           int64_t bytes_this_thread = 0,      // set for cache only
                                           off_t file_offset = 0
                                           )
        {

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            std::shared_ptr<s3_multipart_upload::callback_for_write_to_s3_base<CharT>> write_callback;

            // read upload_id from shmem

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // if not using cache, the bytes_this_thread is set up by the s3_transport
            if (!use_cache_) {
                bytes_this_thread = get_bytes_this_thread();
            }

            std::string upload_id;
            bool error =  shm_obj.atomic_exec([this, bytes_this_thread, &upload_id](auto& data) {
                upload_id = data.upload_id.c_str();
                if (upload_id == "") {
                    this->set_error(ERROR(S3_PUT_ERROR, "Upload id was null."));
                    data.last_error_code = error_codes::UPLOAD_FILE_ERROR;
                    return true;
                }
                if (bytes_this_thread == 0) {
                    this->set_error(ERROR(S3_PUT_ERROR, "bytes per thread was set to zero."));
                    data.last_error_code = error_codes::UPLOAD_FILE_ERROR;
                    return true;
                }
                if (!this->use_cache_ && this->config_.circular_buffer_size == 0) {
                    this->set_error(ERROR(S3_PUT_ERROR, "circular_buffer_size was set to zero."));
                    data.last_error_code = error_codes::UPLOAD_FILE_ERROR;
                    return true;
                }
                return false;
            });

            if (error) {
                return;
            }

            unsigned int retry_cnt = 0;

            S3PutObjectHandler put_object_handler = {
                {
                    s3_multipart_upload::callback_for_write_to_s3_base<CharT>::on_response_properties,
                    s3_multipart_upload::callback_for_write_to_s3_base<CharT>::on_response_completion
                },
                s3_multipart_upload::callback_for_write_to_s3_base<CharT>::invoke_callback
            };

            unsigned int start_part_number;
            unsigned int end_part_number;
            int64_t content_length;
            std::vector<int64_t> part_sizes;

            // resize the etags vector if necessary
            int resize_error = shm_obj.atomic_exec([this, &shm_obj](auto& data) {

                if (constants::MAXIMUM_NUMBER_ETAGS_PER_UPLOAD > data.etags.size()) {

                    rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] resize etags vector from %lu to %u\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), data.etags.size(), constants::MAXIMUM_NUMBER_ETAGS_PER_UPLOAD);

                    try {
                        data.etags.resize(constants::MAXIMUM_NUMBER_ETAGS_PER_UPLOAD, types::shm_char_string("", shm_obj.get_allocator()));
                    } catch (boost::interprocess::bad_alloc &biba) {
                        this->set_error(ERROR(S3_PUT_ERROR, "Error on reallocation of etags buffer in shared memory."));
                        data.last_error_code = error_codes::BAD_ALLOC;
                        return true;
                    }

                }

                return false;

            });

            if (resize_error) {
                rodsLog(LOG_ERROR, "Error on reallocation of etags buffer in shared memory.");
                this->set_error(ERROR(S3_PUT_ERROR, "Error on reallocation of etags buffer in shared memory."));
                return;
            }

            if (read_from_cache) {

                // read from cache, write to s3

                write_callback.reset(new s3_multipart_upload::callback_for_write_from_cache_to_s3<CharT>
                        (bucket_context_, upload_manager_));

                s3_multipart_upload::callback_for_write_from_cache_to_s3<CharT>
                    *write_callback_from_cache =
                    static_cast<s3_multipart_upload::callback_for_write_from_cache_to_s3<CharT>*>
                    (write_callback.get());

                write_callback_from_cache->set_and_open_cache_file(cache_file_path_);

                content_length = bytes_this_thread;
                start_part_number = end_part_number = part_number;

            } else {

                // Read from buffer, write to s3

                write_callback.reset(new
                        s3_multipart_upload::callback_for_write_from_buffer_to_s3<CharT>(
                            bucket_context_, upload_manager_, circular_buffer_));

                // determine the part number from the offset, file size, and buffer size
                // the last page might be larger so doing a little trick to handle that case (second term)
                //  Note:  We bailed early if bytes_this_thread == 0
                determine_start_and_end_part_from_offset_and_bytes_this_thread(bytes_this_thread, file_offset_,
                        config_.circular_buffer_size, start_part_number, end_part_number, part_sizes);

            }

            write_callback->enable_md5 = config_.enable_md5_flag;
            write_callback->thread_identifier = get_thread_identifier();
            write_callback->object_key = object_key_;
            write_callback->shmem_key = shmem_key_;
            write_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;
            write_callback->transport_object_ptr = this;

            int retry_wait_seconds = config_.retry_wait_seconds;

            bool circular_buffer_read_timeout = false;

            for (unsigned int part_number = start_part_number; part_number <= end_part_number; ++part_number) {

                retry_cnt = 0;

                do {

                    if (read_from_cache) {
                        write_callback->offset = file_offset;
                        write_callback->content_length = content_length;
                    } else {
                        write_callback->content_length = part_sizes[part_number - start_part_number];
                    }

                    write_callback->sequence = part_number;

                    auto msg = fmt::format("Multipart:  Start part {}, key \"{}\", uploadid \"{}\", len {}",
                            static_cast<int>(part_number),
                            object_key_,
                            upload_id,
                            static_cast<int>(write_callback->content_length));

                    rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            msg.c_str() );

                    S3PutProperties put_props{};
                    put_props.md5 = nullptr;
                    put_props.expires = -1;

                    // server encrypt flag not valid for part upload
                    put_props.useServerSideEncryption = false;

                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] S3_upload_part (ctx, %s, props, handler, %u, "
                           "uploadId, %lu, 0, partData) bytes_this_thread=%lld\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                           object_key_.c_str(), part_number,
                           write_callback->content_length, (int64_t)bytes_this_thread);

                    S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props,
                            &put_object_handler, part_number, upload_id.c_str(),
                            write_callback->content_length, 0, 120000, write_callback.get());

                    // zero out bytes_written in case of failure and re-run
                    write_callback->bytes_written = 0;

                    rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] S3_upload_part returned [part=%u][status=%s].\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), part_number,
                            S3_get_status_name(write_callback->status));

                    msg = fmt::format("Multipart:  -- END --");
                    rodsLog(config_.developer_messages_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            msg.c_str() );

                    retry_cnt += 1;
                    if (write_callback->status != libs3_types::status_ok && retry_cnt <= config_.retry_count_limit) {

                        // Check for a timeout reading from circular buffer.  If we got one then bypass retries.
                        circular_buffer_read_timeout =  shm_obj.atomic_exec([](auto& data) {
                            return data.circular_buffer_read_timeout;
                        });

                        // break out of do/while if we timed out reading from circular buffer
                        if (circular_buffer_read_timeout) {
                            break;
                        } else {

                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] S3_upload_part returned error [status=%s][attempt=%d][retry_count_limit=%d].  Sleeping between %d and %d seconds\n",
                                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), S3_get_status_name(write_callback->status), retry_cnt, config_.retry_count_limit,
                                    retry_wait_seconds >> 1, retry_wait_seconds);
                            s3_sleep( retry_wait_seconds );
                            retry_wait_seconds *= 2;
                            if (retry_wait_seconds > config_.max_retry_wait_seconds) {
                                retry_wait_seconds = config_.max_retry_wait_seconds;
                            }

                        }
                    }

                } while ((write_callback->status != libs3_types::status_ok)
                        && irods::experimental::io::s3_transport::S3_status_is_retryable(write_callback->status)
                        && (retry_cnt <= config_.retry_count_limit));

                if (write_callback->status != libs3_types::status_ok) {

                    this->set_error(ERROR(S3_PUT_ERROR, "failed in S3_upload_part"));

                    shm_obj.atomic_exec([](auto& data) {
                        data.last_error_code = error_codes::UPLOAD_FILE_ERROR;
                    });
                }
                write_callback->bytes_written = 0;

                // break out of for loop if we timed out reading from circular buffer
                if (circular_buffer_read_timeout) {
                    break;
                }

            } // for

            rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] Breaking out of circular_buffer_read loop.  End part number = %d",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), end_part_number);
        }

        error_codes s3_upload_file(bool read_from_cache = false)
        {

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            std::shared_ptr<s3_upload::callback_for_write_to_s3_base<CharT>> write_callback;

            unsigned int retry_cnt = 0;

            int retry_wait_seconds = config_.retry_wait_seconds;
            bool circular_buffer_read_timeout = false;

            do {

                S3PutObjectHandler put_object_handler = {
                    {
                        s3_upload::callback_for_write_to_s3_base<CharT>::on_response_properties,
                        s3_upload::callback_for_write_to_s3_base<CharT>::on_response_completion
                    },
                    s3_upload::callback_for_write_to_s3_base<CharT>::invoke_callback
                };

                if (read_from_cache) {

                    // read from cache

                    write_callback.reset(new s3_upload::callback_for_write_from_cache_to_s3<CharT>
                            (bucket_context_, upload_manager_));

                    s3_upload::callback_for_write_from_cache_to_s3<CharT>
                        *write_callback_from_cache =
                        static_cast<s3_upload::callback_for_write_from_cache_to_s3<CharT>*>
                        (write_callback.get());

                    write_callback_from_cache->set_and_open_cache_file(cache_file_path_);

                    write_callback->content_length = get_cache_file_size();
                    write_callback->offset = 0;


                } else {

                    // Read from buffer

                    write_callback.reset(new
                            s3_upload::callback_for_write_from_buffer_to_s3<CharT>(
                                bucket_context_, upload_manager_, circular_buffer_));

                    write_callback->content_length = config_.object_size;

                }

                write_callback->enable_md5 = config_.enable_md5_flag;
                write_callback->thread_identifier = get_thread_identifier();
                write_callback->object_key = object_key_;
                write_callback->shmem_key = shmem_key_;
                write_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;
                write_callback->transport_object_ptr = this;

                S3PutProperties put_props{};
                put_props.md5 = nullptr;
                put_props.expires = -1;
                put_props.useServerSideEncryption = config_.server_encrypt_flag;

                // zero out bytes_written in case of failure and re-run
                write_callback->bytes_written = 0;

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] S3_put_object(ctx, %s, "
                       "%lu, put_props, 0, &putObjectHandler, &data)\n",
                       __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                       object_key_.c_str(),
                       write_callback->content_length);

                S3_put_object(&bucket_context_, object_key_.c_str(), write_callback->content_length,
                        &put_props, 0, 0, &put_object_handler, write_callback.get());

                rodsLog(config_.developer_messages_log_level, "%s:%d (%s) [[%lu]] S3_put_object returned [status=%s].\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        S3_get_status_name(write_callback->status));

                if (write_callback->status != libs3_types::status_ok) {

                    // Check for a timeout reading from circular buffer.  If we got one then bypass retries.
                    named_shared_memory_object shm_obj{shmem_key_,
                        config_.shared_memory_timeout_in_seconds,
                        constants::MAX_S3_SHMEM_SIZE};

                    circular_buffer_read_timeout =  shm_obj.atomic_exec([](auto& data) {
                        return data.circular_buffer_read_timeout;
                    });

                    // break out of do/while if we timed out reading from circular buffer
                    if (circular_buffer_read_timeout) {
                        break;
                    } else {

                        s3_sleep( retry_wait_seconds );
                        retry_wait_seconds *= 2;
                        if (retry_wait_seconds > config_.max_retry_wait_seconds) {
                            retry_wait_seconds = config_.max_retry_wait_seconds;
                        }
                    }
                }

            } while ((write_callback->status != libs3_types::status_ok)
                    && irods::experimental::io::s3_transport::S3_status_is_retryable(write_callback->status)
                    && (++retry_cnt <= config_.retry_count_limit));

            if (write_callback->status != libs3_types::status_ok) {
                this->set_error(ERROR(S3_PUT_ERROR, "failed in S3_put_object"));
                return error_codes::UPLOAD_FILE_ERROR;
            }


            return error_codes::SUCCESS;

        } // end s3_upload_file

        bool use_streaming_multipart() {

            assert(config_.circular_buffer_size / 2 <= std::numeric_limits<uint64_t>::max());

            return !(use_cache_)
                && is_full_upload()
                && ( config_.number_of_client_transfer_threads > 1 || config_.object_size > static_cast<int64_t>(config_.circular_buffer_size) );

        }

        struct root_resource_name root_resc_name_;
        struct leaf_resource_name leaf_resc_name_;
        struct replica_number replica_number_;
        struct replica_token replica_token_;

        config                       config_;
        int                          fd_;
        nlohmann::json               fd_info_;

        bool                         call_s3_upload_part_flag_;
        bool                         call_s3_download_part_flag_;

        std::unique_ptr<std::thread> begin_part_upload_thread_ptr_;

        irods::experimental::circular_buffer<char_type>
                                     circular_buffer_;

        std::ios_base::openmode      mode_;

        inline static std::mutex     file_offset_mutex_;
        off_t                        file_offset_;
        int64_t                      existing_object_size_;


        // operational modes based on input flags
        bool                         download_to_cache_;
        bool                         use_cache_;
        bool                         object_must_exist_;

        libs3_types::bucket_context  bucket_context_;
        upload_manager               upload_manager_;

        std::string                  object_key_;
        std::string                  shmem_key_;

        std::string                  cache_file_path_;
        std::fstream                 cache_fstream_;

        inline static int            file_descriptor_counter_ = minimum_valid_file_descriptor;

        // This counter keeps track of whether this process has initialized
        // S3 and how many threads in the process are active.  The first thread (counter=0)
        // intitializes S3 and the last thread (counter decremented to 0) deinitializes.
        // If we are in a multithreaded / single process environment the
        // initialization will run only once.  If we are in a multiprocess
        // environment it will run multiple times.
        inline static int            s3_initialized_counter_ = 0;
        inline static std::mutex     s3_initialized_counter_mutex_;

        inline static std::mutex     region_name_mutex_;
        inline static std::mutex     bytes_this_thread_mutex_;

        // this is set to true when the last file closes
        bool                         last_file_to_close_;

        // when an error occurs this is set to something other than SUCCESS()
        inline static std::mutex     error_mutex_;
        irods::error                 error_;


    }; // s3_transport

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

