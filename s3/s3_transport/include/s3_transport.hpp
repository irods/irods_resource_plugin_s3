#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "circular_buffer.hpp"

// iRODS includes
#include <transport/transport.hpp>
#include <thread_pool.hpp>
#include <rcMisc.h>

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
#include "s3_transport_types.hpp"
#include "s3_transport_util.hpp"
#include "s3_transport_callbacks.hpp"

namespace irods::experimental::io::s3_transport
{

    struct config
    {

        config()
            : object_size{UNKNOWN_OBJECT_SIZE}
            , number_of_cache_transfer_threads{1}  // this is the number of transfer threads when transferring from cache
            , number_of_client_transfer_threads{1}  // this is the number of transfer threads defined by iRODS for PUTs, GETs
            , part_size{1000}
            , retry_count_limit{3}
            , retry_wait_seconds{3}
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
            , circular_buffer_size{10}
            , s3_uri_request_style{""}
            , minimum_part_size{DEFAULT_MINIMUM_PART_SIZE}
            , put_repl_flag{false}

        {}

        int64_t      object_size;
        unsigned int number_of_cache_transfer_threads; // only used when doing full file upload/download via cache
        unsigned int number_of_client_transfer_threads; // controlled by iRODS
        int64_t      part_size;                        // only used when doing a multipart upload
        unsigned int retry_count_limit;
        int          retry_wait_seconds;
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
        unsigned int circular_buffer_size;
        std::string  s3_uri_request_style;
        int64_t      minimum_part_size;
        static const int64_t UNKNOWN_OBJECT_SIZE = -1;
        static const int64_t DEFAULT_MINIMUM_PART_SIZE = 5*1024*1024;
        int          debug_log_level = LOG_NOTICE;

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
            , circular_buffer_{_config.circular_buffer_size}
            , mode_{0}
            , file_offset_{0}
            , existing_object_size_{config::UNKNOWN_OBJECT_SIZE}
            , download_to_cache_{true}
            , use_cache_{true}
            , object_must_exist_{false}
            , put_props_{}
            , bucket_context_{}
            , upload_manager_{bucket_context_}
            , critical_error_encountered_{false}
            , last_file_to_close_{false}
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
                return file_offset_;
            }

        }

        bool object_exists_in_s3(int64_t& object_size) {

            data_for_head_callback data(bucket_context_);

            S3ResponseHandler head_object_handler = { &s3_head_object_callback::on_response_properties,
                &s3_head_object_callback::on_response_complete };

            S3_head_object(&bucket_context_, object_key_.c_str(), 0, 0, &head_object_handler, &data);

            object_size = data.content_length;

            return libs3_types::status_ok == data.status;
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
            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] fd_=%d, is_open=%d use_cache_=%d\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), fd_, is_open(), use_cache_);

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


            if ( !use_cache_ && is_full_upload() && config_.number_of_client_transfer_threads > 1 ) {

                // This was a full multipart upload, wait for the upload to complete

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] wait for join of upload thread\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());

                // upload was in background.  wait for it to complete.
                if (begin_part_upload_thread_ptr_) {
                    begin_part_upload_thread_ptr_->join();
                    begin_part_upload_thread_ptr_ = nullptr;
                }

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] join for part\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());
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
                last_file_to_close_ = data.threads_remaining_to_close == 0;

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] [last_file_to_close=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(),
                        last_file_to_close_);

                // if a critical error occurred - do not flush cache file or complete multipart upload

                if (this->critical_error_encountered_) {

                    return_value = false;

                } else if (last_file_to_close_) {

                    if (this->use_cache_) {

                        rv = additional_processing_enum::DO_FLUSH_CACHE_FILE;


                    } else {


                        if ( this->is_full_upload() && this->config_.number_of_client_transfer_threads > 1 ) {

                            if (error_codes::SUCCESS != complete_multipart_upload()) {
                                return_value = false;
                            }
                        }

                        return_value = true;

                    }

                } else if (this->use_cache_) {

                    // not last file to close and using cache - close cache stream
                    if (use_cache_) {
                        rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] closing cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());
                        cache_fstream_.close();
                    }
                }

                return rv;

            }); // end close lock

            if (result == additional_processing_enum::DO_FLUSH_CACHE_FILE) {

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] closing cache file\n",
                        __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());

                cache_fstream_.close();

                if (error_codes::SUCCESS != flush_cache_file(shm_obj)) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] flush_cache_file returned error\n",
                            __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier());
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

                    std::stringstream msg;
                    msg << "send() position=" << position_before_write << " size=" << _buffer_size << " position_after_write=" << current_position;
                    rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), msg.str().c_str());

                    // return bytes written
                    return current_position - position_before_write;
                });
            }

            // Not using cache.

            // if this is a multipart upload and we have not yet initiated it, do so
            bool return_value = true;
            shm_obj.atomic_exec([this, &shm_obj, &return_value](auto& data) {

                if ( !(this->use_cache_) && this->is_full_upload() && this->config_.number_of_client_transfer_threads > 1 && !data.done_initiate_multipart) {

                    bool multipart_upload_success = this->begin_multipart_upload(shm_obj);
                    if (!multipart_upload_success) {
                        rodsLog(LOG_ERROR, "Initiate multipart failed.\n");
                        this->critical_error_encountered_ = true;
                        return_value = false;
                    } else {
                        data.done_initiate_multipart = true;
                    }
                }
            });

            // could not initiate multipart, return error
            if (return_value == false) {
                return 0;
            }

            // Put the buffer on the circular buffer.
            // We must copy the buffer because it will persist after send returns.
            buffer_type copied_buffer(_buffer, _buffer + _buffer_size);
            circular_buffer_.push_back({copied_buffer});

            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] wrote buffer of size %ld\n",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), _buffer_size);

            // if config_.part_size is 0 then bail
            if (config_.number_of_client_transfer_threads > 1 && 0 == config_.part_size) {
                rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] part size is zero\n", __FILE__, __LINE__,
                        __FUNCTION__, get_thread_identifier());
                    return 0;
            }

            // if we haven't already started an upload thread, start it
            if (!begin_part_upload_thread_ptr_) {
                if (config_.number_of_client_transfer_threads > 1) {
                    begin_part_upload_thread_ptr_ = std::make_unique<std::thread>(
                            &s3_transport::s3_upload_part_worker_routine, this, false, 0, 0);
                } else {
                    begin_part_upload_thread_ptr_ = std::make_unique<std::thread>(
                            &s3_transport::s3_upload_file, this, false);
                }
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

                switch (_dir) {
                    case std::ios_base::beg:
                        file_offset_ = _offset;
                        break;

                    case std::ios_base::cur:
                        file_offset_ = file_offset_ + _offset;
                        break;

                    case std::ios_base::end:
                        file_offset_ = config_.object_size + _offset;
                        break;

                    default:
                        return seek_error;
                }
                return file_offset_;
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


        void set_part_size(int64_t part_size) {
            config_.part_size = part_size;
        }

        bool is_last_file_to_close() {
            return last_file_to_close_;
        }

        // used for unit testing
        bool get_use_cache() {
            return use_cache_;
        }

    private:


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
                    return initiate_multipart_upload();
                });

                if (error_codes::SUCCESS != ret) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] open returning false [last_error_code=%d]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), ret);

                    // update the last error
                    shm_obj.atomic_exec([ret](auto& data) {
                        data.last_error_code = ret;
                    });

                    return false;
                }
            } else {
                if (error_codes::SUCCESS != last_error_code) {
                    rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] open returning false [last_error_code=%d]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            last_error_code);
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

            bool start_download = shm_obj.exec([](auto& data) {
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

                    return shm_obj.exec([](auto& data) {
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
                    return shm_obj.exec([](auto& data) {
                        return data.cache_file_download_progress = cache_file_download_status::FAILED;
                    });
                }

                return shm_obj.exec([](auto& data) {


                    data.cache_file_download_progress = cache_file_download_status::SUCCESS;
                    return data.cache_file_download_progress;
                });

            }

            // check the download status and return
            return shm_obj.exec([](auto& data) { return data.cache_file_download_progress; });

        }

        error_codes flush_cache_file(named_shared_memory_object& shm_obj) {

            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] Flushing cache file.\n",
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

            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] cache_file_size is %ld\n",
                    __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_size);

            // each part must be at least 5MB in size so adjust number_of_cache_transfer_threads accordingly
            int64_t minimum_part_size = config_.minimum_part_size;
            config_.number_of_cache_transfer_threads
                = minimum_part_size * config_.number_of_cache_transfer_threads < cache_file_size
                ? config_.number_of_cache_transfer_threads
                : cache_file_size / minimum_part_size == 0 ? 1 : cache_file_size / minimum_part_size;

            int64_t part_size = cache_file_size / config_.number_of_cache_transfer_threads;

            if (config_.number_of_cache_transfer_threads > 1) {

                initiate_multipart_upload();

                irods::thread_pool cache_flush_threads{static_cast<int>(config_.number_of_cache_transfer_threads)};

                for (unsigned int thr_id= 0; thr_id < config_.number_of_cache_transfer_threads; ++thr_id) {

                        irods::thread_pool::post(cache_flush_threads, [this, thr_id, part_size] () {
                                // upload part and read your part from cache file
                                s3_upload_part_worker_routine(true, thr_id, part_size);
                    });
                }

                cache_flush_threads.join();

                return_value = complete_multipart_upload();

            } else {

                return_value = s3_upload_file(true);
            }

            // remove cache file
            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] removing cache file %s\n",
                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), cache_file_path_.c_str());
            std::remove(cache_file_path_.c_str());

            // set cache file download flag to NOT_STARTED
            // already locked so just exec()
            shm_obj.exec([](auto& data) {
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
        //   - use_cache_         - Set to false unless one of the following is true:
        //                          * the object was opened in read only mode
        //                          * the put_repl_flag is true indicating a predictable full file write
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
                //   2. If doing multiplart upload file size < #threads * minimum part size
                if ( config_.object_size == 0 || config_.object_size == config::UNKNOWN_OBJECT_SIZE ||
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

            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] [_mode & in = %d][_mode & out = %d]"
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
                std::to_string(std::hash<std::string>{}(object_key_));

            upload_manager_.object_key = object_key_;
            upload_manager_.shmem_key = shmem_key_;

            mode_ = _mode;

            populate_open_mode_flags();

            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] [object_key_ = %s][use_cache_ = %d]"
                "[download_to_cache_ = %d]\n",
                __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                object_key_.c_str(),
                use_cache_,
                download_to_cache_);

            // each process must intitialize S3
            {
                std::lock_guard<std::mutex> lock(s3_initialized_counter_mutex_);
                if (s3_initialized_counter_ == 0) {

                    int flags = S3_INIT_ALL;

                    int status = S3_initialize( "s3", flags, bucket_context_.hostName );
                    if (status != libs3_types::status_ok) {
                        rodsLog(LOG_ERROR, "S3_initialize returned error\n");
                        return false;
                    }
                }
                ++s3_initialized_counter_;
            }

            bool object_exists = false;
            int64_t s3_object_size = 0;


            if (object_must_exist_ || download_to_cache_) {

                object_exists = object_exists_in_s3(s3_object_size);

                // save the size of the existing object as we may need it later
                existing_object_size_ = s3_object_size;

            }

            if (object_must_exist_ && !object_exists) {
                rodsLog(LOG_ERROR, "Object does not exist and open mode requires it to exist.\n");
                return false;
            }

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // only allow open/close to run one at a time for this object
            bool return_value = true;
            shm_obj.atomic_exec([this, object_exists, &return_value, &shm_obj, s3_object_size](auto& data) {

                if (object_exists && this->download_to_cache_) {

                    cache_file_download_status download_status = this->download_object_to_cache(shm_obj, s3_object_size);

                    if (cache_file_download_status::SUCCESS != download_status) {
                            rodsLog(LOG_ERROR, "failed to download file to cache, download_status =%d\n", download_status);
                        return_value = false;
                    }
                }

                if (this->use_cache_) {

                    // using cache, open the cache file for subsequent reads/writes
                    // use the mode that was passed in

                    if (!this->cache_fstream_ || !this->cache_fstream_.is_open()) {

                        bf::path cache_file =  bf::path(this->config_.cache_directory) / bf::path(object_key_ + "-cache");
                        bf::path parent_path = cache_file.parent_path();

                        try {
                            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] Creating parent_path  %s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), parent_path.string().c_str());
                            boost::filesystem::create_directories(parent_path);
                        } catch (boost::filesystem::filesystem_error& e) {
                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Could not create parent directories for cache file.  %s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), e.what());
                            return_value = false;
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
                            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] opened cache file %s with create [trunc_flag=%d]\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_path_.c_str(), trunc_flag);
                            // file may not exist, open with std::ios_base::out to create then with in/out
                            cache_fstream_.open(cache_file_path_.c_str(), std::ios_base::out);
                            cache_fstream_.close();
                            cache_fstream_.open(cache_file_path_.c_str(), mode | std::ios_base::in | std::ios_base::out);
                        } else {
                            rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] opened cache file %s [trunc_flag=%d]\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), cache_file_path_.c_str(), trunc_flag);
                        }

                        if (!cache_fstream_ || !cache_fstream_.is_open()) {
                            rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] Failed to open cache file %s, error=%s\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->get_thread_identifier(), cache_file_path_.c_str(), strerror(errno));
                            this->critical_error_encountered_ = true;
                            return_value = false;
                        }

                        if (!this->seek_to_end_if_required(this->mode_)) {
                            critical_error_encountered_ = true;
                            return_value = false;
                        }
                    }

                } else {

                    // not using cache, just create our own fd

                    const auto fd = this->file_descriptor_counter_++;

                    if (fd < minimum_valid_file_descriptor) {
                        this->critical_error_encountered_ = true;
                        return_value = false;
                    }

                    this->fd_ = fd;

                    if (!this->seek_to_end_if_required(mode_)) {
                        this->critical_error_encountered_ = true;
                        return_value = false;
                    }
                }

                // set the number of threads to close to the number of client transfer threads
                // this will count down as closes happen
                if (data.threads_remaining_to_close == 0) {
                    data.threads_remaining_to_close = this->config_.number_of_client_transfer_threads;
                }


            });

            return return_value;


        }  // end open_impl

        error_codes initiate_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            unsigned int retry_cnt    = 0;
            put_props_.useServerSideEncryption = config_.server_encrypt_flag;
            std::stringstream msg;

            put_props_.md5 = nullptr;
            put_props_.expires = -1;

            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;
            upload_manager_.xml = "";

            msg.str( std::string() ); // Clear

            data_for_write_callback data{bucket_context_, circular_buffer_};
            data.thread_identifier = get_thread_identifier();

            // read shared memory entry for this key

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            return shm_obj.exec([this, &retry_cnt](auto& data) {

                retry_cnt = 0;

                // These expect a upload_manager* as cbdata
                S3MultipartInitialHandler mpu_initial_handler
                    = { { s3_multipart_upload::initialization_callback::on_response_properties,
                          s3_multipart_upload::initialization_callback::on_response_complete },
                        s3_multipart_upload::initialization_callback::on_response };

                do {
                    rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] call S3_initiate_multipart [object_key=%s]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), object_key_.c_str());

                    S3_initiate_multipart(&bucket_context_, object_key_.c_str(),
                            &put_props_, &mpu_initial_handler, nullptr, 0, &upload_manager_);

                    rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] [manager.status=%s]\n", __FILE__, __LINE__,
                            __FUNCTION__, get_thread_identifier(), S3_get_status_name(upload_manager_.status));

                    if (upload_manager_.status != libs3_types::status_ok) {
                        s3_sleep( config_.retry_wait_seconds, 0 );
                    }

                } while ( (upload_manager_.status != libs3_types::status_ok)
                        && S3_status_is_retryable(upload_manager_.status)
                        && ( ++retry_cnt < config_.retry_count_limit));

                if ("" == data.upload_id || upload_manager_.status != libs3_types::status_ok) {
                    return error_codes::INITIATE_MULTIPART_UPLOAD_ERROR;
                }

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] S3_initiate_multipart returned.  Upload ID = %s\n",
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
            // no lock here as it is already locked
            std::string upload_id = shm_obj.exec([](auto& data) {
                return data.upload_id.c_str();
            });

            S3AbortMultipartUploadHandler abort_handler
                = { { s3_multipart_upload::cancel_callback::on_response_properties,
                      s3_multipart_upload::cancel_callback::on_response_completion } };

            std::stringstream msg;
            libs3_types::status status;

            msg << "Cancelling multipart upload: key=\""
                << object_key_ << "\", upload_id=\"" << upload_id << "\"";
            rodsLog(config_.debug_log_level,  "%s\n", msg.str().c_str() );

            s3_multipart_upload::cancel_callback::g_response_completion_status = libs3_types::status_ok;
            s3_multipart_upload::cancel_callback::g_response_completion_saved_bucket_context = &bucket_context_;
            S3_abort_multipart_upload(&bucket_context_, object_key_.c_str(),
                    upload_id.c_str(), 0, &abort_handler);
            status = s3_multipart_upload::cancel_callback::g_response_completion_status;
            if (status != libs3_types::status_ok) {
                msg.str( std::string() ); // Clear
                msg << "] " << __FUNCTION__
                    << " - Error cancelling the multipart upload of S3 object: \""
                    << object_key_ << "\"";
                if (status >= 0) {
                    msg << " - \"" << S3_get_status_name(status) << "\"";
                }
                rodsLog(config_.debug_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.str().c_str() );
            }
        } // end mpu_cancel


        error_codes complete_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            error_codes result = shm_obj.exec([this](auto& data) {

                std::stringstream msg;

                std::stringstream xml("");

                std::string upload_id  = data.upload_id.c_str();

                if (error_codes::SUCCESS == data.last_error_code) { // If someone aborted, don't complete...

                    msg.str( std::string() ); // Clear
                    msg << "Multipart:  Completing key \"" << object_key_.c_str() << "\" Upload ID \""
                        << upload_id << "\"";
                    rodsLog(config_.debug_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            msg.str().c_str() );

                    uint64_t i;
                    xml << "<CompleteMultipartUpload>\n";
                    for ( i = 0; i < data.etags.size(); i++ ) {
                        xml << "<Part><PartNumber>";
                        xml << (i + 1);
                        xml << "</PartNumber><ETag>";
                        xml << data.etags[i];
                        xml << "</ETag></Part>";
                    }
                    xml << "</CompleteMultipartUpload>\n";

                    rodsLog(config_.debug_log_level,  "%s:%d (%s) [[%lu]] Response: %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                            xml.str().c_str() );

                    int manager_remaining = xml.str().size();
                    upload_manager_.offset = 0;
                    unsigned int retry_cnt = 0;
                    S3MultipartCommitHandler commit_handler
                        = { {s3_multipart_upload::commit_callback::on_response_properties,
                             s3_multipart_upload::commit_callback::on_response_completion },
                            s3_multipart_upload::commit_callback::on_response, nullptr };
                    do {
                        // On partial error, need to restart XML send from the beginning
                        upload_manager_.remaining = manager_remaining;
                        upload_manager_.xml = xml.str().c_str();

                        upload_manager_.offset = 0;
                        S3_complete_multipart_upload(&bucket_context_, object_key_.c_str(),
                                &commit_handler, upload_id.c_str(),
                                upload_manager_.remaining, nullptr, 0, &upload_manager_);
                        rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] [manager.status=%s]\n", __FILE__, __LINE__,
                                __FUNCTION__, get_thread_identifier(), S3_get_status_name(upload_manager_.status));
                        if (upload_manager_.status != libs3_types::status_ok) s3_sleep( config_.retry_wait_seconds, 0 );
                    } while ((upload_manager_.status != libs3_types::status_ok) &&
                            S3_status_is_retryable(upload_manager_.status) &&
                            ( ++retry_cnt < config_.retry_count_limit));

                    if (upload_manager_.status != libs3_types::status_ok) {
                        msg.str( std::string() ); // Clear
                        msg << __FUNCTION__ << " - Error putting the S3 object: \""
                            << object_key_ << "\"";
                        if(upload_manager_.status >= 0) {
                            msg << " - \"" << S3_get_status_name( upload_manager_.status ) << "\"";
                        }
                        return error_codes::UPLOAD_FILE_ERROR;
                    }
                }

                if (error_codes::SUCCESS != data.last_error_code && "" != data.upload_id ) {

                    // Someone aborted after we started, delete the partial object on S3
                    rodsLog(config_.debug_log_level, "Cancelling multipart upload\n");
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

            std::stringstream msg;

            unsigned int retry_cnt = 0;

            if (0 > offset) {
                offset = file_offset_;
            }

            std::shared_ptr<callback_for_read_from_s3_base<buffer_type>> read_callback;

            S3GetObjectHandler get_object_handler = {
                {
                    callback_for_read_from_s3_base<buffer_type>::on_response_properties,
                    callback_for_read_from_s3_base<buffer_type>::on_response_completion
                },
                callback_for_read_from_s3_base<buffer_type>::invoke_callback
            };

            if (buffer == nullptr) {
                // Download to cache
                read_callback.reset(new callback_for_read_from_s3_to_cache<buffer_type>
                        (bucket_context_));
                static_cast<callback_for_read_from_s3_to_cache<buffer_type>*>
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

                read_callback.reset(new callback_for_read_from_s3_to_buffer<buffer_type>(bucket_context_));
                static_cast<callback_for_read_from_s3_to_buffer<buffer_type>*>(read_callback.get())
                    ->set_output_buffer(buffer);
                static_cast<callback_for_read_from_s3_to_buffer<buffer_type>*>(read_callback.get())
                    ->set_output_buffer_size(length);

            }
            read_callback->content_length = length;
            read_callback->thread_identifier = get_thread_identifier();
            read_callback->shmem_key = shmem_key_;
            read_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;


            do {

                // if we are reading into cache, write to cache file at offset
                // if reading into buffer, write at beginning of buffer
                if (buffer == nullptr) {
                    read_callback->offset = offset;
                } else {
                    read_callback->offset = 0;
                }

                msg.str( std::string() ); // Clear
                msg << "Multirange:  Start range key \"" << object_key_ << "\", offset "
                    << static_cast<long>(offset) << ", len "
                    << static_cast<int>(read_callback->content_length);
                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.str().c_str());

                uint64_t start_microseconds = get_time_in_microseconds();

                S3_get_object( &bucket_context_, object_key_.c_str(), NULL,
                        offset, read_callback->content_length, 0, 0,
                        &get_object_handler, read_callback.get() );

                uint64_t end_microseconds = get_time_in_microseconds();
                double bw = (read_callback->content_length / (1024.0*1024.0)) /
                    ( (end_microseconds - start_microseconds) / 1000000.0 );
                msg << " -- END -- BW=" << bw << " MB/s";
                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        get_thread_identifier(), msg.str().c_str());

                if (read_callback->status != libs3_types::status_ok) s3_sleep( config_.retry_wait_seconds, 0 );

            } while ((read_callback->status != libs3_types::status_ok)
                    && S3_status_is_retryable(read_callback->status)
                    && (++retry_cnt < config_.retry_count_limit));

            if (read_callback->status != libs3_types::status_ok) {
                msg.str( std::string() ); // Clear
                msg << " - Error getting the S3 object: \"" << object_key_ << " ";
                if (read_callback->status >= 0) {
                    msg << " - \"" << S3_get_status_name( read_callback->status ) << "\"";
                }
                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        get_thread_identifier(), msg.str().c_str());

                // update the last error in shmem

                named_shared_memory_object shm_obj{shmem_key_,
                    config_.shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                if (shmem_already_locked) {
                    shm_obj.exec([](auto& data) {
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
                                           unsigned int part_number = 0,
                                           unsigned int part_size = 0)
        {

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            std::shared_ptr<s3_multipart_upload::callback_for_write_to_s3_base<buffer_type>> write_callback;

            // read upload_id from shmem

            named_shared_memory_object shm_obj{shmem_key_,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            std::string upload_id =  shm_obj.atomic_exec([](auto& data) {
                return data.upload_id.c_str();
            });

            unsigned int retry_cnt = 0;

            S3PutObjectHandler put_object_handler = {
                {
                    s3_multipart_upload::callback_for_write_to_s3_base<buffer_type>::on_response_properties,
                    s3_multipart_upload::callback_for_write_to_s3_base<buffer_type>::on_response_completion
                },
                s3_multipart_upload::callback_for_write_to_s3_base<buffer_type>::invoke_callback
            };

            off_t offset;

            if (read_from_cache) {

                // read from cache

                write_callback.reset(new s3_multipart_upload::callback_for_write_from_cache_to_s3<buffer_type>
                        (bucket_context_, upload_manager_));

                s3_multipart_upload::callback_for_write_from_cache_to_s3<buffer_type>
                    *write_callback_from_cache =
                    static_cast<s3_multipart_upload::callback_for_write_from_cache_to_s3<buffer_type>*>
                    (write_callback.get());

                write_callback_from_cache->set_and_open_cache_file(cache_file_path_);

                offset = part_size * part_number;
                int64_t content_length;

                // get the object size from the cache file
                auto object_size = get_cache_file_size();

                // last thread gets extra bits
                if (part_number == config_.number_of_cache_transfer_threads - 1) {
                    content_length = part_size + (object_size -
                            part_size * config_.number_of_cache_transfer_threads);
                } else {
                    content_length = part_size;
                }

                write_callback->sequence = part_number + 1;
                write_callback->content_length = content_length;
                write_callback->offset = offset;
                write_callback->thread_identifier = get_thread_identifier();

            } else {

                // Read from buffer

                write_callback.reset(new
                        s3_multipart_upload::callback_for_write_from_buffer_to_s3<buffer_type>(
                            bucket_context_, upload_manager_, circular_buffer_));

                s3_multipart_upload::callback_for_write_from_buffer_to_s3<buffer_type>
                    *write_callback_from_buffer =
                    static_cast<s3_multipart_upload::callback_for_write_from_buffer_to_s3<buffer_type>*>
                    (write_callback.get());

                upload_page<buffer_type> page;

                // read the first page
                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] waiting to read\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());

                circular_buffer_.pop_front(page);

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] read page [buffer=%p][buffer_size=%lu]\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), page.buffer.data(),
                        page.buffer.size());

                write_callback_from_buffer->buffer = page.buffer;

                // determine the sequence number from the offset, file size, and buffer size
                // the last page might be larger so doing a little trick to handle that case (second term)
                //  Note:  We bailed early if config_.part_size == 0
                unsigned long sequence = (file_offset_ / config_.part_size) +
                    (file_offset_ % config_.part_size == 0 ? 0 : 1) + 1;

                write_callback->sequence = sequence;
                write_callback->content_length = config_.part_size;

                // estimate the size and resize the etags vector
                unsigned long  number_of_parts = config_.object_size / config_.part_size;
                number_of_parts = number_of_parts < sequence ? sequence : number_of_parts;

                // resize the etags vector if necessary
                int resize_error = shm_obj.atomic_exec([number_of_parts, &shm_obj](auto& data) {

                    if (number_of_parts > data.etags.size()) {
                        try {
                            data.etags.resize(number_of_parts, types::shm_char_string("", shm_obj.get_allocator()));
                        } catch (std::bad_alloc& ba) {
                            data.last_error_code = error_codes::BAD_ALLOC;
                            return true;
                        }
                    }

                    return false;

                });

                if (resize_error) {
                    rodsLog(config_.debug_log_level, "Error on reallocation of etags buffer in shared memory.");
                    return;
                }
            }

            write_callback->enable_md5 = config_.enable_md5_flag;
            write_callback->thread_identifier = get_thread_identifier();
            write_callback->object_key = object_key_;
            write_callback->shmem_key = shmem_key_;
            write_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;

            do {

                if (read_from_cache) {
                    write_callback->offset = offset;
                }

                std::stringstream msg;

                msg << "Multipart:  Start part " << static_cast<int>(write_callback->sequence) << ", key \""
                    << object_key_ << "\", uploadid \"" << upload_id
                    << "\", len " << static_cast<int>(write_callback->content_length);
                rodsLog(config_.debug_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.str().c_str() );

                put_props_.md5 = nullptr;
                put_props_.expires = -1;

                // server encrypt flag not valid for part upload
                put_props_.useServerSideEncryption = false;

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] S3_upload_part (ctx, %s, props, handler, %lu, "
                       "uploadId, %lu, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                       object_key_.c_str(), write_callback->sequence,
                       write_callback->content_length);

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_,
                        &put_object_handler, write_callback->sequence, upload_id.c_str(),
                        write_callback->content_length, 0, 0, write_callback.get());

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] S3_upload_part returned [part=%lu][status=%s].\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), write_callback->sequence,
                        S3_get_status_name(write_callback->status));

                msg.str(std::string());
                msg.clear();
                msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                rodsLog(config_.debug_log_level,  "%s:%d (%s) [[%lu]] %s\n", __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        msg.str().c_str() );

                if (write_callback->status != libs3_types::status_ok) s3_sleep( config_.retry_wait_seconds, 0 );
            } while ((write_callback->status != libs3_types::status_ok) && S3_status_is_retryable(write_callback->status) &&
                    (++retry_cnt < config_.retry_count_limit));

            if (write_callback->status != libs3_types::status_ok) {

                shm_obj.atomic_exec([](auto& data) {
                    data.last_error_code = error_codes::UPLOAD_FILE_ERROR;
                });
            }
        }

        error_codes s3_upload_file(bool read_from_cache = false)
        {

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;


            std::shared_ptr<s3_upload::callback_for_write_to_s3_base<buffer_type>> write_callback;

            unsigned int retry_cnt = 0;

            do {

                S3PutObjectHandler put_object_handler = {
                    {
                        s3_upload::callback_for_write_to_s3_base<buffer_type>::on_response_properties,
                        s3_upload::callback_for_write_to_s3_base<buffer_type>::on_response_completion
                    },
                    s3_upload::callback_for_write_to_s3_base<buffer_type>::invoke_callback
                };

                if (read_from_cache) {

                    // read from cache

                    write_callback.reset(new s3_upload::callback_for_write_from_cache_to_s3<buffer_type>
                            (bucket_context_, upload_manager_));

                    s3_upload::callback_for_write_from_cache_to_s3<buffer_type>
                        *write_callback_from_cache =
                        static_cast<s3_upload::callback_for_write_from_cache_to_s3<buffer_type>*>
                        (write_callback.get());

                    write_callback_from_cache->set_and_open_cache_file(cache_file_path_);

                    write_callback->content_length = get_cache_file_size();
                    write_callback->offset = 0;


                } else {

                    // Read from buffer

                    write_callback.reset(new
                            s3_upload::callback_for_write_from_buffer_to_s3<buffer_type>(
                                bucket_context_, upload_manager_, circular_buffer_));

                    s3_upload::callback_for_write_from_buffer_to_s3<buffer_type>
                        *write_callback_from_buffer =
                        static_cast<s3_upload::callback_for_write_from_buffer_to_s3<buffer_type>*>
                        (write_callback.get());

                    upload_page<buffer_type> page;

                    // read the first page
                    rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] waiting to read\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier());

                    circular_buffer_.pop_front(page);

                    rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] read page [buffer=%p][buffer_size=%lu]\n",
                            __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(), page.buffer.data(),
                            page.buffer.size());

                    write_callback_from_buffer->buffer = page.buffer;
                    write_callback->content_length = config_.object_size;

                }

                write_callback->offset = 0;
                write_callback->enable_md5 = config_.enable_md5_flag;
                write_callback->thread_identifier = get_thread_identifier();
                write_callback->object_key = object_key_;
                write_callback->shmem_key = shmem_key_;
                write_callback->shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;

                std::stringstream msg;

                put_props_.md5 = nullptr;
                put_props_.expires = -1;
                put_props_.useServerSideEncryption = config_.server_encrypt_flag;

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] S3_put_object(ctx, %s, "
                       "%lu, put_props_, 0, &putObjectHandler, &data)\n",
                       __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                       object_key_.c_str(),
                       write_callback->content_length);

                S3_put_object(&bucket_context_, object_key_.c_str(), write_callback->content_length,
                        &put_props_, 0, 0, &put_object_handler, write_callback.get());

                rodsLog(config_.debug_log_level, "%s:%d (%s) [[%lu]] S3_put_object returned [status=%s].\n",
                        __FILE__, __LINE__, __FUNCTION__, get_thread_identifier(),
                        S3_get_status_name(write_callback->status));

                if (write_callback->status != libs3_types::status_ok) s3_sleep( config_.retry_wait_seconds, 0 );
            } while ((write_callback->status != libs3_types::status_ok) && S3_status_is_retryable(write_callback->status) &&
                    (++retry_cnt < config_.retry_count_limit));

            if (write_callback->status != libs3_types::status_ok) {
                return error_codes::UPLOAD_FILE_ERROR;
            }

            return error_codes::SUCCESS;

        } // end s3_upload_file


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

        irods::experimental::circular_buffer<upload_page<buffer_type>>
                                     circular_buffer_;

        std::ios_base::openmode      mode_;
        off_t                        file_offset_;
        int64_t                      existing_object_size_;

        // operational modes based on input flags
        bool                         download_to_cache_;
        bool                         use_cache_;
        bool                         object_must_exist_;

        S3PutProperties              put_props_;
        libs3_types::bucket_context  bucket_context_;
        upload_manager               upload_manager_;

        bool                         critical_error_encountered_;

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

        // this is set to true when the last file closes
        bool                         last_file_to_close_;

    }; // s3_transport

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

