/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef _LIBIRODS_S3_HPP_
#define _LIBIRODS_S3_HPP_

// Needed to support pread with > 2GB offsets
#define _USE_FILE_OFFSET64

#include <irods/rodsType.h>
#include <irods/rodsDef.h>
#include <irods/irods_resource_plugin.hpp>
#include <irods/irods_lookup_table.hpp>
#include <irods/irods_file_object.hpp>
#include <irods/rcConnect.h>
#include <libs3.h>

#define S3_AUTH_FILE "s3Auth"
#define ARCHIVE_NAMING_POLICY_KW    "ARCHIVE_NAMING_POLICY"
#define CONSISTENT_NAMING           "consistent"
#define DECOUPLED_NAMING            "decoupled"

// For s3PutCopyFile to identify the real source type
typedef enum { S3_PUTFILE, S3_COPYOBJECT } s3_putcopy;

extern const std::string  s3_default_hostname;
extern const std::string  s3_default_hostname_vector;
extern const std::string  s3_hostname_index;
extern const std::string  host_mode;
extern const std::string  s3_auth_file;
extern const std::string  s3_key_id;
extern const std::string  s3_access_key;
extern const std::string  s3_retry_count;
extern const std::string  s3_wait_time_sec;
extern const std::string  s3_proto;
extern const std::string  s3_stsdate;
extern const std::string  s3_max_upload_size;
extern const std::string  s3_enable_mpu;
extern const std::string  s3_mpu_chunk;
extern const std::string  s3_mpu_threads;
extern const std::string  s3_enable_md5;
extern const std::string  s3_server_encrypt;
extern const std::string  s3_region_name;
extern const std::string  REPL_POLICY_KEY;
extern const std::string  REPL_POLICY_VAL;
extern const std::string  s3_cache_dir;
extern const std::string  s3_circular_buffer_size;
extern const std::string  s3_circular_buffer_timeout_seconds; // timeout for read or write to circular buffer
extern const std::string  s3_uri_request_style;        //  either "path" or "virtual_hosted" - default "path"
extern const std::string  s3_number_of_threads;        //  to save number of threads
extern const size_t       S3_DEFAULT_RETRY_WAIT_SECONDS;
extern const size_t       S3_DEFAULT_MAX_RETRY_WAIT_SECONDS;
extern const size_t       S3_DEFAULT_RETRY_COUNT;
extern const int          S3_DEFAULT_CIRCULAR_BUFFER_SIZE;
extern const unsigned int S3_DEFAULT_CIRCULAR_BUFFER_TIMEOUT_SECONDS;
extern const unsigned int S3_DEFAULT_RESTORATION_DAYS;
extern const std::string  S3_DEFAULT_RESTORATION_TIER;
extern const unsigned int S3_DEFAULT_NON_DATA_TRANSFER_TIMEOUT_SECONDS;

std::string s3GetHostname(irods::plugin_property_map& _prop_map);
int64_t s3GetMPUChunksize(irods::plugin_property_map& _prop_map);
ssize_t s3GetMPUThreads(irods::plugin_property_map& _prop_map);
bool s3GetEnableMultiPartUpload (irods::plugin_property_map& _prop_map);
S3UriStyle s3_get_uri_request_style(irods::plugin_property_map& _prop_map);
std::string get_region_name(irods::plugin_property_map& _prop_map);
bool s3GetServerEncrypt (irods::plugin_property_map& _prop_map);
std::string get_cache_directory(irods::plugin_property_map& _prop_map);

size_t get_retry_wait_time_sec(irods::plugin_property_map& _prop_map);
size_t get_max_retry_wait_time_sec(irods::plugin_property_map& _prop_map);
size_t get_retry_count(irods::plugin_property_map& _prop_map);
unsigned int get_non_data_transfer_timeout_seconds(irods::plugin_property_map& _prop_map);
unsigned int s3_get_restoration_days(irods::plugin_property_map& _prop_map);
std::string s3_get_restoration_tier(irods::plugin_property_map& _prop_map);

void StoreAndLogStatus(S3Status status, const S3ErrorDetails *error,
        const char *function, const S3BucketContext *pCtx, S3Status *pStatus,
        bool ignore_not_found_error = false);


typedef struct S3Auth {
    char accessKeyId[MAX_NAME_LEN];
    char secretAccessKey[MAX_NAME_LEN];
} s3Auth_t;

typedef struct s3Stat
{
    char key[MAX_NAME_LEN];
    rodsLong_t size;
    time_t lastModified;
} s3Stat_t;

typedef struct callback_data
{
    int fd;
    int64_t offset;       /* For multiple upload */
    rodsLong_t contentLength, originalContentLength;
    S3Status status;
    int keyCount;
    s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */

    S3BucketContext *pCtx; /* To enable more detailed error messages */
    irods::plugin_property_map *prop_map_ptr;
} callback_data_t;

typedef struct upload_manager
{
    char *upload_id;    /* Returned from S3 on MP begin */
    char **etags;       /* Each upload part's MD5 */

    /* Below used for the upload completion command, need to send in XML */
    char *xml;
    int64_t remaining;
    int64_t offset;

    S3BucketContext *pCtx; /* To enable more detailed error messages */

    S3Status status;
} upload_manager_t;

typedef struct multipart_data
{
    int seq;                       /* Sequence number, i.e. which part */
    int mode;                      /* PUT or COPY */
    S3BucketContext *pSrcCtx;      /* Source bucket context, ignored in a PUT */
    const char *srcKey;            /* Source key, ignored in a PUT */
    callback_data put_object_data; /* File being uploaded */
    upload_manager_t *manager;     /* To update w/the MD5 returned */

    S3Status status;
    bool server_encrypt;
} multipart_data_t;

typedef struct multirange_data
{
    int seq;
    callback_data get_object_data;
    S3Status status;

    S3BucketContext *pCtx; /* To enable more detailed error messages */
    irods::plugin_property_map *prop_map_ptr;
} multirange_data_t;

// Sleep between _s/2 and _s seconds.
// The random addition ensures that threads don't all cluster up and retry
// at the same time (dogpile effect)
void s3_sleep( int _s );

void responseCompleteCallback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData);

void responseCompleteCallbackIgnoreLoggingNotFound(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData);

S3Status responsePropertiesCallback(
    const S3ResponseProperties *properties,
    void *callbackData);

// Utility functions
irods::error parseS3Path (
    const std::string& _s3ObjName,
    std::string&       _bucket,
    std::string&       _key,
    irods::plugin_property_map& _prop_map );

irods::error s3Init ( irods::plugin_property_map& _prop_map );
irods::error s3InitPerOperation ( irods::plugin_property_map& _prop_map );

S3Protocol s3GetProto( irods::plugin_property_map& _prop_map);

S3STSDate s3GetSTSDate( irods::plugin_property_map& _prop_map);

int64_t s3GetMaxUploadSizeMB (irods::plugin_property_map& _prop_map);

bool s3_copyobject_disabled(irods::plugin_property_map& _prop_map);

irods::error s3GetFile(
    const std::string& _filename,
    const std::string& _s3ObjName,
    rodsLong_t _fileSize,
    const std::string& _key_id,
    const std::string& _access_key,
    irods::plugin_property_map& _prop_map );

irods::error s3PutCopyFile(
    const s3_putcopy _mode,
    const std::string& _filename,
    const std::string& _s3ObjName,
    rodsLong_t _fileSize,
    const std::string& _key_id,
    const std::string& _access_key,
    irods::plugin_property_map& _prop_map);

/// @brief Function to copy the specified src file to the specified dest file
irods::error s3CopyFile(
    irods::plugin_context& _src_ctx,
    const std::string& _src_file,
    const std::string& _dest_file,
    const std::string& _key_id,
    const std::string& _access_key,
    const S3Protocol _proto,
    const S3STSDate _stsDate,
    const S3UriStyle _s3_uri_style);

// =-=-=-=-=-=-=-
/// @brief Checks the basic operation parameters and updates the physical path in the file object
irods::error s3CheckParams(irods::plugin_context& _ctx );


std::tuple<bool, bool> get_modes_from_properties(irods::plugin_property_map& _prop_map_);

std::string get_resource_name(irods::plugin_property_map& _prop_map);

bool determine_unlink_for_repl_policy(
    rsComm_t*          _comm,
    const std::string& _logical_path,
    const std::string& _vault_path);

// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectCreate(
    irods::plugin_property_map& _prop_map,
    irods::file_object&         _file_obj,
    const std::string&          _resc_name,
    const std::string&          _curr_host,
    float&                      _out_vote );

// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectOpen(
    rsComm_t*                   _comm,
    irods::plugin_property_map& _prop_map,
    irods::file_object_ptr      _file_obj,
    const std::string&          _resc_name,
    const std::string&          _curr_host,
    float&                      _out_vote );

irods::error s3GetAuthCredentials(
    irods::plugin_property_map& _prop_map,
    std::string& _rtn_key_id,
    std::string& _rtn_access_key);

#endif
