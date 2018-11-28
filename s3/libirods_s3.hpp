/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef _LIBIRODS_S3_HPP_
#define _LIBIRODS_S3_HPP_

// Needed to support pread with > 2GB offsets
#define _USE_FILE_OFFSET64

#include <rodsType.h>
#include <rodsDef.h>
#include <irods_resource_plugin.hpp>
#include <irods_lookup_table.hpp>
#include <irods_file_object.hpp>
#include <rcConnect.h>
#include <libs3.h>

#define S3_AUTH_FILE "s3Auth"
#define ARCHIVE_NAMING_POLICY_KW    "ARCHIVE_NAMING_POLICY"
#define CONSISTENT_NAMING           "consistent"
#define DECOUPLED_NAMING            "decoupled"

// For s3PutCopyFile to identify the real source type
typedef enum { S3_PUTFILE, S3_COPYOBJECT } s3_putcopy;

const std::string s3_default_hostname{"S3_DEFAULT_HOSTNAME"};
const std::string host_mode{"HOST_MODE"};
const std::string s3_auth_file{"S3_AUTH_FILE"};
const std::string s3_key_id{"S3_ACCESS_KEY_ID"};
const std::string s3_access_key{"S3_SECRET_ACCESS_KEY"};
const std::string s3_retry_count{"S3_RETRY_COUNT"};
const std::string s3_wait_time_sec{"S3_WAIT_TIME_SEC"};
const std::string s3_proto{"S3_PROTO"};
const std::string s3_stsdate{"S3_STSDATE"};
const std::string s3_max_upload_size{"S3_MAX_UPLOAD_SIZE"};
const std::string s3_enable_mpu = "S3_ENABLE_MPU";
const std::string s3_mpu_chunk{"S3_MPU_CHUNK"};
const std::string s3_mpu_threads{"S3_MPU_THREADS"};
const std::string s3_enable_md5{"S3_ENABLE_MD5"};
const std::string s3_server_encrypt{"S3_SERVER_ENCRYPT"};
const std::string s3_signature_version{"S3_SIGNATURE_VERSION"};
const std::string s3_region_name{"S3_REGIONNAME"};
const std::string REPL_POLICY_KEY{"repl_policy"};
const std::string REPL_POLICY_VAL{"reg_repl"};

const char* s3GetHostname();


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
    long offset;       /* For multiple upload */
    rodsLong_t contentLength, originalContentLength;
    S3Status status;
    int keyCount;
    s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */

    S3BucketContext *pCtx; /* To enable more detailed error messages */
} callback_data_t;

typedef struct upload_manager
{
    char *upload_id;    /* Returned from S3 on MP begin */
    char **etags;       /* Each upload part's MD5 */

    /* Below used for the upload completion command, need to send in XML */
    char *xml;
    long remaining;
    long offset;

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
    bool enable_md5;
    bool server_encrypt;
} multipart_data_t;

typedef struct multirange_data
{
    int seq;
    callback_data get_object_data;
    S3Status status;

    S3BucketContext *pCtx; /* To enable more detailed error messages */
} multirange_data_t;

// Sleep for *at least* the given time, plus some up to 1s additional
// The random addition ensures that threads don't all cluster up and retry
// at the same time (dogpile effect)
void s3_sleep(
    int _s,
    int _ms ); 

const char *s3GetHostname();

void responseCompleteCallback(
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
    std::string&       _key); 

irods::error s3Init ( irods::plugin_property_map& _prop_map ); 

S3Protocol s3GetProto( irods::plugin_property_map& _prop_map);

S3STSDate s3GetSTSDate( irods::plugin_property_map& _prop_map);

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
    irods::plugin_property_map& _prop_map );

/// @brief Function to copy the specified src file to the specified dest file
irods::error s3CopyFile(
    irods::plugin_context& _src_ctx,
    const std::string& _src_file,
    const std::string& _dest_file,
    const std::string& _key_id,
    const std::string& _access_key,
    const S3Protocol _proto,
    const S3STSDate _stsDate);

// =-=-=-=-=-=-=-
/// @brief Checks the basic operation parameters and updates the physical path in the file object
irods::error s3CheckParams(irods::plugin_context& _ctx ); 

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
