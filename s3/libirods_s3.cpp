// =-=-=-=-=-=-=-
// Needed to support pread with > 2GB offsets
#define _USE_FILE_OFFSET64

#include "libirods_s3.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include <msParam.h>
#include <rcConnect.h>
#include <rodsLog.h>
#include <rodsErrorTable.h>
#include <objInfo.h>

#ifdef USING_JSON
#include <json/json.h>
#endif

// =-=-=-=-=-=-=-
// irods includes
#include "irods_resource_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_physical_object.hpp"
#include "irods_collection_object.hpp"
#include "irods_string_tokenize.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_kvp_string_parser.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <ctime>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string.hpp>

// =-=-=-=-=-=-=-
// system includes
#include <openssl/md5.h>
#ifndef _WIN32
#include <sys/file.h>
#include <sys/param.h>
#endif
#include <errno.h>
#include <sys/stat.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#if defined(osx_platform)
#include <sys/malloc.h>
#else
#include <malloc.h>
#endif
#include <fcntl.h>
#ifndef _WIN32
#include <sys/file.h>
#include <unistd.h>
#endif
#include <dirent.h>

#if defined(solaris_platform)
#include <sys/statvfs.h>
#endif
#include <sys/stat.h>

#include <string.h>

const std::string s3_default_hostname = "S3_DEFAULT_HOSTNAME";
const std::string s3_auth_file = "S3_AUTH_FILE";
const std::string s3_key_id = "S3_ACCESS_KEY_ID";
const std::string s3_access_key = "S3_SECRET_ACCESS_KEY";
const std::string s3_retry_count = "S3_RETRY_COUNT";
const std::string s3_wait_time_sec = "S3_WAIT_TIME_SEC";
const std::string s3_proto = "S3_PROTO";
const std::string s3_stsdate = "S3_STSDATE";
const std::string s3_max_upload_size = "S3_MAX_UPLOAD_SIZE";
const std::string s3_mpu_chunk = "S3_MPU_CHUNK";
const std::string s3_mpu_threads = "S3_MPU_THREADS";
const std::string s3_enable_md5 = "S3_ENABLE_MD5";
const std::string s3_server_encrypt = "S3_SERVER_ENCRYPT";
const std::string s3_signature_version = "S3_SIGNATURE_VERSION";
const std::string s3_region_name = "S3_REGIONNAME";

// For s3PutCopyFile to identify the real source type
typedef enum { S3_PUTFILE, S3_COPYOBJECT } s3_putcopy;

size_t g_retry_count = 10;
size_t g_retry_wait = 1;



void ds3_log_error(const ds3_error* error) {
    rodsLog(LOG_ERROR, "DS3_ERROR_MESSAGE: %s\n", error->message->value);
    if (error->error != NULL) {
        rodsLog(LOG_ERROR,"DS3_HTTP_ERROR_CODE: %lu\n", (long unsigned int)error->error->http_error_code);
        rodsLog(LOG_ERROR,"DS3_CODE: %s\n", error->error->code->value);
        rodsLog(LOG_ERROR,"DS3_STATUS_MESSAGE: %s\n", error->error->message->value);
        rodsLog(LOG_ERROR,"DS3_RESOURCE: %s\n", error->error->resource->value);
    }
}


void ds3_handle_error(ds3_error* error) {
    if (error != NULL) {
        ds3_log_error(error);
        ds3_error_free(error);
        //exit(1);
    }
}



#ifdef ERROR_INJECT
// Callback error injection
// If defined(ERROR_INJECT), then the specified pread/write below will fail.
// Only 1 failure happens, but this is OK since for every irods command we
// actually restart from 0 since the .SO is reloaded
// Pairing this with LIBS3 error injection will exercise the error recovery
// and retry code paths.
static boost::mutex g_error_mutex;
static long g_werr = 0; // counter
static long g_rerr = 0; // counter
static long g_merr = 0; // counter
static const long g_werr_idx = 4; // Which # pwrite to fail
static const long g_rerr_idx = 4; // Which # pread to fail
static const long g_merr_idx = 4; // Which part of Multipart Finish XML to fail
#endif

//////////////////////////////////////////////////////////////////////
// s3 specific functionality
static bool S3Initialized = false; // so we only initialize the s3 library once
static std::vector<char *> g_hostname;
static int g_hostnameIdx = 0;
static boost::mutex g_hostnameIdxLock;

// Sleep for *at least* the given time, plus some up to 1s additional
// The random addition ensures that threads don't all cluster up and retry
// at the same time (dogpile effect)
static void s3_sleep(
    int _s,
    int _ms ) {
    // We're the only user of libc rand(), so if we mutex around calls we can
    // use the thread-unsafe rand() safely and randomly...if this is changed
    // in the future, need to use rand_r and init a static seed in this function
    static boost::mutex randMutex;
    randMutex.lock();
    int random = rand();
    randMutex.unlock();
    int addl = (int)(((double)random / (double)RAND_MAX) * 1000.0); // Add up to 1000 ms (1 sec)
    useconds_t us = ( _s * 1000000 ) + ( (_ms + addl) * 1000 );
    usleep( us );
}

// Returns timestamp in usec for delta-t comparisons
static unsigned long long usNow() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long long us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
    return us;
}

// Return a malloc()'d C string containing the ASCII MD5 signature of the file
// from start through length bytes, using pread to not affect file pointers.
// The returned string needs to be free()d by the caller
static char *s3CalcMD5( int fd, off_t start, off_t length )
{
    char *buff; // Temp buff to do MD5 calc on
    unsigned char md5_bin[MD5_DIGEST_LENGTH];
    MD5_CTX md5_ctx;
    long read;

    buff = (char *)malloc( 1024*1024 ); // 1MB chunk reads
    if ( buff == NULL ) {
        rodsLog( LOG_ERROR, "Out of memory in S3 MD5 calculation, MD5 checksum will NOT be used for upload." );
        return NULL;
    }

    MD5_Init( &md5_ctx );
    for ( read=0; (read + 1024*1024) < length; read += 1024*1024 ) {
        long ret = pread( fd, buff, 1024*1024, start );
        if ( ret != 1024*1024 ) {
            rodsLog( LOG_ERROR, "Error during MD5 pread of file, checksum will NOT be used for upload." );
            free( buff );
            return NULL;
        }
        MD5_Update( &md5_ctx, buff, 1024*1024 );
        start += 1024 * 1024;
    }
    // Partial read for the last bit
    long ret = pread( fd, buff, length-read, start );
    if ( ret != length-read ) {
        rodsLog( LOG_ERROR, "Error during MD5 pread of file, checksum will NOT be used for upload." );
        free( buff );
        return NULL;
    }
    MD5_Update( &md5_ctx, buff, length-read );
    MD5_Final( md5_bin, &md5_ctx );
    free( buff );

    // Now we need to do BASE64 encoding of the MD5_BIN
    BIO *bmem, *b64;
    BUF_MEM *bptr;

    b64 = BIO_new(BIO_f_base64());
    bmem = BIO_new(BIO_s_mem());
    if ( (b64 == NULL) || (bmem == NULL) ) {
        rodsLog( LOG_ERROR, "Error during Base64 allocation, checksum will NOT be used for upload." );
        return NULL;
    }

    b64 = BIO_push(b64, bmem);
    BIO_write(b64, md5_bin, MD5_DIGEST_LENGTH);
    if (BIO_flush(b64) != 1) {
        rodsLog( LOG_ERROR, "Error during Base64 computation, checksum will NOT be used for upload." );
        return NULL;
    }
    BIO_get_mem_ptr(b64, &bptr);

    char *md5_b64 = (char*)malloc( bptr->length );
    if ( md5_b64 == NULL ) {
        rodsLog( LOG_ERROR, "Error during MD5 allocation, checksum will NOT be used for upload." );
        return NULL;
    }
    memcpy( md5_b64, bptr->data, bptr->length-1 );
    md5_b64[bptr->length-1] = 0;  // 0-terminate the string, not done by BIO_*
    BIO_free_all(b64);

    return md5_b64;
}
// Increment through all specified hostnames in the list, locking in the case
// where we may be multithreaded
static const char *s3GetHostname()
{
    if (g_hostname.empty())
        return NULL; // Short-circuit default case
    g_hostnameIdxLock.lock();
    char *ret = g_hostname[g_hostnameIdx];
    g_hostnameIdx = (g_hostnameIdx + 1) % g_hostname.size();
    g_hostnameIdxLock.unlock();
    return ret;
}


// Callbacks for S3
static void StoreAndLogStatus (
    S3Status status,
    const S3ErrorDetails *error,
    const char *function,
    const S3BucketContext *pCtx,
    S3Status *pStatus )
{
    int i;

    *pStatus = status;
    if( status != S3StatusOK ) {
        rodsLog( LOG_ERROR, "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
        rodsLog( LOG_ERROR, "    S3Host: %s", pCtx->hostName );
    }
    if (status != S3StatusOK && function )
        rodsLog( LOG_ERROR, "  Function: %s\n", function );
    if (error && error->message)
        rodsLog( LOG_ERROR, "  Message: %s\n", error->message);
    if (error && error->resource)
        rodsLog( LOG_ERROR, "  Resource: %s\n", error->resource);
    if (error && error->furtherDetails)
        rodsLog( LOG_ERROR, "  Further Details: %s\n", error->furtherDetails);
    if (error && error->extraDetailsCount) {
        rodsLog( LOG_ERROR, "%s", "  Extra Details:\n");

        for (i = 0; i < error->extraDetailsCount; i++) {
            rodsLog( LOG_ERROR, "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
        }
    }
}

static void responseCompleteCallback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
}

static S3Status responsePropertiesCallback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

static S3Status getObjectDataCallback(
    int bufferSize,
    const char *buffer,
    void *callbackData)
{
    callback_data_t *cb = (callback_data_t *)callbackData;

    irods::error result = ASSERT_ERROR(bufferSize != 0 && buffer != NULL && callbackData != NULL,
                                       SYS_INVALID_INPUT_PARAM, "Invalid input parameter.");
    if(!result.ok()) {
        irods::log(result);
    }

    ssize_t wrote = pwrite(cb->fd, buffer, bufferSize, cb->offset);
    if (wrote>0) cb->offset += wrote;

#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_werr++;
    if (g_werr == g_werr_idx) {
        rodsLog(LOG_ERROR, "Injecting a PWRITE error during S3 callback");
        g_error_mutex.unlock();
        return S3StatusAbortedByCallback;
    }
    g_error_mutex.unlock();
#endif

    return ((wrote < (ssize_t) bufferSize) ?
            S3StatusAbortedByCallback : S3StatusOK);
}

static int putObjectDataCallback(
    int bufferSize,
    char *buffer,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t *) callbackData;
    long ret = 0;

    if (data->contentLength) {
        int length = ((data->contentLength > (unsigned) bufferSize) ?
                      (unsigned) bufferSize : data->contentLength);
        ret = pread(data->fd, buffer, length, data->offset);
    }
    data->contentLength -= ret;
    data->offset += ret;

#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_rerr++;
    if (g_rerr == g_rerr_idx) {
        rodsLog(LOG_ERROR, "Injecting pread error in S3 callback");
        ret = -1;
    }
    g_error_mutex.unlock();
#endif

    return (long)ret;
}

S3Status listBucketCallback(
    int isTruncated,
    const char *nextMarker,
    int contentsCount,
    const S3ListBucketContent *contents,
    int commonPrefixesCount,
    const char **commonPrefixes,
    void *callbackData)
{
    callback_data_t *data =
        (callback_data_t *) callbackData;

    if (contentsCount <= 0) {
        data->keyCount = 0;
        return S3StatusOK;
    } else if (contentsCount > 1) {
        rodsLog (LOG_ERROR,
                 "listBucketCallback: contentsCount %d > 1 for %s",
                 contentsCount, contents->key);
    }
    data->keyCount = contentsCount;
    data->s3Stat.size = contents->size;
    data->s3Stat.lastModified = contents->lastModified;
    rstrcpy (data->s3Stat.key, (char *) contents->key, MAX_NAME_LEN);

    return S3StatusOK;
}

// Utility functions
irods::error parseS3Path (
    const std::string& _s3ObjName,
    std::string& _bucket,
    std::string& _key)
{
    irods::error result = SUCCESS();
    size_t start_pos = 0;
    size_t slash_pos = 0;
    slash_pos = _s3ObjName.find_first_of("/");
    // skip a leading slash
    if(slash_pos == 0) {
        start_pos = 1;
        slash_pos = _s3ObjName.find_first_of("/", 1);
    }
    // have to have at least one slash to separate bucket from key
    if((result = ASSERT_ERROR(slash_pos != std::string::npos, SYS_INVALID_FILE_PATH, "Problem parsing \"%s\".",
                              _s3ObjName.c_str())).ok()) {
        _bucket = _s3ObjName.substr(start_pos, slash_pos - start_pos);
        _key = _s3ObjName.substr(slash_pos + 1);
    }
    return result;
}

// Get S3 Signature version from plugin property map
static S3SignatureVersion s3GetSignatureVersion (irods::plugin_property_map& _prop_map)
{
    std::string version_str;

    irods::error ret = _prop_map.get< std::string >(s3_signature_version, version_str);
    if (ret.ok()) {
        if (version_str == "4" || boost::iequals(version_str, "V4")) {
            return S3SignatureV4;
        }
    }

    return S3SignatureV2; // default
}

irods::error readS3AuthInfo (
    const std::string& _filename,
    std::string& _rtn_key_id,
    std::string& _rtn_access_key)
{
    irods::error result = SUCCESS();
    irods::error ret;
    FILE *fptr;
    char inbuf[MAX_NAME_LEN];
    int lineLen, bytesCopied;
    int linecnt = 0;
    char access_key_id[S3_MAX_KEY_SIZE];
    char secret_access_key[S3_MAX_KEY_SIZE];

    fptr = fopen (_filename.c_str(), "r");

    if ((result = ASSERT_ERROR(fptr != NULL, SYS_CONFIG_FILE_ERR, "Failed to open S3 auth file: \"%s\", errno = \"%s\".",
                               _filename.c_str(), strerror(errno))).ok()) {
        while ((lineLen = getLine (fptr, inbuf, MAX_NAME_LEN)) > 0) {
            char *inPtr = inbuf;
            if (linecnt == 0) {
                while ((bytesCopied = getStrInBuf (&inPtr, access_key_id, &lineLen, S3_MAX_KEY_SIZE)) > 0) {
                    linecnt ++;
                    break;
                }
            } else if (linecnt == 1) {
                while ((bytesCopied = getStrInBuf (&inPtr, secret_access_key, &lineLen, S3_MAX_KEY_SIZE)) > 0) {
                    linecnt ++;
                    break;
                }
            }
        }
        if ((result = ASSERT_ERROR(linecnt == 2, SYS_CONFIG_FILE_ERR, "Read %d lines in the auth file. Expected 2.",
                                   linecnt)).ok())  {
            _rtn_key_id = access_key_id;
            _rtn_access_key = secret_access_key;
        }
        return result;
    }

    result = ERROR( SYS_CONFIG_FILE_ERR, "Unknown error in authorization file." );
    return result;
}

/// @brief Retrieves the auth info from either the environment or the resource's specified auth file and set the appropriate
/// fields in the property map
irods::error s3ReadAuthInfo(
    irods::plugin_property_map& _prop_map)
{
    irods::error result = SUCCESS();
    irods::error ret;
    char* tmpPtr;
    std::string key_id;
    std::string access_key;

    if ((tmpPtr = getenv(s3_key_id.c_str())) != NULL) {
        key_id = tmpPtr;
        if ((tmpPtr = getenv(s3_access_key.c_str())) != NULL) {
            access_key = tmpPtr;
        }
    } else {
        std::string auth_file;
        ret = _prop_map.get<std::string>(s3_auth_file, auth_file);
        if((result = ASSERT_PASS(ret, "Failed to retrieve S3 auth filename property.")).ok()) {
            ret = readS3AuthInfo(auth_file, key_id, access_key);
            if ((result = ASSERT_PASS(ret, "Failed reading the authorization credentials file.")).ok()) {
                ret = _prop_map.set<std::string>(s3_key_id, key_id);
                if((result = ASSERT_PASS(ret, "Failed to set the \"%s\" property.", s3_key_id.c_str())).ok()) {
                    ret = _prop_map.set<std::string>(s3_access_key, access_key);
                    result = ASSERT_PASS(ret, "Failed to set the \"%s\" property.", s3_access_key.c_str());
                }
            }
        }
    }
    return result;
}

irods::error s3Init (
    irods::plugin_property_map& _prop_map )
{
    irods::error result = SUCCESS();

    if (!S3Initialized) {
        // First, parse the default hostname (if present) into a list of
        // hostnames separated on the definition line by commas (,)
        std::string hostname_list;
        irods::error ret;
        ret = _prop_map.get< std::string >(
            s3_default_hostname,
            hostname_list );
        if( !ret.ok() ) {
            // ok to fail
            g_hostname.push_back(strdup(S3_DEFAULT_HOSTNAME)); // Default to Amazon
        } else {
            std::stringstream ss(hostname_list);
            std::string item;
            while (std::getline(ss, item, ',')) {
                g_hostname.push_back(strdup(item.c_str()));
            }
            // Because each resource operation is a new instance, randomize the starting
            // hostname offset so we don't always hit the first in the list between different
            // operations.
            srand(time(NULL));
            g_hostnameIdx = rand() % g_hostname.size();
        }

        std::string retry_count_str;
        ret = _prop_map.get< std::string >(
            s3_retry_count,
            retry_count_str );
        if( ret.ok() ) {
            try {
                g_retry_count = boost::lexical_cast<int>( retry_count_str );
            } catch ( const boost::bad_lexical_cast& ) {
                rodsLog(
                    LOG_ERROR,
                    "failed to cast retry count [%s] to an int",
                    retry_count_str.c_str() );
            }
        }

        std::string wait_time_str;
        ret = _prop_map.get< std::string >(
            s3_wait_time_sec,
            wait_time_str );
        if( ret.ok() ) {
            try {
                g_retry_wait = boost::lexical_cast<int>( wait_time_str );
            } catch ( const boost::bad_lexical_cast& ) {
                rodsLog(
                    LOG_ERROR,
                    "failed to cast wait time [%s] to an int",
                    wait_time_str.c_str() );
            }
        }

        size_t ctr = 0;
        while( ctr < g_retry_count ) {
            int status = 0;
            int flags = S3_INIT_ALL;
            S3SignatureVersion signature_version = s3GetSignatureVersion(_prop_map);

            if (signature_version == S3SignatureV4) {
                flags |= S3_INIT_SIGNATURE_V4;
            }

            const char* host_name = s3GetHostname();  // Iterate through on each try
            status = S3_initialize( "s3", flags, host_name );

            std::stringstream msg;
            if( status >= 0 ) {
                msg << " - \"";
                msg << S3_get_status_name((S3Status)status);
                msg << "\"";
            }

            result = ASSERT_ERROR(status == S3StatusOK, status, "Error initializing the S3 library. Status = %d.",
                                  status, msg.str().c_str());
            if( result.ok() ) {

                // If using V4 we also need to set the S3 region name
                if (signature_version == S3SignatureV4) {
                    std::string region_name = "us-east-1";

                    // Get S3 region name from plugin property map
                    if (!_prop_map.get< std::string >(s3_region_name, region_name ).ok()) {
                        rodsLog( LOG_ERROR, "Failed to retrieve S3 region name from resource plugin properties, using 'us-east-1'");
                    }

                    S3Status status = S3_set_region_name(region_name.c_str());
                    if (status != S3StatusOK) {
                        rodsLog(LOG_ERROR, "failed to set region name to %s: %s", region_name.c_str(), S3_get_status_name(status));
                        return ERROR(S3_INIT_ERROR, "S3_set_region_name() failed.");
                    }
                }

                break;
            }

            ctr++;

            s3_sleep( g_retry_wait, 0 );

            rodsLog(
                LOG_NOTICE,
                "s3Init - Error in connection, retry count %d",
                ctr );

        } // while

        if( result.ok() ) {
            S3Initialized = true;
        }

    } // if !init

    return result;
}

static S3Protocol s3GetProto( irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string proto_str;
    ret = _prop_map.get< std::string >(
        s3_proto,
        proto_str );
    if (!ret.ok()) { // Default to original behavior
        return S3ProtocolHTTPS;
    }
    if (!strcasecmp(proto_str.c_str(), "http")) {
        return S3ProtocolHTTP;
    }
    return S3ProtocolHTTPS;
}

// returns the upper limit of the MPU chunk size parameter, in megabytes
// used for validating the value of S3_MPU_CHUNK
static long s3GetMaxUploadSize (irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string max_size_str;   // max size from context string, in MB

    ret = _prop_map.get<std::string>(s3_max_upload_size, max_size_str);
    if (ret.ok()) {
        // should be between 5MB and 5TB
        long max_megs = atol(max_size_str.c_str());
        if ( max_megs >= 5 && max_megs <= 5L * 1024 * 1024 ) {
            return max_megs;
        }
    }
    return 5L * 1024;    // default to 5GB
}

// returns the chunk size for multipart upload, in bytes
static long s3GetMPUChunksize (irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string chunk_str;
    long bytes = 64L * 1024 * 1024; // default to safe value
    ret = _prop_map.get< std::string >(s3_mpu_chunk, chunk_str );

    if (ret.ok()) {
        // AWS S3 allows chunk sizes from 5MB to 5GB.
        // Other S3 appliances may have a different upper limit.
        long megs = atol(chunk_str.c_str());
        if ( megs >= 5 && megs <= s3GetMaxUploadSize(_prop_map) )
            bytes = megs * 1024 * 1024;
    }
    return bytes;
}

static ssize_t s3GetMPUThreads (
    irods::plugin_property_map& _prop_map )
{
    irods::error ret;
    std::string threads_str;
    int threads = 10; // 10 upload threads by default
    ret = _prop_map.get< std::string >(
        s3_mpu_threads,
        threads_str );
    if (ret.ok()) {
        int parse = atol(threads_str.c_str());
        if ( (parse >= 1) && (parse <= 100) )
            threads = parse;
    }
    return threads;
}

static bool s3GetEnableMD5 (
    irods::plugin_property_map& _prop_map )
{
    irods::error ret;
    std::string enable_str;
    bool enable = false;

    // Don't send md5 digest when using signature V4
    if (s3GetSignatureVersion(_prop_map) == S3SignatureV4) {
        return false;
    }

    ret = _prop_map.get< std::string >(
        s3_enable_md5,
        enable_str );
    if (ret.ok()) {
        // Only 0 = no, 1 = yes.  Adding in strings would require localization I think
        int parse = atol(enable_str.c_str());
        if (parse != 0)
            enable = true;
    }
    return enable;
}

static bool s3GetServerEncrypt (
    irods::plugin_property_map& _prop_map )
{
    irods::error ret;
    std::string enable_str;
    bool enable = false;

    ret = _prop_map.get< std::string >(
        s3_server_encrypt,
        enable_str );
    if (ret.ok()) {
        // Only 0 = no, 1 = yes.  Adding in strings would require localization I think
        int parse = atol(enable_str.c_str());
        if (parse != 0)
            enable = true;
    }
    return enable;
}

static boost::mutex g_mrdLock; // Multirange download has a mutex-protected global work queue
static volatile int g_mrdNext = 0;
static int g_mrdLast = -1;
static multirange_data_t *g_mrdData = NULL;
static const char *g_mrdKey = NULL;
static irods::error g_mrdResult;  // Last thread reporting an error wins, mutex protected

static S3Status mrdRangeGetDataCB (
    int bufferSize,
    const char *buffer,
    void *callbackData)
{
    multirange_data_t *data = (multirange_data_t*)callbackData;
    return getObjectDataCallback( bufferSize, buffer, &(data->get_object_data) );
}

static S3Status mrdRangeRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    // Don't need to do anything here
    return S3StatusOK;
}

static void mrdRangeRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    multirange_data_t *data = (multirange_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}


static void mrdWorkerThread (
    void *param )
{
    S3BucketContext bucketContext = *((S3BucketContext*)param);
    irods::error result;
    std::stringstream msg;
    S3GetObjectHandler getObjectHandler = { {mrdRangeRespPropCB, mrdRangeRespCompCB }, mrdRangeGetDataCB };

    /* Will break out when no work detected */
    while (1) {
        // Pointer is probably safe w/o mutex, but JIK...
        g_mrdLock.lock();
        bool ok = (g_mrdResult.ok());
        g_mrdLock.unlock();
        if (!ok) break;

        int seq;
        g_mrdLock.lock();
        if (g_mrdNext >= g_mrdLast) {
            g_mrdLock.unlock();
            break;
        }
        seq = g_mrdNext + 1;
        g_mrdNext++;
        g_mrdLock.unlock();

        size_t retry_cnt = 0;
        multirange_data_t rangeData;
        do {
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            rangeData = g_mrdData[seq-1];
            rangeData.pCtx = &bucketContext;

            msg.str( std::string() ); // Clear
            msg << "Multirange:  Start range " << (int)seq << ", key \"" << g_mrdKey << "\", offset "
                << (long)rangeData.get_object_data.offset << ", len " << (int)rangeData.get_object_data.contentLength;
            rodsLog( LOG_DEBUG, msg.str().c_str() );

            unsigned long long usStart = usNow();
            bucketContext.hostName = s3GetHostname(); // Safe to do, this is a local copy of the data structure
            S3_get_object( &bucketContext, g_mrdKey, NULL, rangeData.get_object_data.offset,
                           rangeData.get_object_data.contentLength, 0, &getObjectHandler, &rangeData );
            unsigned long long usEnd = usNow();
            double bw = (g_mrdData[seq-1].get_object_data.contentLength / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
            msg << " -- END -- BW=" << bw << " MB/s";
            rodsLog( LOG_DEBUG, msg.str().c_str() );
            if (rangeData.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
        } while ((rangeData.status != S3StatusOK) && S3_status_is_retryable(rangeData.status) && (++retry_cnt < g_retry_count));
        if (rangeData.status != S3StatusOK) {
            msg.str( std::string() ); // Clear
            msg << __FUNCTION__ << " - Error getting the S3 object: \"" << g_mrdKey << "\" range " << seq;
            if (rangeData.status >= 0) {
                msg << " - \"" << S3_get_status_name( rangeData.status ) << "\"";
            }
            result = ERROR( S3_GET_ERROR, msg.str() );
            rodsLog( LOG_ERROR, msg.str().c_str() );
            g_mrdLock.lock();
            g_mrdResult = result;
            g_mrdLock.unlock();
        }
    }
}


static S3STSDate s3GetSTSDate( irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string stsdate_str;
    ret = _prop_map.get< std::string >(
        s3_stsdate,
        stsdate_str );
    if (!ret.ok()) { // Default to original behavior
        return S3STSAmzOnly;
    }
    if (!strcasecmp(stsdate_str.c_str(), "date")) {
        return S3STSDateOnly;
    }
    if (!strcasecmp(stsdate_str.c_str(), "both")) {
        return S3STSAmzAndDate;
    }
    return S3STSAmzOnly;
}

irods::error s3GetFile(
    const std::string& _filename,
    const std::string& _s3ObjName,
    rodsLong_t _fileSize,
    const std::string& _key_id,
    const std::string& _access_key,
    irods::plugin_property_map& _prop_map )
{
    irods::error result = SUCCESS();
    irods::error ret;
    int cache_fd = -1;
    std::string bucket;
    std::string key;
    ret = parseS3Path(_s3ObjName, bucket, key);
    if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                             _s3ObjName.c_str())).ok()) {
        ret = s3Init( _prop_map );
        if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

            cache_fd = open(_filename.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
            if((result = ASSERT_ERROR(cache_fd != -1, UNIX_FILE_OPEN_ERR, "Failed to open the cache file: \"%s\".",
                                      _filename.c_str())).ok()) {

                std::string default_hostname;
                irods::error ret = _prop_map.get< std::string >(
                    s3_default_hostname,
                    default_hostname );
                if( !ret.ok() ) {
                    return PASS(ret);
                }

                callback_data_t data;
                S3BucketContext bucketContext;

                bzero (&bucketContext, sizeof (bucketContext));
                bucketContext.bucketName = bucket.c_str();
                bucketContext.protocol = s3GetProto(_prop_map);
                bucketContext.stsDate = s3GetSTSDate(_prop_map);
                bucketContext.uriStyle = S3UriStylePath;
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();

                long chunksize = s3GetMPUChunksize( _prop_map );

                if ( _fileSize < chunksize ) {
                    S3GetObjectHandler getObjectHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &getObjectDataCallback
                    };

                    size_t retry_cnt = 0;
                    do {
                        bzero (&data, sizeof (data));
                        data.fd = cache_fd;
                        data.contentLength = data.originalContentLength = _fileSize;
                        unsigned long long usStart = usNow();
                        bucketContext.hostName = s3GetHostname();  // Iterate different one on each try
                        data.pCtx = &bucketContext;
                        S3_get_object (&bucketContext, key.c_str(), NULL, 0, _fileSize, 0, &getObjectHandler, &data);
                        unsigned long long usEnd = usNow();
                        double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                        rodsLog( LOG_DEBUG, "GETBW=%lf", bw);
                        if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                    } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );
                    if (data.status != S3StatusOK) {
                        std::stringstream msg;
                        msg << __FUNCTION__ << " - Error fetching the S3 object: \"" << _s3ObjName << "\"";
                        if (data.status >= 0) {
                            msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                        }
                        result = ERROR(S3_GET_ERROR, msg.str());
                    }
                } else {
                    std::stringstream msg;

                    // Only the FD part of this will be constant
                    bzero (&data, sizeof (data));
                    data.fd = cache_fd;
                    data.contentLength = data.originalContentLength = _fileSize;

                    // Multirange get
                    g_mrdResult = SUCCESS();

                    long seq;
                    long totalSeq = (data.contentLength + chunksize - 1) / chunksize;

                    multirange_data_t rangeData;
                    int rangeLength = 0;

                    g_mrdData = (multirange_data_t*)calloc(totalSeq, sizeof(multirange_data_t));
                    if (!g_mrdData) {
                        const char *msg = "Out of memory error in S3 multirange g_mrdData allocation.";
                        rodsLog( LOG_ERROR, msg );
                        result = ERROR( SYS_MALLOC_ERR, msg );
                        return result;
                    }

                    g_mrdNext = 0;
                    g_mrdLast = totalSeq;
                    g_mrdKey = key.c_str();
                    for(seq = 0; seq < totalSeq ; seq ++) {
                        memset(&rangeData, 0, sizeof(rangeData));
                        rangeData.seq = seq;
                        rangeData.get_object_data = data;
                        rangeLength = (data.contentLength > chunksize)?chunksize:data.contentLength;
                        rangeData.get_object_data.contentLength = rangeLength;
                        rangeData.get_object_data.offset = seq * chunksize;
                        g_mrdData[seq] = rangeData;
                        data.contentLength -= rangeLength;
                    }

                    // Make the worker threads and start
                    int nThreads = s3GetMPUThreads(_prop_map);

                    unsigned long long usStart = usNow();
                    std::list<boost::thread*> threads;
                    for (int thr_id=0; thr_id<nThreads; thr_id++) {
                        boost::thread *thisThread = new boost::thread(mrdWorkerThread, &bucketContext);
                        threads.push_back(thisThread);
                    }

                    // And wait for them to finish...
                    while (!threads.empty()) {
                        boost::thread *thisThread = threads.front();
                        thisThread->join();
                        delete thisThread;
                        threads.pop_front();
                    }
                    unsigned long long usEnd = usNow();
                    double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                    rodsLog( LOG_DEBUG, "MultirangeBW=%lf", bw);

                    if (!g_mrdResult.ok()) {
                        // Someone aborted after we started, delete the partial object on S3
                        rodsLog(LOG_ERROR, "Cancelling multipart download");
                        // 0-length the file, it's garbage
                        if (ftruncate( cache_fd, 0 ))
                            rodsLog(LOG_ERROR, "Unable to 0-length the result file");
                        result = g_mrdResult;
                    }
                    // Clean up memory
                    if (g_mrdData) free(g_mrdData);
                }
                close(cache_fd);
            }
        }
    }
    return result;
}

static boost::mutex g_mpuLock; // Multipart upload has a mutex-protected global work queue
static volatile int g_mpuNext = 0;
static int g_mpuLast = -1;
static multipart_data_t *g_mpuData = NULL;
static char *g_mpuUploadId = NULL;
static const char *g_mpuKey = NULL;
static irods::error g_mpuResult;  // Last thread error written wins, mutex protected

/******************* Multipart Initialization Callbacks *****************************/

/* Captures the upload_id returned and stores it away in our data structure */
static S3Status mpuInitXmlCB (
    const char* upload_id,
    void *callbackData )
{
    upload_manager_t *manager = (upload_manager_t *)callbackData;
    manager->upload_id = strdup(upload_id);
    return S3StatusOK;
}

static S3Status mpuInitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

static void mpuInitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}


/******************* Multipart Put Callbacks *****************************/

/* Upload data from the part, use the plain callback_data reader */
static int mpuPartPutDataCB (
    int bufferSize,
    char *buffer,
    void *callbackData)
{
    return putObjectDataCallback( bufferSize, buffer, &((multipart_data_t*)callbackData)->put_object_data );
}

static S3Status mpuPartRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;

    int seq = data->seq;
    const char *etag = properties->eTag;
    data->manager->etags[seq - 1] = strdup(etag);

    return S3StatusOK;
}

static void mpuPartRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}

/******************* Multipart Commit Callbacks *****************************/
/* Uploading the multipart completion XML from our buffer */
static int mpuCommitXmlCB (
    int bufferSize,
    char *buffer,
    void *callbackData )
{
    upload_manager_t *manager = (upload_manager_t *)callbackData;
    long ret = 0;
    if (manager->remaining) {
        int toRead = ((manager->remaining > bufferSize) ?
                      bufferSize : manager->remaining);
        memcpy(buffer, manager->xml+manager->offset, toRead);
        ret = toRead;
    }
    manager->remaining -= ret;
    manager->offset += ret;

#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_merr++;
    if (g_merr == g_merr_idx) {
        rodsLog(LOG_ERROR, "Injecting a XML upload error during S3 callback");
        g_error_mutex.unlock();
        ret = -1;
    }
    g_error_mutex.unlock();
#endif

    return (int)ret;
}

static S3Status mpuCommitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

static void mpuCommitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}

static S3Status mpuCancelRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

// S3_abort_multipart_upload() does not allow a callbackData parameter, so pass the
// final operation status using this global.
static S3Status g_mpuCancelRespCompCB_status = S3StatusOK;
static S3BucketContext *g_mpuCancelRespCompCB_pCtx = NULL;
static void mpuCancelRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    S3Status *pStatus = (S3Status*)&g_mpuCancelRespCompCB_status;
    StoreAndLogStatus( status, error, __FUNCTION__, g_mpuCancelRespCompCB_pCtx, pStatus );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}

static void mpuCancel( S3BucketContext *bucketContext, const char *key, const char *upload_id )
{
    S3AbortMultipartUploadHandler abortHandler = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };
    std::stringstream msg;
    S3Status status;

    msg << "Cancelling multipart upload: key=\"" << key << "\", upload_id=\"" << upload_id << "\"";
    rodsLog( LOG_ERROR, msg.str().c_str() );
    g_mpuCancelRespCompCB_status = S3StatusOK;
    g_mpuCancelRespCompCB_pCtx = bucketContext;
    S3_abort_multipart_upload(bucketContext, key, upload_id, &abortHandler);
    status = g_mpuCancelRespCompCB_status;
    if (status != S3StatusOK) {
        msg.str( std::string() ); // Clear
        msg << __FUNCTION__ << " - Error cancelling the multipart upload of S3 object: \"" << key << "\"";
        if (status >= 0) {
            msg << " - \"" << S3_get_status_name(status) << "\"";
        }
        rodsLog( LOG_ERROR, msg.str().c_str() );
    }
}


/* Multipart worker thread, grabs a job from the queue and uploads it */
static void mpuWorkerThread (
    void *param )
{
    S3BucketContext bucketContext = *((S3BucketContext*)param);
    irods::error result;
    std::stringstream msg;
    S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

    /* Will break out when no work detected */
    while (1) {
        // Pointer is probably safe w/o mutex, but JIK...
        g_mpuLock.lock();
        bool ok = (g_mpuResult.ok());
        g_mpuLock.unlock();
        if (!ok) break;

        int seq;
        g_mpuLock.lock();
        if (g_mpuNext >= g_mpuLast) {
            g_mpuLock.unlock();
            break;
        }
        seq = g_mpuNext + 1;
        g_mpuNext++;
        g_mpuLock.unlock();

        multipart_data_t partData;
        size_t retry_cnt = 0;
        do {
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            partData = g_mpuData[seq-1];
            partData.put_object_data.pCtx = &bucketContext;

            msg.str( std::string() ); // Clear
            msg << "Multipart:  Start part " << (int)seq << ", key \"" << g_mpuKey << "\", uploadid \"" << g_mpuUploadId << "\", offset "
                << (long)partData.put_object_data.offset << ", len " << (int)partData.put_object_data.contentLength;
            rodsLog( LOG_DEBUG, msg.str().c_str() );

            S3PutProperties *putProps = NULL;
            putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
            if ( putProps && partData.enable_md5 )
                putProps->md5 = s3CalcMD5( partData.put_object_data.fd, partData.put_object_data.offset, partData.put_object_data.contentLength );
            if ( putProps && partData.server_encrypt )
                putProps->useServerSideEncryption = true;
            putProps->expires = -1;
            unsigned long long usStart = usNow();
            bucketContext.hostName = s3GetHostname(); // Safe to do, this is a local copy of the data structure
            if (partData.mode == S3_COPYOBJECT) {
                unsigned long long startOffset = partData.put_object_data.offset;
                unsigned long long count = partData.put_object_data.contentLength;
                S3ResponseHandler copyResponseHandler = {mpuInitRespPropCB /*Do nothing*/, mpuPartRespCompCB};
                int64_t lastModified;
                // The default copy callback tries to set this for us, need to allocate here
                partData.manager->etags[seq-1] = (char *)malloc(512); // TBD - magic #!  Is there a max etag defined?
                S3_copy_object_range(partData.pSrcCtx, partData.srcKey, bucketContext.bucketName, g_mpuKey,
                                     seq, g_mpuUploadId,
                                     startOffset, count,
                                     putProps,
                                     &lastModified, 512 /*TBD - magic # */, partData.manager->etags[seq-1], 0,
                                     &copyResponseHandler, &partData);
            } else {
                S3_upload_part(&bucketContext, g_mpuKey, putProps, &putObjectHandler, seq, g_mpuUploadId, partData.put_object_data.contentLength, 0, &partData);
            }
            unsigned long long usEnd = usNow();
            double bw = (g_mpuData[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / ( (usEnd - usStart) / 1000000.0 );
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            msg << " -- END -- BW=" << bw << " MB/s";
            rodsLog( LOG_DEBUG, msg.str().c_str() );
            if (partData.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
        } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt < g_retry_count));
        if (partData.status != S3StatusOK) {
            msg.str( std::string() ); // Clear
            msg << __FUNCTION__ << " - Error putting the S3 object: \"" << g_mpuKey << "\"" << " part " << seq;
            if(partData.status >= 0) {
                msg << " - \"" << S3_get_status_name(partData.status) << "\"";
            }
            result = ERROR( S3_PUT_ERROR, msg.str() );
            rodsLog( LOG_ERROR, msg.str().c_str() );
            g_mpuResult = result;
        }
    }
}


irods::error ds3_bulk_put(
        const std::vector<std::string>& _file_names,
        const std::vector<std::string>& _object_names,
        //rodsLong_t _fileSize,
        const std::string& _key_id,
        const std::string& _access_key,
        irods::plugin_property_map& _prop_map)
{
    // Get a client instance which uses the environment variables to get the endpoint and credentials
    ds3_client* client;
    ds3_request* request;
    ds3_error* error;
    ds3_bulk_object_list_response* obj_list;
    ds3_master_object_list_response* chunks_response;
    ds3_bulk_object_response* current_obj_to_put;
    uint64_t chunk_count, current_chunk_count = 0;
    uint64_t chunk_index, obj_index;
    FILE* obj_file;



    irods::error result = SUCCESS();

    return result;

}


irods::error s3PutCopyFile(
    const s3_putcopy _mode,
    const std::string& _filename,
    const std::string& _s3ObjName,
    rodsLong_t _fileSize,
    const std::string& _key_id,
    const std::string& _access_key,
    irods::plugin_property_map& _prop_map )
{
    irods::error result = SUCCESS();
    irods::error ret;
    int cache_fd = -1;
    std::string bucket;
    std::string key;
    std::string srcBucket;
    std::string srcKey;
    int err_status = 0;
    long chunksize = s3GetMPUChunksize( _prop_map );
    size_t retry_cnt    = 0;
    bool enable_md5 = s3GetEnableMD5 ( _prop_map );
    //bool server_encrypt = s3GetServerEncrypt ( _prop_map );
    std::stringstream msg;

    ret = parseS3Path(_s3ObjName, bucket, key);
    if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                             _s3ObjName.c_str())).ok()) {

        ret = s3Init( _prop_map );
        if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

            if (_mode == S3_PUTFILE) {
                cache_fd = open(_filename.c_str(), O_RDONLY);
                err_status = UNIX_FILE_OPEN_ERR - errno;
            } else if (_mode == S3_COPYOBJECT && _fileSize > s3GetMPUChunksize( _prop_map )) {
                // Multipart copy, don't open anything
                cache_fd = 0;
                err_status = 0;
            } else {
                // Singlepart copy is NOT implemented here!
                cache_fd = -1;
                err_status = UNIX_FILE_OPEN_ERR;
            }
            if((result = ASSERT_ERROR(cache_fd  != -1, err_status, "Failed to open the cache file: \"%s\".",
                                      _filename.c_str())).ok()) {

                callback_data_t data;
                S3BucketContext bucketContext;

                bzero (&bucketContext, sizeof (bucketContext));
                bucketContext.bucketName = bucket.c_str();
                bucketContext.protocol = s3GetProto(_prop_map);
                bucketContext.stsDate = s3GetSTSDate(_prop_map);
                bucketContext.uriStyle = S3UriStylePath;
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();


                S3PutProperties *putProps = NULL;
                putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
                if ( putProps && enable_md5 )
                    putProps->md5 = s3CalcMD5( cache_fd, 0, _fileSize );
                putProps->expires = -1;

                if ( _fileSize < chunksize ) {
                    S3PutObjectHandler putObjectHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &putObjectDataCallback
                    };

                    do {
                        bzero (&data, sizeof (data));
                        data.fd = cache_fd;
                        data.contentLength = data.originalContentLength = _fileSize;
                        data.pCtx = &bucketContext;

                        unsigned long long usStart = usNow();
                        bucketContext.hostName = s3GetHostname();
                        S3_put_object (&bucketContext, key.c_str(), _fileSize, putProps, 0, &putObjectHandler, &data);
                        unsigned long long usEnd = usNow();
                        double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                        rodsLog( LOG_DEBUG, "BW=%lf", bw);
                        if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                    } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );
                    if (data.status != S3StatusOK) {
                        std::stringstream msg;
                        msg << __FUNCTION__ << " - Error putting the S3 object: \"" << _s3ObjName << "\"";
                        if ( data.status >= 0 ) {
                            msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                        }
                        result = ERROR(S3_PUT_ERROR, msg.str());
                    }

                    // Clear up the S3PutProperties, if it exists
                    if (putProps) {
                        if (putProps->md5) free( (char*)putProps->md5 );
                        free( putProps );
                    }
                } else {
                    // Multi-part upload or copy
                    upload_manager_t manager;
                    memset(&manager, 0, sizeof(manager));

                    manager.upload_id = NULL;
                    manager.remaining = 0;
                    manager.offset  = 0;
                    manager.xml = NULL;

                    g_mpuResult = SUCCESS();

                    msg.str( std::string() ); // Clear

                    long seq;
                    long totalSeq = (_fileSize + chunksize - 1) / chunksize;

                    multipart_data_t partData;
                    int partContentLength = 0;

                    bzero (&data, sizeof (data));
                    data.fd = cache_fd;
                    data.contentLength = data.originalContentLength = _fileSize;

                    // Allocate all dynamic storage now, so we don't start a job we can't finish later
                    manager.etags = (char**)calloc(sizeof(char*) * totalSeq, 1);
                    if (!manager.etags) {
                        // Clear up the S3PutProperties, if it exists
                        if (putProps) {
                            if (putProps->md5) free( (char*)putProps->md5 );
                            free( putProps );
                        }
                        const char *msg = "Out of memory error in S3 multipart ETags allocation.";
                        rodsLog( LOG_ERROR, msg );
                        result = ERROR( SYS_MALLOC_ERR, msg );
                        return result;
                    }
                    g_mpuData = (multipart_data_t*)calloc(totalSeq, sizeof(multipart_data_t));
                    if (!g_mpuData) {
                        // Clear up the S3PutProperties, if it exists
                        if (putProps) {
                            if (putProps->md5) free( (char*)putProps->md5 );
                            free( putProps );
                        }
                        free(manager.etags);
                        const char *msg = "Out of memory error in S3 multipart g_mpuData allocation.";
                        rodsLog( LOG_ERROR, msg );
                        result = ERROR( SYS_MALLOC_ERR, msg );
                        return result;
                    }
                    // Maximum XML completion length with extra space for the <complete...></complete...> tag
                    manager.xml = (char *)malloc((totalSeq+2) * 256);
                    if (manager.xml == NULL) {
                        // Clear up the S3PutProperties, if it exists
                        if (putProps) {
                            if (putProps->md5) free( (char*)putProps->md5 );
                            free( putProps );
                        }
                        free(g_mpuData);
                        free(manager.etags);
                        const char *msg = "Out of memory error in S3 multipart XML allocation.";
                        rodsLog( LOG_ERROR, msg );
                        result = ERROR( SYS_MALLOC_ERR, msg );
                        return result;
                    }

                    retry_cnt = 0;
                    // These expect a upload_manager_t* as cbdata
                    S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };
                    do {
                        bucketContext.hostName = s3GetHostname();
                        manager.pCtx = &bucketContext;
                        S3_initiate_multipart(&bucketContext, key.c_str(), putProps, &mpuInitialHandler, NULL, &manager);
                        if (manager.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                    } while ( (manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < g_retry_count ));
                    if (manager.upload_id == NULL || manager.status != S3StatusOK) {
                        // Clear up the S3PutProperties, if it exists
                        if (putProps) {
                            if (putProps->md5) free( (char*)putProps->md5 );
                            free( putProps );
                        }
                        msg.str( std::string() ); // Clear
                        msg << __FUNCTION__ << " - Error initiating multipart upload of the S3 object: \"" << _s3ObjName << "\"";
                        if(manager.status >= 0) {
                            msg << " - \"" << S3_get_status_name(manager.status) << "\"";
                        }
                        rodsLog( LOG_ERROR, msg.str().c_str() );
                        result = ERROR( S3_PUT_ERROR, msg.str() );
                        return result; // Abort early
                    }

                    // Following used by S3_COPYOBJECT only
                    S3BucketContext srcBucketContext;
                    if (_mode == S3_COPYOBJECT) {
                        ret = parseS3Path(_filename, srcBucket, srcKey);
                        if(!(result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                                                  _filename.c_str())).ok()) {
                            return result;  // Abort early
                        }
                        bzero (&srcBucketContext, sizeof (srcBucketContext));
                        srcBucketContext.bucketName = srcBucket.c_str();
                        srcBucketContext.protocol = s3GetProto(_prop_map);
                        srcBucketContext.stsDate = s3GetSTSDate(_prop_map);
                        srcBucketContext.uriStyle = S3UriStylePath;
                        srcBucketContext.accessKeyId = _key_id.c_str();
                        srcBucketContext.secretAccessKey = _access_key.c_str();
                    }

                    g_mpuNext = 0;
                    g_mpuLast = totalSeq;
                    g_mpuUploadId = manager.upload_id;
                    g_mpuKey = key.c_str();
                    for(seq = 1; seq <= totalSeq ; seq ++) {
                        memset(&partData, 0, sizeof(partData));
                        partData.manager = &manager;
                        partData.seq = seq;
                        partData.mode = _mode;
                        if (_mode == S3_COPYOBJECT) {
                            partData.pSrcCtx = &srcBucketContext;
                            partData.srcKey = srcKey.c_str();
                        }
                        partData.put_object_data = data;
                        partContentLength = (data.contentLength > chunksize)?chunksize:data.contentLength;
                        partData.put_object_data.contentLength = partContentLength;
                        partData.put_object_data.offset = (seq-1) * chunksize;
                        partData.enable_md5 = s3GetEnableMD5( _prop_map );
                        partData.server_encrypt = s3GetServerEncrypt( _prop_map );
                        g_mpuData[seq-1] = partData;
                        data.contentLength -= partContentLength;
                    }

                    unsigned long long usStart = usNow();

                    // Make the worker threads and start
                    int nThreads = s3GetMPUThreads(_prop_map);

                    std::list<boost::thread*> threads;
                    for (int thr_id=0; thr_id<nThreads; thr_id++) {
                        boost::thread *thisThread = new boost::thread(mpuWorkerThread, &bucketContext);
                        threads.push_back(thisThread);
                    }

                    // And wait for them to finish...
                    while (!threads.empty()) {
                        boost::thread *thisThread = threads.front();
                        thisThread->join();
                        delete thisThread;
                        threads.pop_front();
                    }

                    unsigned long long usEnd = usNow();
                    double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                    rodsLog( LOG_DEBUG, "MultipartBW=%lf", bw);

                    manager.remaining = 0;
                    manager.offset  = 0;

                    if (g_mpuResult.ok()) { // If someone aborted, don't complete...
                        msg.str( std::string() ); // Clear
                        msg << "Multipart:  Completing key \"" << key.c_str() << "\"";
                        rodsLog( LOG_DEBUG, msg.str().c_str() );

                        int i;
                        strcpy(manager.xml, "<CompleteMultipartUpload>\n");
                        manager.remaining = strlen(manager.xml);
                        char buf[256];
                        int n;
                        for ( i = 0; i < totalSeq; i++ ) {
                            n = snprintf( buf, 256, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", i + 1, manager.etags[i] );
                            strcpy( manager.xml+manager.remaining, buf );
                            manager.remaining += n;
                        }
                        strcat( manager.xml + manager.remaining, "</CompleteMultipartUpload>\n" );
                        manager.remaining += strlen( manager.xml+manager.remaining );
                        int manager_remaining = manager.remaining;
                        manager.offset = 0;
                        retry_cnt = 0;
                        S3MultipartCommitHandler commit_handler = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, NULL };
                        do {
                            // On partial error, need to restart XML send from the beginning
                            manager.remaining = manager_remaining;
                            manager.offset = 0;
                            bucketContext.hostName = s3GetHostname();
                            manager.pCtx = &bucketContext;
                            S3_complete_multipart_upload(&bucketContext, key.c_str(), &commit_handler, manager.upload_id, manager.remaining, NULL, &manager);
                            if (manager.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                        } while ((manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < g_retry_count ));
                        if (manager.status != S3StatusOK) {
                            msg.str( std::string() ); // Clear
                            msg << __FUNCTION__ << " - Error putting the S3 object: \"" << _s3ObjName << "\"";
                            if(manager.status >= 0) {
                                msg << " - \"" << S3_get_status_name( manager.status ) << "\"";
                            }
                            g_mpuResult = ERROR( S3_PUT_ERROR, msg.str() );
                        }
                    }
                    if ( !g_mpuResult.ok() && manager.upload_id ) {
                        // Someone aborted after we started, delete the partial object on S3
                        rodsLog(LOG_ERROR, "Cancelling multipart upload");
                        mpuCancel( &bucketContext, key.c_str(), manager.upload_id );
                        // Return the error
                        result = g_mpuResult;
                    }
                    // Clean up memory
                    if (manager.xml) free(manager.xml);
                    if (manager.upload_id) free(manager.upload_id);
                    for (int i=0; manager.etags && i<totalSeq; i++) {
                        if (manager.etags[i]) free(manager.etags[i]);
                    }
                    if (manager.etags) free(manager.etags);
                    if (g_mpuData) free(g_mpuData);
                    // Clear up the S3PutProperties, if it exists
                    if (putProps) {
                        if (putProps->md5) free( (char*)putProps->md5 );
                        free( putProps );
                    }
                }

                if (_mode != S3_COPYOBJECT) close(cache_fd);
            }
        }
    }
    return result;
}

// Define interface so we can use object stat to get the size we're copying
irods::error s3FileStatPlugin( irods::plugin_context& _ctx, struct stat* _statbuf );

/// @brief Function to copy the specified src file to the specified dest file
irods::error s3CopyFile(
    irods::plugin_context& _src_ctx,
    const std::string& _src_file,
    const std::string& _dest_file,
    const std::string& _key_id,
    const std::string& _access_key,
    const S3Protocol _proto,
    const S3STSDate _stsDate)
{
    irods::error result = SUCCESS();
    irods::error ret;
    std::string src_bucket;
    std::string src_key;
    std::string dest_bucket;
    std::string dest_key;

    // Check the size, and if too large punt to the multipart copy/put routine
    struct stat statbuf;
    ret = s3FileStatPlugin( _src_ctx, &statbuf );
    if (( result = ASSERT_PASS(ret, "Unable to get original object size for source file name: \"%s\".",
                               _src_file.c_str())).ok()) {
        if ( statbuf.st_size > s3GetMPUChunksize(_src_ctx.prop_map()) ) {
            // Early return for cleaner code...
            return s3PutCopyFile( S3_COPYOBJECT, _src_file, _dest_file, statbuf.st_size, _key_id, _access_key, _src_ctx.prop_map() );
        }

        // Parse the src file
        ret = parseS3Path(_src_file, src_bucket, src_key);
        if((result = ASSERT_PASS(ret, "Failed to parse the source file name: \"%s\".",
                                 _src_file.c_str())).ok()) {

            // Parse the dest file
            ret = parseS3Path(_dest_file, dest_bucket, dest_key);
            if((result = ASSERT_PASS(ret, "Failed to parse the destination file name: \"%s\".",
                                     _dest_file.c_str())).ok()) {

                callback_data_t data;
                S3BucketContext bucketContext;
                int64_t lastModified;
                char eTag[256];

                bzero (&bucketContext, sizeof (bucketContext));
                bucketContext.bucketName = src_bucket.c_str();
                bucketContext.protocol = _proto;
                bucketContext.stsDate = _stsDate;
                bucketContext.uriStyle = S3UriStylePath;
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();

                S3ResponseHandler responseHandler = {
                    &responsePropertiesCallback,
                    &responseCompleteCallback
                };

                // initialize put properties
                S3PutProperties putProps;
                memset(&putProps, 0, sizeof(S3PutProperties));
                putProps.expires = -1;

                size_t retry_cnt = 0;
                do {
                    bzero (&data, sizeof (data));
                    bucketContext.hostName = s3GetHostname();
                    data.pCtx = &bucketContext;
                    S3_copy_object(&bucketContext, src_key.c_str(), dest_bucket.c_str(), dest_key.c_str(), &putProps, &lastModified, sizeof(eTag), eTag, 0,
                                   &responseHandler, &data);
                    if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );
                if (data.status != S3StatusOK) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Error copying the S3 object: \"" << _src_file << "\" to S3 object: \"" << _dest_file << "\"";
                    if (data.status >= 0) {
                        msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                    }
                    result = ERROR(S3_FILE_COPY_ERR, msg.str());
                }
            }
        }
    }
    return result;
}

irods::error s3GetAuthCredentials(
    irods::plugin_property_map& _prop_map,
    std::string& _rtn_key_id,
    std::string& _rtn_access_key)
{
    irods::error result = SUCCESS();
    irods::error ret;
    std::string key_id;
    std::string access_key;

    ret = _prop_map.get<std::string>(s3_key_id, key_id);
    if((result = ASSERT_PASS(ret, "Failed to get the S3 access key id property.")).ok()) {

        ret = _prop_map.get<std::string>(s3_access_key, access_key);
        if((result = ASSERT_PASS(ret, "Failed to get the S3 secret access key property.")).ok()) {

            _rtn_key_id = key_id;
            _rtn_access_key = access_key;
        }
    }

    return result;
}
//
//////////////////////////////////////////////////////////////////////

// =-=-=-=-=-=-=-
/// @brief Checks the basic operation parameters and updates the physical path in the file object
irods::error s3CheckParams(irods::plugin_context& _ctx ) {

    irods::error result = SUCCESS();
    irods::error ret;

    // =-=-=-=-=-=-=-
    // verify that the resc context is valid
    ret = _ctx.valid();
    result = ASSERT_PASS(ret, "Resource context is invalid");

    return result;

} // Check Params

/// @brief Start up operation - Initialize the S3 library and set the auth fields in the properties.
irods:: error s3StartOperation(irods::plugin_property_map& _prop_map)
{
    irods::error result = SUCCESS();
    irods::error ret;

    // Initialize the S3 library
    // only call s3Init within a given operation in order to not conflict with
    // other instances of S3 resources.  leaving the code here as there is still
    // an open issue regarding iRODS connection reuse with the option to use
    // another S3 resource which will cause an error.
    //ret = s3Init( _prop_map );
    //if((result = ASSERT_PASS(ret, "Failed to initialize the S3 library.")).ok()) {
    // Retrieve the auth info and set the appropriate fields in the property map
    ret = s3ReadAuthInfo(_prop_map);
    result = ASSERT_PASS(ret, "Failed to read S3 auth info.");
    //}

    return result;
}

/// @brief stop operation. All this does is deinitialize the s3 library
irods::error s3StopOperation(irods::plugin_property_map& _prop_map)
{
    irods::error result = SUCCESS();
    if(S3Initialized) {
        S3Initialized = false;

        S3_deinitialize();
    }
    return result;
}

// =-=-=-=-=-=-=-
// interface for file registration
irods::error s3RegisteredPlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
}

// =-=-=-=-=-=-=-
// interface for file unregistration
irods::error s3UnregisteredPlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
}

// =-=-=-=-=-=-=-
// interface for file modification
irods::error s3ModifiedPlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
}

// =-=-=-=-=-=-=-
// interface for POSIX create
irods::error s3FileCreatePlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
}

// =-=-=-=-=-=-=-
// interface for POSIX Open
irods::error s3FileOpenPlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
}

// =-=-=-=-=-=-=-
// interface for POSIX Read
irods::error s3FileReadPlugin( irods::plugin_context& _ctx,
                               void*               _buf,
                               int                 _len ) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

}

// =-=-=-=-=-=-=-
// interface for POSIX Write
irods::error s3FileWritePlugin( irods::plugin_context& _ctx,
                                void*               _buf,
                                int                 _len ) {
    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

}

// =-=-=-=-=-=-=-
// interface for POSIX Close
irods::error s3FileClosePlugin(  irods::plugin_context& _ctx ) {

    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

}

// =-=-=-=-=-=-=-
// interface for POSIX Unlink
irods::error s3FileUnlinkPlugin(
    irods::plugin_context& _ctx )
{
    irods::error result = SUCCESS();

    // =-=-=-=-=-=-=-
    // check incoming parameters
    irods::error ret = s3CheckParams( _ctx );
    if(!ret.ok()) {
        std::stringstream msg;
        msg << __FUNCTION__ << " - Invalid parameters or physical path.";
        result = PASSMSG(msg.str(), ret);
    }
    else {

        // =-=-=-=-=-=-=-
        // get ref to fco
        irods::data_object_ptr _object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());

        irods::error ret;
        std::string bucket;
        std::string key;
        std::string key_id;
        std::string access_key;

        ret = parseS3Path(_object->physical_path(), bucket, key);
        if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                                 _object->physical_path().c_str())).ok()) {

            ret = s3Init( _ctx.prop_map() );
            if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

                ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                if((result = ASSERT_PASS(ret, "Failed to get the S3 credentials properties.")).ok()) {

                    callback_data_t data;
                    S3BucketContext bucketContext;

                    bzero (&bucketContext, sizeof (bucketContext));
                    bucketContext.bucketName = bucket.c_str();
                    bucketContext.protocol = s3GetProto(_ctx.prop_map());
                    bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
                    bucketContext.uriStyle = S3UriStylePath;
                    bucketContext.accessKeyId = key_id.c_str();
                    bucketContext.secretAccessKey = access_key.c_str();

                    S3ResponseHandler responseHandler = { 0, &responseCompleteCallback };
                    size_t retry_cnt = 0;
                    do {
                        bzero (&data, sizeof (data));
                        bucketContext.hostName = s3GetHostname();
                        data.pCtx = &bucketContext;
                        S3_delete_object(&bucketContext, key.c_str(), 0, &responseHandler, &data);
                        if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                    } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );

                    if (data.status != S3StatusOK) {
                        std::stringstream msg;
                        msg << __FUNCTION__;
                        msg << " - Error unlinking the S3 object: \"";
                        msg << _object->physical_path();
                        msg << "\"";
                        if(data.status >= 0) {
                            msg << " - \"";
                            msg << S3_get_status_name((S3Status)data.status);
                            msg << "\"";
                        }
                        result = ERROR(S3_FILE_UNLINK_ERR, msg.str());
                    }
                }
            }
        }
    }
    return result;
}

// =-=-=-=-=-=-=-
// interface for POSIX Stat
irods::error s3FileStatPlugin(
    irods::plugin_context& _ctx,
    struct stat* _statbuf )
{

    irods::error result = SUCCESS();

    // =-=-=-=-=-=-=-
    // check incoming parameters
    irods::error ret = s3CheckParams( _ctx );
    if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

        // =-=-=-=-=-=-=-
        // get ref to fco
        irods::data_object_ptr _object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());

        bzero (_statbuf, sizeof (struct stat));

        if(_object->physical_path().find("/", _object->physical_path().size()) != std::string::npos) {
            // A directory
            _statbuf->st_mode = S_IFDIR;
        } else {

            irods::error ret;
            std::string bucket;
            std::string key;
            std::string key_id;
            std::string access_key;

            ret = parseS3Path(_object->physical_path(), bucket, key);
            if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                                     _object->physical_path().c_str())).ok()) {

                ret = s3Init( _ctx.prop_map() );
                if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

                    ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                    if((result = ASSERT_PASS(ret, "Failed to get the S3 credentials properties.")).ok()) {

                        callback_data_t data;
                        S3BucketContext bucketContext;

                        bzero (&bucketContext, sizeof (bucketContext));
                        bucketContext.bucketName = bucket.c_str();
                        bucketContext.protocol = s3GetProto(_ctx.prop_map());
                        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
                        bucketContext.uriStyle = S3UriStylePath;
                        bucketContext.accessKeyId = key_id.c_str();
                        bucketContext.secretAccessKey = access_key.c_str();

                        S3ListBucketHandler listBucketHandler = {
                            { &responsePropertiesCallback, &responseCompleteCallback },
                            &listBucketCallback
                        };

                        data.keyCount = 0;

                        size_t retry_cnt = 0;
                        do {
                            bzero (&data, sizeof (data));
                            bucketContext.hostName = s3GetHostname();
                            data.pCtx = &bucketContext;
                            S3_list_bucket(&bucketContext, key.c_str(), NULL, NULL, 1, 0, &listBucketHandler, &data);
                            if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                        } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );

                        if (data.status != S3StatusOK) {
                            std::stringstream msg;
                            msg << __FUNCTION__ << " - Error stat'ing the S3 object: \"" << _object->physical_path() << "\"";
                            if (data.status >= 0) {
                                msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                            }
                            result = ERROR(S3_FILE_STAT_ERR, msg.str());
                        }

                        else if(data.keyCount > 0) {
                            _statbuf->st_mode = S_IFREG;
                            _statbuf->st_nlink = 1;
                            _statbuf->st_uid = getuid ();
                            _statbuf->st_gid = getgid ();
                            _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = data.s3Stat.lastModified;
                            _statbuf->st_size = data.s3Stat.size;
                        }

                        else {
                            std::stringstream msg;
                            msg << __FUNCTION__;
                            msg << " - S3 object not found: \"";
                            msg << _object->physical_path();
                            msg << "\"";
                            result = ERROR(S3_FILE_STAT_ERR, msg.str());
                        }
                    }
                }
            }
        }
    }
    if( !result.ok() ) {
        irods::log( result );
    }
    return result;
}

// =-=-=-=-=-=-=-
// interface for POSIX Fstat
irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                 struct stat*        _statbuf ) {
    return ERROR( SYS_NOT_SUPPORTED, "s3FileFstatPlugin" );

} // s3FileFstatPlugin

// =-=-=-=-=-=-=-
// interface for POSIX lseek
irods::error s3FileLseekPlugin(  irods::plugin_context& _ctx,
                                 size_t              _offset,
                                 int                 _whence ) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileLseekPlugin" );

} // wosFileLseekPlugin

// =-=-=-=-=-=-=-
// interface for POSIX mkdir
irods::error s3FileMkdirPlugin(  irods::plugin_context& _ctx ) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileMkdirPlugin" );

} // s3FileMkdirPlugin

// =-=-=-=-=-=-=-
// interface for POSIX mkdir
irods::error s3FileRmdirPlugin(  irods::plugin_context& _ctx ) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileRmdirPlugin" );
} // s3FileRmdirPlugin

// =-=-=-=-=-=-=-
// interface for POSIX opendir
irods::error s3FileOpendirPlugin( irods::plugin_context& _ctx ) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileOpendirPlugin" );
} // s3FileOpendirPlugin

// =-=-=-=-=-=-=-
// interface for POSIX closedir
irods::error s3FileClosedirPlugin( irods::plugin_context& _ctx) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileClosedirPlugin" );
} // s3FileClosedirPlugin

// =-=-=-=-=-=-=-
// interface for POSIX readdir
irods::error s3FileReaddirPlugin( irods::plugin_context& _ctx,
                                  struct rodsDirent**     _dirent_ptr ) {

    return ERROR( SYS_NOT_SUPPORTED, "s3FileReaddirPlugin" );
} // s3FileReaddirPlugin

// =-=-=-=-=-=-=-
// interface for POSIX rename
irods::error s3FileRenamePlugin( irods::plugin_context& _ctx,
                                 const char*         _new_file_name )
{
    irods::error result = SUCCESS();
    irods::error ret;
    std::string key_id;
    std::string access_key;

    // retrieve archive naming policy from resource plugin context
    std::string archive_naming_policy = CONSISTENT_NAMING; // default
    ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
    if(!ret.ok()) {
        irods::log(PASS(ret));
    }
    boost::to_lower(archive_naming_policy);

    irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

    // if archive naming policy is decoupled we're done
    if (archive_naming_policy == DECOUPLED_NAMING) {
        object->file_descriptor(ENOSYS);
        return SUCCESS();
    }

    ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
    if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

        // copy the file to the new location
        ret = s3CopyFile(_ctx, object->physical_path(), _new_file_name, key_id, access_key,
                         s3GetProto(_ctx.prop_map()), s3GetSTSDate(_ctx.prop_map()));
        if((result = ASSERT_PASS(ret, "Failed to copy file from: \"%s\" to \"%s\".",
                                 object->physical_path().c_str(), _new_file_name)).ok()) {
            // delete the old file
            ret = s3FileUnlinkPlugin(_ctx);
            result = ASSERT_PASS(ret, "Failed to unlink old S3 file: \"%s\".",
                                 object->physical_path().c_str());
        }
    }
    return result;
} // s3FileRenamePlugin

// =-=-=-=-=-=-=-
// interface for POSIX truncate
irods::error s3FileTruncatePlugin(
    irods::plugin_context& _ctx )
{
    return ERROR( SYS_NOT_SUPPORTED, "s3FileTruncatePlugin" );
} // s3FileTruncatePlugin


// interface to determine free space on a device given a path
irods::error s3FileGetFsFreeSpacePlugin(
    irods::plugin_context& _ctx )
{
    return ERROR(SYS_NOT_SUPPORTED, "s3FileGetFsFreeSpacePlugin");

} // s3FileGetFsFreeSpacePlugin

irods::error s3FileCopyPlugin( int mode, const char *srcFileName,
                               const char *destFileName)
{
    return ERROR( SYS_NOT_SUPPORTED, "s3FileCopyPlugin" );
}


// =-=-=-=-=-=-=-
// s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
// Just copy the file from filename to cacheFilename. optionalInfo info
// is not used.
irods::error s3StageToCachePlugin(
    irods::plugin_context& _ctx,
    const char*                               _cache_file_name )
{
    irods::error result = SUCCESS();

    // =-=-=-=-=-=-=-
    // check incoming parameters
    irods::error ret = s3CheckParams( _ctx );
    if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

        struct stat statbuf;
        std::string key_id;
        std::string access_key;

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        ret = s3FileStatPlugin(_ctx, &statbuf);
        if((result = ASSERT_PASS(ret, "Failed stating the file: \"%s\".",
                                 object->physical_path().c_str())).ok()) {

            if((result = ASSERT_ERROR((statbuf.st_mode & S_IFREG) != 0, S3_FILE_STAT_ERR, "Error stating the file: \"%s\".",
                                      object->physical_path().c_str())).ok()) {

                if((result = ASSERT_ERROR(object->size() <= 0 || object->size() == static_cast<size_t>(statbuf.st_size), SYS_COPY_LEN_ERR,
                                          "Error for file: \"%s\" inp data size: %ld does not match stat size: %ld.",
                                          object->physical_path().c_str(), object->size(), statbuf.st_size)).ok()) {

                    ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                    if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                        ret = s3GetFile( _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
                        result = ASSERT_PASS(ret, "Failed to copy the S3 object: \"%s\" to the cache: \"%s\".",
                                             object->physical_path().c_str(), _cache_file_name);
                    }
                }
            }
        }
    }
    return result;
} // s3StageToCachePlugin

// =-=-=-=-=-=-=-
// s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
// Just copy the file from cacheFilename to filename. optionalInfo info
// is not used.
irods::error s3SyncToArchPlugin(
    irods::plugin_context& _ctx,
    const char* _cache_file_name )
{
    irods::error result = SUCCESS();
    // =-=-=-=-=-=-=-
    // check incoming parameters
    irods::error ret = s3CheckParams( _ctx );
    if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

        struct stat statbuf;
        int status;
        std::string key_id;
        std::string access_key;

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        status = stat(_cache_file_name, &statbuf);
        int err_status = UNIX_FILE_STAT_ERR - errno;
        if((result = ASSERT_ERROR(status >= 0, err_status, "Failed to stat cache file: \"%s\".",
                                  _cache_file_name)).ok()) {

            if((result = ASSERT_ERROR((statbuf.st_mode & S_IFREG) != 0, UNIX_FILE_STAT_ERR, "Cache file: \"%s\" is not a file.",
                                      _cache_file_name)).ok()) {

                ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                    std::string default_hostname;
                    ret = _ctx.prop_map().get< std::string >(
                        s3_default_hostname,
                        default_hostname );
                    if( !ret.ok() ) {
                        irods::log(ret);
                    }

                    // retrieve archive naming policy from resource plugin context
                    std::string archive_naming_policy = CONSISTENT_NAMING; // default
                    ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
                    if(!ret.ok()) {
                        irods::log(ret);
                    }
                    boost::to_lower(archive_naming_policy);

                    // if archive naming policy is decoupled
                    // we use the object's reversed id as S3 key name prefix
                    if (archive_naming_policy == DECOUPLED_NAMING) {
                        // extract object name and bucket name from physical path
                        std::vector< std::string > tokens;
                        irods::string_tokenize(object->physical_path(), "/", tokens);
                        std::string bucket_name = tokens.front();
                        std::string object_name = tokens.back();

                        // reverse object id
                        std::string obj_id = boost::lexical_cast<std::string>(object->id());
                        std::reverse(obj_id.begin(), obj_id.end());

                        // make S3 key name
                        std::ostringstream s3_key_name;
                        s3_key_name << "/" << bucket_name << "/" << obj_id << "/" << object_name;

                        // update physical path
                        object->physical_path(s3_key_name.str());
                    }

                    ret = s3PutCopyFile(S3_PUTFILE, _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
                    result = ASSERT_PASS(ret, "Failed to copy the cache file: \"%s\" to the S3 object: \"%s\".",
                                         _cache_file_name, object->physical_path().c_str());

                }
            }
        }
    }
    if( !result.ok() ) {
        irods::log( result );
    }
    return result;
} // s3SyncToArchPlugin



// =-=-=-=-=-=-=-
// ds3SyncToArch
irods::error ds3SyncToArchPlugin(
    irods::plugin_context& _ctx,
    const char* _cache_file_name )
{
    irods::error result = SUCCESS();
    // =-=-=-=-=-=-=-
    // check incoming parameters
    irods::error ret = s3CheckParams( _ctx );
    if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

        struct stat statbuf;
        int status;
        std::string key_id;
        std::string access_key;

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        status = stat(_cache_file_name, &statbuf);
        int err_status = UNIX_FILE_STAT_ERR - errno;
        if((result = ASSERT_ERROR(status >= 0, err_status, "Failed to stat cache file: \"%s\".",
                                  _cache_file_name)).ok()) {

            if((result = ASSERT_ERROR((statbuf.st_mode & S_IFREG) != 0, UNIX_FILE_STAT_ERR, "Cache file: \"%s\" is not a file.",
                                      _cache_file_name)).ok()) {

                ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                    std::string default_hostname;
                    ret = _ctx.prop_map().get< std::string >(
                        s3_default_hostname,
                        default_hostname );
                    if( !ret.ok() ) {
                        irods::log(ret);
                    }

                    // retrieve archive naming policy from resource plugin context
                    std::string archive_naming_policy = CONSISTENT_NAMING; // default
                    ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
                    if(!ret.ok()) {
                        irods::log(ret);
                    }
                    boost::to_lower(archive_naming_policy);

                    // if archive naming policy is decoupled
                    // we use the object's reversed id as S3 key name prefix
                    if (archive_naming_policy == DECOUPLED_NAMING) {
                        // extract object name and bucket name from physical path
                        std::vector< std::string > tokens;
                        irods::string_tokenize(object->physical_path(), "/", tokens);
                        std::string bucket_name = tokens.front();
                        std::string object_name = tokens.back();

                        // reverse object id
                        std::string obj_id = boost::lexical_cast<std::string>(object->id());
                        std::reverse(obj_id.begin(), obj_id.end());

                        // make S3 key name
                        std::ostringstream s3_key_name;
                        s3_key_name << "/" << bucket_name << "/" << obj_id << "/" << object_name;

                        // update physical path
                        object->physical_path(s3_key_name.str());
                    }


                    ////////////////////////////////////

                        // Get Service
                        ds3_client* client;
                        ds3_request* request;
                        ds3_error* error;
                        ds3_list_all_my_buckets_result_response *response;
                        uint64_t bucket_index;

                        // Create a client from environment variables
                        error = ds3_create_client_from_env(&client);
                        ds3_handle_error(error);

                        // Create the get service request.  All requests to a DS3 appliance start this way.
                        // All ds3_init_* functions return a ds3_request struct
                        request = ds3_init_get_service_request();

                        // This performs the request to a DS3 appliance.
                        // If there is an error 'error' will not be NULL
                        // If the request completed successfully then 'error' will be NULL
                        error = ds3_get_service_request(client, request, &response);
                        ds3_request_free(request);
                        ds3_handle_error(error);

                        if(response->num_buckets == 0) {
                            printf("No buckets returned\n");
                            ds3_list_all_my_buckets_result_response_free(response);
                            ds3_creds_free(client->creds);
                            ds3_client_free(client);
                            return result;
                        }

                        for (bucket_index = 0; bucket_index < response->num_buckets; bucket_index++) {
                            ds3_bucket_details_response* bucket = response->buckets[bucket_index];

                            // IRODS LOG
                            rodsLog( LOG_NOTICE, "@@@@@@@@@@@@@@@  Bucket: (%s) created on %s\n", bucket->name->value, bucket->creation_date->value);
                            //printf("Bucket: (%s) created on %s\n", bucket->name->value, bucket->creation_date->value);
                        }

                        ds3_list_all_my_buckets_result_response_free(response);
                        ds3_creds_free(client->creds);
                        ds3_client_free(client);
                        ds3_cleanup();

                        //return 0;

                    ////////////////////////////////////



//                    ret = s3PutCopyFile(S3_PUTFILE, _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
//                    result = ASSERT_PASS(ret, "Failed to copy the cache file: \"%s\" to the S3 object: \"%s\".",
//                                         _cache_file_name, object->physical_path().c_str());

                }
            }
        }
    }
    if( !result.ok() ) {
        irods::log( result );
    }
    return result;
} // s3SyncToArchPlugin


// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectCreate(
    irods::plugin_property_map& _prop_map,
    irods::file_object&           _file_obj,
    const std::string&             _resc_name,
    const std::string&             _curr_host,
    float&                         _out_vote )
{
    irods::error result = SUCCESS();
    irods::error ret;
    int resc_status = 0;
    std::string host_name;

    // =-=-=-=-=-=-=-
    // determine if the resource is down
    ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
    if((result = ASSERT_PASS(ret, "Failed to retrieve status property.")).ok() ) {

        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
        if((result = ASSERT_PASS(ret, "Failed to get location property.")).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
            }

            // =-=-=-=-=-=-=-
            // vote higher if we are on the same host
            else if( _curr_host == host_name ) {
                _out_vote = 1.0;
            } else {
                _out_vote = 0.5;
            }
        }
    }

    return result;
} // s3RedirectCreate

// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectOpen(
    irods::plugin_property_map& _prop_map,
    irods::file_object&           _file_obj,
    const std::string&             _resc_name,
    const std::string&             _curr_host,
    float&                         _out_vote )
{
    irods::error result = SUCCESS();
    irods::error ret;
    int resc_status = 0;
    std::string host_name;

    // =-=-=-=-=-=-=-
    // determine if the resource is down
    ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
    if((result = ASSERT_PASS(ret, "Failed to get status property for resource.")).ok() ) {

        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
        if((result = ASSERT_PASS(ret, "Failed to get the location property.")).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
            }

            // =-=-=-=-=-=-=-
            // vote higher if we are on the same host
            else if( _curr_host == host_name ) {
                _out_vote = 1.0;
            } else {
                _out_vote = 0.5;
            }
        }
    }

    return result;
} // s3RedirectOpen

// =-=-=-=-=-=-=-
// used to allow the resource to determine which host
// should provide the requested operation
irods::error s3RedirectPlugin(
    irods::plugin_context& _ctx,
    const std::string*                  _opr,
    const std::string*                  _curr_host,
    irods::hierarchy_parser*           _out_parser,
    float*                              _out_vote )
{
    irods::error result = SUCCESS();
    irods::error ret;

    // =-=-=-=-=-=-=-
    // check the context validity
    ret = _ctx.valid< irods::file_object >();
    if((result = ASSERT_PASS(ret, "Invalid resource context.")).ok()) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        if((result = ASSERT_ERROR(_opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM,
                                  "One or more NULL pointer arguments.")).ok()) {

            std::string resc_name;

            // =-=-=-=-=-=-=-
            // cast down the chain to our understood object type
            irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

            // =-=-=-=-=-=-=-
            // get the name of this resource
            ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
            if((result = ASSERT_PASS(ret, "Failed to get resource name property.")).ok() ) {

                // =-=-=-=-=-=-=-
                // add ourselves to the hierarchy parser by default
                _out_parser->add_child( resc_name );

                // =-=-=-=-=-=-=-
                // test the operation to determine which choices to make
                if( irods::OPEN_OPERATION == (*_opr) ) {
                    // =-=-=-=-=-=-=-
                    // call redirect determination for 'get' operation
                    result = s3RedirectOpen( _ctx.prop_map(), *file_obj, resc_name, (*_curr_host), (*_out_vote)  );

                } else if( irods::CREATE_OPERATION == (*_opr) ) {
                    // =-=-=-=-=-=-=-
                    // call redirect determination for 'create' operation
                    result = s3RedirectCreate( _ctx.prop_map(), *file_obj, resc_name, (*_curr_host), (*_out_vote)  );
                }
                else {
                    result = ASSERT_ERROR(false, SYS_INVALID_INPUT_PARAM, "Unknown redirect operation: \"%s\".",
                                          _opr->c_str());
                }
            }
        }
    }

    return result;
} // s3RedirectPlugin

// =-=-=-=-=-=-=-
// code which would rebalance the resource, S3 does not rebalance.
irods::error s3FileRebalance(
    irods::plugin_context& _ctx ) {
    return SUCCESS();

} // s3FileRebalance


class s3_resource : public irods::resource {
public:
    s3_resource( const std::string& _inst_name,
                 const std::string& _context ) :
        irods::resource( _inst_name, _context ) {

        // =-=-=-=-=-=-=-
        // parse context string into property pairs assuming a ; as a separator
        std::vector< std::string > props;
        irods::kvp_map_t kvp;
        irods::parse_kvp_string(_context, kvp);

        // =-=-=-=-=-=-=-
        // copy the properties from the context to the prop map
        irods::kvp_map_t::iterator itr = kvp.begin();
        for( ; itr != kvp.end(); ++itr ) {
            properties_.set< std::string >(
                itr->first,
                itr->second );
        } // for itr

        // Add start and stop operations
        set_start_operation( s3StartOperation );
        set_stop_operation( s3StopOperation );
    } // ctor

    irods::error need_post_disconnect_maintenance_operation( bool& _b ) {
        _b = false;
        return SUCCESS();
    }


    // =-=-=-=-=-=-=-
    // 3b. pass along a functor for maintenance work after
    //     the client disconnects, uncomment the first two lines for effect.
    irods::error post_disconnect_maintenance_operation( irods::pdmo_type& _op  ) {
        return SUCCESS();
    }

}; // class s3_resource



extern "C"
irods::resource* plugin_factory( const std::string& _inst_name, const std::string& _context ) {
    s3_resource* resc = new s3_resource(_inst_name, _context);

    resc->add_operation(
        irods::RESOURCE_OP_CREATE,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileCreatePlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_OPEN,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileOpenPlugin ) );

    resc->add_operation<void*,int>(
        irods::RESOURCE_OP_READ,
        std::function<irods::error(irods::plugin_context&,void*,int)>(
            s3FileReadPlugin ) );

    resc->add_operation<void*,int>(
        irods::RESOURCE_OP_WRITE,
        std::function<irods::error(irods::plugin_context&,void*,int)>(
            s3FileWritePlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_CLOSE,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileClosePlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_UNLINK,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileUnlinkPlugin ) );

    resc->add_operation<struct stat*>(
        irods::RESOURCE_OP_STAT,
        std::function<irods::error(irods::plugin_context&, struct stat*)>(
            s3FileStatPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_MKDIR,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileMkdirPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_OPENDIR,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileOpendirPlugin ) );

    resc->add_operation<struct rodsDirent**>(
        irods::RESOURCE_OP_READDIR,
        std::function<irods::error(irods::plugin_context&,struct rodsDirent**)>(
            s3FileReaddirPlugin ) );

    resc->add_operation<const char*>(
        irods::RESOURCE_OP_RENAME,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            s3FileRenamePlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_FREESPACE,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileGetFsFreeSpacePlugin ) );

    resc->add_operation<long long, int>(
        irods::RESOURCE_OP_LSEEK,
        std::function<irods::error(irods::plugin_context&, long long, int)>(
            s3FileLseekPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_RMDIR,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileRmdirPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_CLOSEDIR,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileClosedirPlugin ) );

    resc->add_operation<const char*>(
        irods::RESOURCE_OP_STAGETOCACHE,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            s3StageToCachePlugin ) );

    resc->add_operation<const char*>(
        irods::RESOURCE_OP_SYNCTOARCH,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            ds3SyncToArchPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_REGISTERED,
        std::function<irods::error(irods::plugin_context&)>(
            s3RegisteredPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_UNREGISTERED,
        std::function<irods::error(irods::plugin_context&)>(
            s3UnregisteredPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_MODIFIED,
        std::function<irods::error(irods::plugin_context&)>(
            s3ModifiedPlugin ) );

    resc->add_operation<const std::string*, const std::string*, irods::hierarchy_parser*, float*>(
        irods::RESOURCE_OP_RESOLVE_RESC_HIER,
        std::function<irods::error(irods::plugin_context&,const std::string*, const std::string*, irods::hierarchy_parser*, float*)>(
            s3RedirectPlugin ) );

    resc->add_operation(
        irods::RESOURCE_OP_REBALANCE,
        std::function<irods::error(irods::plugin_context&)>(
            s3FileRebalance ) );

    // set some properties necessary for backporting to iRODS legacy code
    resc->set_property< int >( irods::RESOURCE_CHECK_PATH_PERM, DO_CHK_PATH_PERM );
    resc->set_property< int >( irods::RESOURCE_CREATE_PATH,     CREATE_PATH );
    resc->set_property< int >( "category",        FILE_CAT );

    return static_cast<irods::resource*>( resc );

} // plugin_factory
