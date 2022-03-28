// =-=-=-=-=-=-=-
// local includes
#include "libirods_s3.hpp"
#include "s3_operations.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include <irods/msParam.h>
#include <irods/rcConnect.h>
#include <irods/rodsLog.h>
#include <irods/rodsErrorTable.h>
#include <irods/objInfo.h>
#include <irods/rsRegReplica.hpp>
#include <irods/dataObjOpr.hpp>
#include <irods/irods_stacktrace.hpp>

#ifdef USING_JSON
#include <json/json.h>
#endif

// =-=-=-=-=-=-=-
// irods includes
#include <irods/irods_resource_plugin.hpp>
#include <irods/irods_file_object.hpp>
#include <irods/irods_physical_object.hpp>
#include <irods/irods_collection_object.hpp>
#include <irods/irods_string_tokenize.hpp>
#include <irods/irods_hierarchy_parser.hpp>
#include <irods/irods_resource_redirect.hpp>
#include <irods/irods_kvp_string_parser.hpp>
#include <irods/irods_virtual_path.hpp>
#include <irods/irods_resource_backport.hpp>
#include <irods/irods_query.hpp>

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <ctime>
#include <tuple>
#include <random>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <boost/filesystem/path.hpp>

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

#include <chrono>
#include <thread>

// =-=-=-=-=-=-=-
// other includes
#include <string.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <fmt/format.h>

#ifdef ERROR_INJECT
// Callback error injection
// If defined(ERROR_INJECT), then the specified pread/write below will fail.
// Only 1 failure happens, but this is OK since for every irods command we
// actually restart from 0 since the .SO is reloaded
// Pairing this with LIBS3 error injection will exercise the error recovery
// and retry code paths.
static boost::mutex g_error_mutex;
static std::int64_t g_werr{0}; // counter
static std::int64_t g_rerr{0}; // counter
static std::int64_t g_merr{0}; // counter
static const std::int64_t g_werr_idx{4}; // Which # pwrite to fail
static const std::int64_t g_rerr_idx{4}; // Which # pread to fail
static const std::int64_t g_merr_idx{4}; // Which part of Multipart Finish XML to fail
#endif

//////////////////////////////////////////////////////////////////////
// s3 specific functionality
static bool S3Initialized = false; // so we only initialize the s3 library once
static boost::mutex g_hostnameIdxLock;

const std::string  s3_default_hostname{"S3_DEFAULT_HOSTNAME"};
const std::string  s3_default_hostname_vector{"S3_DEFAULT_HOSTNAME_VECTOR"};
const std::string  s3_hostname_index{"S3_HOSTNAME_INDEX"};
const std::string  host_mode{"HOST_MODE"};
const std::string  s3_auth_file{"S3_AUTH_FILE"};
const std::string  s3_key_id{"S3_ACCESS_KEY_ID"};
const std::string  s3_access_key{"S3_SECRET_ACCESS_KEY"};
const std::string  s3_retry_count{"S3_RETRY_COUNT"};
const std::string  s3_wait_time_seconds{"S3_WAIT_TIME_SECONDS"};
const std::string  s3_wait_time_sec{"S3_WAIT_TIME_SEC"};                 // being deprecated
const std::string  s3_max_wait_time_seconds{"S3_MAX_WAIT_TIME_SECONDS"};
const std::string  s3_max_wait_time_sec{"S3_MAX_WAIT_TIME_SEC"};     // being deprecated
const std::string  s3_proto{"S3_PROTO"};
const std::string  s3_stsdate{"S3_STSDATE"};
const std::string  s3_max_upload_size{"S3_MAX_UPLOAD_SIZE"};
const std::string  s3_enable_mpu{"S3_ENABLE_MPU"};
const std::string  s3_mpu_chunk{"S3_MPU_CHUNK"};
const std::string  s3_mpu_threads{"S3_MPU_THREADS"};
const std::string  s3_enable_md5{"S3_ENABLE_MD5"};
const std::string  s3_server_encrypt{"S3_SERVER_ENCRYPT"};
const std::string  s3_region_name{"S3_REGIONNAME"};
const std::string  REPL_POLICY_KEY{"repl_policy"};
const std::string  REPL_POLICY_VAL{"reg_repl"};
const std::string  s3_cache_dir{"S3_CACHE_DIR"};
const std::string  s3_circular_buffer_size{"CIRCULAR_BUFFER_SIZE"};
const std::string  s3_circular_buffer_timeout_seconds{"CIRCULAR_BUFFER_TIMEOUT_SECONDS"};
const std::string  s3_uri_request_style{"S3_URI_REQUEST_STYLE"};        //  either "path" or "virtual_hosted" - default "path"
const std::string  s3_restoration_days{"S3_RESTORATION_DAYS"};          //  number of days sent to the RestoreObject operation
const std::string  s3_restoration_tier{"S3_RESTORATION_TIER"};          //  either "standard", "bulk", or "expedited"
const std::string  s3_enable_copyobject{"S3_ENABLE_COPYOBJECT"};       //  If set to 0 the CopyObject API will not be used.  Default is to use CopyObject.
const std::string  s3_non_data_transfer_timeout_seconds{"S3_NON_DATA_TRANSFER_TIMEOUT_SECONDS"};

const std::string  s3_number_of_threads{"S3_NUMBER_OF_THREADS"};        //  to save number of threads
const std::size_t  S3_DEFAULT_RETRY_WAIT_SECONDS = 2;
const std::size_t  S3_DEFAULT_MAX_RETRY_WAIT_SECONDS = 30;
const std::size_t  S3_DEFAULT_RETRY_COUNT = 3;
const int          S3_DEFAULT_CIRCULAR_BUFFER_SIZE = 4;
const unsigned int S3_DEFAULT_CIRCULAR_BUFFER_TIMEOUT_SECONDS = 180;
const unsigned int S3_DEFAULT_NON_DATA_TRANSFER_TIMEOUT_SECONDS = 300;

const std::string  S3_RESTORATION_TIER_STANDARD{"Standard"};
const std::string  S3_RESTORATION_TIER_BULK{"Bulk"};
const std::string  S3_RESTORATION_TIER_EXPEDITED{"Expedited"};
const unsigned int S3_DEFAULT_RESTORATION_DAYS = 7;
const std::string  S3_DEFAULT_RESTORATION_TIER{S3_RESTORATION_TIER_STANDARD};

thread_local S3ResponseProperties savedProperties;

// gets the resource name from the property map
std::string get_resource_name(irods::plugin_property_map& _prop_map) {

    std::string resc_property_name_str;
    irods::error ret = _prop_map.get<std::string>(irods::RESOURCE_NAME, resc_property_name_str);
    if (ret.ok()) {
        return resc_property_name_str;
    } else {
        return "";
    }
}

std::string get_region_name(irods::plugin_property_map& _prop_map) {

        std::string region_name = "us-east-1";
        if (!_prop_map.get< std::string >(s3_region_name, region_name ).ok()) {
            rodsLog( LOG_ERROR, "[resource_name=%s] Failed to retrieve S3 region name from resource plugin properties, using 'us-east-1'", get_resource_name(_prop_map).c_str());
        }
        return region_name;
}

std::tuple<bool, bool> get_modes_from_properties(irods::plugin_property_map& _prop_map) {

    // default modes
    bool cacheless_mode = false;
    bool attached_mode = true;

    std::string host_mode_str;

    irods::error ret = _prop_map.get< std::string >(host_mode, host_mode_str);
    if (ret.ok()) {

        if ( host_mode_str == "archive_attached" ) {
            attached_mode = true;
            cacheless_mode = false;
        } else if ( host_mode_str == "cacheless_attached" ) {
            attached_mode = true;
            cacheless_mode = true;
        } else if ( host_mode_str == "cacheless_detached" ) {
            attached_mode = false;
            cacheless_mode = true;
        }

    }

    return std::make_tuple(cacheless_mode, attached_mode);
}


// Sleep between _s/2 to _s. 
// The random addition ensures that threads don't all cluster up and retry
// at the same time (dogpile effect)
void s3_sleep(
    int _s) {

    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(0, RAND_MAX);
    int random = uniform_dist(e1);
    int sleep_time = (int)((((double)random / (double)RAND_MAX) + 1) * .5 * _s); // sleep between _s/2 and _s
    std::this_thread::sleep_for (std::chrono::seconds (sleep_time));
}

// Returns timestamp in usec for delta-t comparisons
static std::uint64_t usNow() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    std::uint64_t us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
    return us;
}

// Increment through all specified hostnames in the list, locking in the case
// where we may be multithreaded
std::string s3GetHostname(irods::plugin_property_map& _prop_map)
{
    std::vector<std::string> hostname_vector;
    std::size_t hostname_index = 0;
    g_hostnameIdxLock.lock();
    _prop_map.get<std::vector<std::string> >(s3_default_hostname_vector, hostname_vector);
    _prop_map.get<std::size_t>(s3_hostname_index, hostname_index);
    if (hostname_vector.empty()) {
        return {}; // Short-circuit default case
    }

    std::string ret = hostname_vector[hostname_index];
    hostname_index = (hostname_index+ 1) % hostname_vector.size();
    _prop_map.set<std::size_t>(s3_hostname_index, hostname_index);
    g_hostnameIdxLock.unlock();
    return ret;
}


// Callbacks for S3
void StoreAndLogStatus (
    S3Status status,
    const S3ErrorDetails *error,
    const char *function,
    const S3BucketContext *pCtx,
    S3Status *pStatus,
    bool ignore_not_found_error )
{
    int i;

    *pStatus = status;

    if (status == S3StatusHttpErrorNotFound && ignore_not_found_error) {
        return;
    }

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

void responseCompleteCallback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
}

void responseCompleteCallbackIgnoreLoggingNotFound(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status), true );
}

S3Status responsePropertiesCallback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    // Here we are saving the only 2 things iRODS actually cares about.
    savedProperties.lastModified = properties->lastModified;
    savedProperties.contentLength = properties->contentLength;
    return S3StatusOK;
}

static S3Status getObjectDataCallback(
    int bufferSize,
    const char *buffer,
    void *callbackData)
{
    callback_data_t *cb = (callback_data_t *)callbackData;
    irods::plugin_property_map *prop_map_ptr = cb ? cb->prop_map_ptr : nullptr;
    std::string resource_name = prop_map_ptr != nullptr ? get_resource_name(*prop_map_ptr) : "";

    irods::error result = ASSERT_ERROR(bufferSize != 0 && buffer != NULL && callbackData != NULL,
                                       SYS_INVALID_INPUT_PARAM, "[resource_name=%s] Invalid input parameter.", resource_name.c_str() );
    if(!result.ok()) {
        irods::log(result);
    }

    ssize_t wrote = pwrite(cb->fd, buffer, bufferSize, cb->offset);
    if (wrote>0) cb->offset += wrote;

#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_werr++;
    if (g_werr == g_werr_idx) {
        rodsLog(LOG_ERROR, "[resource_name=%s] Injecting a PWRITE error during S3 callback", resource_name.c_str() );
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
    std::int64_t ret = 0;

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
        rodsLog(LOG_ERROR, "[resource_name=%s] Injecting pread error in S3 callback", get_resource_name(_prop_map).c_str());
        ret = -1;
    }
    g_error_mutex.unlock();
#endif

    return (std::int64_t)ret;
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

    irods::plugin_property_map *prop_map_ptr = data ? data->prop_map_ptr : nullptr;
    std::string resource_name = prop_map_ptr != nullptr ? get_resource_name(*prop_map_ptr) : "";

    if (contentsCount <= 0) {
        data->keyCount = 0;
        return S3StatusOK;
    } else if (contentsCount > 1) {
        rodsLog (LOG_ERROR,
                 "[resource_name=%s] listBucketCallback: contentsCount %d > 1 for %s", resource_name.c_str(),
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
    std::string&       _bucket,
    std::string&       _key,
    irods::plugin_property_map& _prop_map ) {

    irods::error result = SUCCESS();
    std::size_t start_pos = 0;
    std::size_t slash_pos = 0;
    slash_pos = _s3ObjName.find_first_of("/");
    // skip a leading slash
    if(slash_pos == 0) {
        start_pos = 1;
        slash_pos = _s3ObjName.find_first_of("/", 1);
    }
    // have to have at least one slash to separate bucket from key
    if((result = ASSERT_ERROR(slash_pos != std::string::npos, SYS_INVALID_FILE_PATH, "[resource_name=%s] Problem parsing \"%s\".", get_resource_name(_prop_map).c_str(),
                              _s3ObjName.c_str())).ok()) {
        _bucket = _s3ObjName.substr(start_pos, slash_pos - start_pos);
        _key = _s3ObjName.substr(slash_pos + 1);
    }
    return result;
}

irods::error readS3AuthInfo (
    const std::string& _filename,
    std::string& _rtn_key_id,
    std::string& _rtn_access_key,
    irods::plugin_property_map& _prop_map )
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

    std::string resource_name = get_resource_name(_prop_map);

    if ((result = ASSERT_ERROR(fptr != NULL, SYS_CONFIG_FILE_ERR, "[resource_name=%s] Failed to open S3 auth file: \"%s\", errno = \"%s\".", resource_name.c_str(),
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
        if ((result = ASSERT_ERROR(linecnt == 2, SYS_CONFIG_FILE_ERR, "[resource_name=%s] Read %d lines in the auth file. Expected 2.", resource_name.c_str(),
                                   linecnt)).ok())  {
            _rtn_key_id = access_key_id;
            _rtn_access_key = secret_access_key;
        }
        return result;
    }

    if (fptr) {
        fclose(fptr);
    }

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

    std::string resource_name = get_resource_name(_prop_map);

    if ((tmpPtr = getenv(s3_key_id.c_str())) != NULL) {
        key_id = tmpPtr;
        if ((tmpPtr = getenv(s3_access_key.c_str())) != NULL) {
            access_key = tmpPtr;
        }
    } else {
        std::string auth_file;
        ret = _prop_map.get<std::string>(s3_auth_file, auth_file);
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to retrieve S3 auth filename property.", resource_name.c_str())).ok()) {
            ret = readS3AuthInfo(auth_file, key_id, access_key, _prop_map);
            if ((result = ASSERT_PASS(ret, "[resource_name=%s] Failed reading the authorization credentials file.", resource_name.c_str())).ok()) {
                ret = _prop_map.set<std::string>(s3_key_id, key_id);
                if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to set the \"%s\" property.", resource_name.c_str(), s3_key_id.c_str())).ok()) {
                    ret = _prop_map.set<std::string>(s3_access_key, access_key);
                    result = ASSERT_PASS(ret, "[resource_name=%s] Failed to set the \"%s\" property.", resource_name.c_str(), s3_access_key.c_str());
                }
            }
        }
    }
    return result;
}


irods::error s3Init (
    irods::plugin_property_map& _prop_map ) {

    irods::error result = SUCCESS();

    std::vector<std::string> hostname_vector;
    std::size_t hostname_index = 0;

    g_hostnameIdxLock.lock();

    // First, parse the default hostname (if present) into a list of
    // hostnames separated on the definition line by commas (,)
    std::string hostname_list;
    irods::error ret;
    ret = _prop_map.get< std::string >(
        s3_default_hostname,
        hostname_list );
    if( !ret.ok() ) {
        // ok to fail
        hostname_vector.push_back(S3_DEFAULT_HOSTNAME); // Default to Amazon
    } else {
        std::stringstream ss(hostname_list);
        std::string item;
        while (std::getline(ss, item, ',')) {
            hostname_vector.push_back(item);
        }
        // Because each resource operation is a new instance, randomize the starting
        // hostname offset so we don't always hit the first in the list between different
        // operations.
        srand(time(NULL));
        hostname_index = rand() % hostname_vector.size();
    }

    _prop_map.set<std::vector<std::string> >(s3_default_hostname_vector, hostname_vector);
    _prop_map.set<std::size_t>(s3_hostname_index, hostname_index);

    g_hostnameIdxLock.unlock();

    return result;
}

// initialization done on every operation
irods::error s3InitPerOperation (
    irods::plugin_property_map& _prop_map ) {

    irods::error result = SUCCESS();

    std::string resource_name = get_resource_name(_prop_map);

    std::size_t retry_count = 10;
    std::string retry_count_str;
    result = _prop_map.get< std::size_t >(
        s3_retry_count,
        retry_count );

    std::size_t wait_time = get_retry_wait_time_sec(_prop_map);

    std::size_t ctr = 0;
    while( ctr < retry_count ) {
        S3Status status;
        int flags = S3_INIT_ALL;

        std::string&& hostname = s3GetHostname(_prop_map);
        const char* host_name = hostname.c_str(); // Iterate through on each try
        status = S3_initialize( "s3", flags, host_name );

        auto msg = fmt::format("[resource_name={}]  - Error initializing the S3 library. Status = {}.",
                resource_name, status);

        if(status >= 0) {
            msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)status));
        }

        result = ASSERT_ERROR(status == S3StatusOK, status, msg);
        if( result.ok() ) {

            // If using V4 we also need to set the S3 region name
            std::string region_name = "us-east-1";

            // Get S3 region name from plugin property map
            if (!_prop_map.get< std::string >(s3_region_name, region_name ).ok()) {
                rodsLog( LOG_ERROR, "[resource_name=%s] Failed to retrieve S3 region name from resource plugin properties, using 'us-east-1'", resource_name.c_str());
            }

            if (status != S3StatusOK) {
                std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to set region name to %s: %s") % resource_name.c_str() %
                    region_name.c_str() % S3_get_status_name(status));
                rodsLog(LOG_ERROR, error_str.c_str());
                return ERROR(S3_INIT_ERROR, error_str.c_str());
            }

            break;
        }

        ctr++;

        s3_sleep( wait_time );

        rodsLog(
            LOG_NOTICE,
            "%s - Error in connection, retry count %d",
            __FUNCTION__,
            ctr );

    } // while

    return result;
}


S3Protocol s3GetProto( irods::plugin_property_map& _prop_map)
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

S3UriStyle s3_get_uri_request_style( irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string uri_request_style_string;
    ret = _prop_map.get< std::string >(
        s3_uri_request_style,
        uri_request_style_string );
    if (!ret.ok()) { // Default to original behavior
        return S3UriStylePath;
    }
    if ( boost::iequals(uri_request_style_string.c_str(), "virtual") ||
            boost::iequals(uri_request_style_string.c_str(), "host") ||
            boost::iequals(uri_request_style_string.c_str(), "virtualhost") ) {

        return S3UriStyleVirtualHost;

    }
    return S3UriStylePath;
}

// returns the upper limit of the MPU chunk size parameter, in megabytes
// used for validating the value of S3_MPU_CHUNK
// also used for determining the maximum size for CopyObject
std::int64_t s3GetMaxUploadSizeMB (irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string max_size_str;   // max size from context string, in MB

    ret = _prop_map.get<std::string>(s3_max_upload_size, max_size_str);
    if (ret.ok()) {
        // should be between 5MB and 5TB
        std::int64_t max_megs = std::atol(max_size_str.c_str());
        if ( max_megs >= 5 && max_megs <= 5L * 1024 * 1024 ) {
            return max_megs;
        }
    }
    return 5L * 1024;    // default to 5GB
}

// returns the chunk size for multipart upload, in bytes
std::int64_t s3GetMPUChunksize (irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string chunk_str;
    std::int64_t bytes = 5L * 1024 * 1024; // default to amazon value
    ret = _prop_map.get< std::string >(s3_mpu_chunk, chunk_str );

    if (ret.ok()) {
        // AWS S3 allows chunk sizes from 5MB to 5GB.
        // Other S3 appliances may have a different upper limit.
        std::int64_t megs = std::atol(chunk_str.c_str());
        if ( megs >= 5 && megs <= s3GetMaxUploadSizeMB(_prop_map) )
            bytes = megs * 1024 * 1024;
    }
    return bytes;
}

ssize_t s3GetMPUThreads (
    irods::plugin_property_map& _prop_map )
{
    irods::error ret;
    std::string threads_str;
    int threads = 10; // 10 upload threads by default
    ret = _prop_map.get< std::string >(
        s3_mpu_threads,
        threads_str );
    if (ret.ok()) {
        int parse = std::atol(threads_str.c_str());
        if ( (parse >= 1) && (parse <= 100) )
            threads = parse;
    }
    return threads;
}

bool s3GetEnableMultiPartUpload (
    irods::plugin_property_map& _prop_map )
{
    irods::error ret;
    std::string enable_str;
    bool enable = true;

    ret = _prop_map.get< std::string >(
        s3_enable_mpu,
        enable_str );
    if (ret.ok()) {
        // Only 0 = no, 1 = yes.  Adding in strings would require localization I think
        try {
            int parse = boost::lexical_cast<int>(enable_str.c_str());
            if (parse == 0) {
                enable = false;
            }
        } catch(const boost::bad_lexical_cast &) {
            // keep enable true
        }

    }
    return enable;
}

bool s3GetServerEncrypt (
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
        int parse = std::atol(enable_str.c_str());
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
    void *bucketContextParam, void *pluginPropertyMapParam)
{
    S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);
    irods::plugin_property_map _prop_map = *((irods::plugin_property_map*)pluginPropertyMapParam);

    std::string resource_name = get_resource_name(_prop_map);

    irods::error result;
    S3GetObjectHandler getObjectHandler = { {mrdRangeRespPropCB, mrdRangeRespCompCB }, mrdRangeGetDataCB };

    std::size_t retry_count_limit = get_retry_count(_prop_map);
    std::size_t retry_wait = get_retry_wait_time_sec(_prop_map);
    std::size_t max_retry_wait = get_max_retry_wait_time_sec(_prop_map);

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
        g_mrdNext = g_mrdNext + 1;
        g_mrdLock.unlock();

        std::size_t retry_cnt = 0;
        multirange_data_t rangeData;
        do {
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            rangeData = g_mrdData[seq-1];
            rangeData.pCtx = &bucketContext;
            rangeData.prop_map_ptr = &_prop_map;

            irods::log(LOG_DEBUG,
                    fmt::format("Multirange:  Start range {}  \"{}\", offset {}, len {}",
                    seq,
                    g_mrdKey,
                    rangeData.get_object_data.offset,
                    rangeData.get_object_data.contentLength));

            std::uint64_t usStart = usNow();
            std::string&& hostname = s3GetHostname(_prop_map);
            bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
            S3_get_object( &bucketContext, g_mrdKey, NULL, rangeData.get_object_data.offset,
                           rangeData.get_object_data.contentLength, 0, 0, &getObjectHandler, &rangeData );
            std::uint64_t usEnd = usNow();
            double bw = (g_mrdData[seq-1].get_object_data.contentLength / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );

            irods::log(LOG_DEBUG, fmt::format(" -- END -- BW={} MB/s", bw));
            if (rangeData.status != S3StatusOK) {
                s3_sleep( retry_wait );
                retry_wait *= 2;
                if (retry_wait > max_retry_wait) {
                    retry_wait = max_retry_wait;
                }
            }
        } while ((rangeData.status != S3StatusOK) && S3_status_is_retryable(rangeData.status) && (++retry_cnt <= retry_count_limit));
        if (rangeData.status != S3StatusOK) {

            auto msg = fmt::format("[resource_name={}] {} - Error getting the S3 object: \"{}\" range {}",
                    resource_name,
                    __FUNCTION__,
                    g_mrdKey,
                    seq);

            if(rangeData.status >= 0) {
                msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)rangeData.status));
            }
            result = ERROR( S3_GET_ERROR, msg );
            irods::log( LOG_ERROR, msg );
            g_mrdLock.lock();
            g_mrdResult = result;
            g_mrdLock.unlock();
        }
    }
}


S3STSDate s3GetSTSDate(irods::plugin_property_map& _prop_map)
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

std::string get_cache_directory(irods::plugin_property_map& _prop_map) {

        // if cachedir is defined, use that else use /tmp/<resc_name>
        std::string s3_cache_dir_str;
        irods::error ret = _prop_map.get< std::string >(s3_cache_dir, s3_cache_dir_str);
        if (!ret.ok()) {
            s3_cache_dir_str = boost::filesystem::temp_directory_path().string();
        }

        // No longer using shared_memory_name_salt because it uses irods main server process PID and a hash
        // which changes every time the server is restarted.  While that is preferable in case something goes
        // terribly wrong, it introduces cleanup issues.  Instead simply use the resource name which
        // can be persistent.
        s3_cache_dir_str += "/" + get_resource_name(_prop_map);

        return s3_cache_dir_str;
}

std::size_t get_retry_wait_time_sec(irods::plugin_property_map& _prop_map) {

    std::size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SECONDS;
    std::string wait_time_str;
    irods::error ret = _prop_map.get< std::string >( s3_wait_time_seconds, wait_time_str );
    if( ret.ok() ) {
        try {
            retry_wait = boost::lexical_cast<std::size_t>( wait_time_str );
        } catch ( const boost::bad_lexical_cast& ) {
            std::string resource_name = get_resource_name(_prop_map);
            rodsLog(
                LOG_ERROR,
                "[resource_name=%s] failed to cast %s [%s] to a std::size_t", resource_name.c_str(),
                s3_wait_time_seconds.c_str(), wait_time_str.c_str() );
        }
    } else {
        // for backward compatibility, look for S3_WAIT_TIME_SEC
        irods::error ret = _prop_map.get< std::string >( s3_wait_time_sec, wait_time_str );
        std::string resource_name = get_resource_name(_prop_map);
        if( ret.ok() ) {
            irods::log(LOG_WARNING, fmt::format("[resource_name={} - {} is deprecated.  Use {}",
                        resource_name, s3_wait_time_sec, s3_wait_time_seconds));
            try {
                retry_wait = boost::lexical_cast<std::size_t>( wait_time_str );
            } catch ( const boost::bad_lexical_cast& ) {
                rodsLog(
                    LOG_ERROR,
                    "[resource_name=%s] failed to cast %s [%s] to a std::size_t", resource_name.c_str(),
                    s3_wait_time_sec.c_str(), wait_time_str.c_str() );
            }
        }
    }

    return retry_wait;
}

std::size_t get_max_retry_wait_time_sec(irods::plugin_property_map& _prop_map) {

    std::size_t max_retry_wait = S3_DEFAULT_MAX_RETRY_WAIT_SECONDS;
    std::string max_retry_wait_str;
    irods::error ret = _prop_map.get< std::string >( s3_max_wait_time_seconds, max_retry_wait_str );
    if( ret.ok() ) {
        try {
            max_retry_wait = boost::lexical_cast<std::size_t>( max_retry_wait_str );
        } catch ( const boost::bad_lexical_cast& ) {
            std::string resource_name = get_resource_name(_prop_map);
            rodsLog(
                LOG_ERROR,
                "[resource_name=%s] failed to cast %s [%s] to a std::size_t", resource_name.c_str(),
                s3_max_wait_time_seconds.c_str(), max_retry_wait_str.c_str() );
        }
    } else {
        // for backward compatibility, look for S3_MAX_WAIT_TIME_SEC
        irods::error ret = _prop_map.get< std::string >( s3_max_wait_time_sec, max_retry_wait_str );
        std::string resource_name = get_resource_name(_prop_map);
        if( ret.ok() ) {
            irods::log(LOG_WARNING, fmt::format("[resource_name={} - {} is being deprecated.  Use {}",
                        resource_name, s3_max_wait_time_sec, s3_max_wait_time_seconds));
            try {
                max_retry_wait = boost::lexical_cast<std::size_t>( max_retry_wait_str );
            } catch ( const boost::bad_lexical_cast& ) {
                rodsLog(
                    LOG_ERROR,
                    "[resource_name=%s] failed to cast %s [%s] to a std::size_t", resource_name.c_str(),
                    s3_max_wait_time_sec.c_str(), max_retry_wait_str.c_str() );
            }
        }
    }
    return max_retry_wait;
}

std::size_t get_retry_count(irods::plugin_property_map& _prop_map) {

    std::size_t retry_count = S3_DEFAULT_RETRY_COUNT;

    std::string retry_count_str;
    irods::error ret = _prop_map.get< std::string >( s3_retry_count, retry_count_str );
    if( ret.ok() ) {
        try {
            retry_count = boost::lexical_cast<std::size_t>( retry_count_str );
        } catch ( const boost::bad_lexical_cast& ) {
            std::string resource_name = get_resource_name(_prop_map);
            rodsLog(
                LOG_ERROR,
                "[resource_name=%s] failed to cast %s [%s] to a std::size_t", resource_name.c_str(),
                s3_retry_count.c_str(), retry_count_str.c_str() );
        }
    }

    return retry_count;
}

unsigned int get_non_data_transfer_timeout_seconds(irods::plugin_property_map& _prop_map) {

    unsigned int non_data_transfer_timeout_seconds = S3_DEFAULT_NON_DATA_TRANSFER_TIMEOUT_SECONDS;
    std::string non_data_transfer_timeout_seconds_str;
    irods::error ret = _prop_map.get< std::string >( s3_non_data_transfer_timeout_seconds, non_data_transfer_timeout_seconds_str );
    if( ret.ok() ) {
        try {
            non_data_transfer_timeout_seconds = boost::lexical_cast<unsigned int>( non_data_transfer_timeout_seconds_str );
        } catch ( const boost::bad_lexical_cast& ) {
            std::string resource_name = get_resource_name(_prop_map);
            rodsLog(
                LOG_ERROR,
                "[resource_name=%s] failed to cast %s [%s] to an unsigned int", resource_name.c_str(),
                s3_non_data_transfer_timeout_seconds.c_str(), non_data_transfer_timeout_seconds_str.c_str() );
        }
    }

    return non_data_transfer_timeout_seconds;
}

unsigned int s3_get_restoration_days(irods::plugin_property_map& _prop_map) {

    unsigned int restoration_days = S3_DEFAULT_RESTORATION_DAYS;
    std::string restoration_days_str;
    irods::error ret = _prop_map.get< std::string >( s3_restoration_days, restoration_days_str );
    if( ret.ok() ) {
        try {
            restoration_days = boost::lexical_cast<unsigned int>( restoration_days_str );
        } catch ( const boost::bad_lexical_cast& ) {
            std::string resource_name = get_resource_name(_prop_map);
            rodsLog(
                LOG_ERROR,
                "[resource_name=%s] failed to cast %s [%s] to a std::size_t.  Using default of %u.", resource_name.c_str(),
                s3_restoration_days.c_str(), restoration_days_str.c_str(), S3_DEFAULT_RESTORATION_DAYS);
        }
    }

    return restoration_days;
}

std::string s3_get_restoration_tier(irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string restoration_tier_str;
    ret = _prop_map.get< std::string >(
        s3_restoration_tier,
        restoration_tier_str );
    if (!ret.ok()) { // Default to original behavior
        return S3_DEFAULT_RESTORATION_TIER;
    }
    if ( boost::iequals(restoration_tier_str.c_str(), S3_RESTORATION_TIER_EXPEDITED)) {
        return S3_RESTORATION_TIER_EXPEDITED;
    } else if ( boost::iequals(restoration_tier_str.c_str(), S3_RESTORATION_TIER_STANDARD)) {
        return S3_RESTORATION_TIER_STANDARD;
    } else if ( boost::iequals(restoration_tier_str.c_str(), S3_RESTORATION_TIER_BULK)) {
        return S3_RESTORATION_TIER_BULK;
    } else {
        std::string resource_name = get_resource_name(_prop_map);
        rodsLog(
            LOG_ERROR,
            "[resource_name=%s] Unknown setting for %s [%s].  Using default of \"%s\".", resource_name.c_str(),
            s3_restoration_tier.c_str(), restoration_tier_str.c_str(), S3_DEFAULT_RESTORATION_TIER.c_str());
        return S3_DEFAULT_RESTORATION_TIER;
    }
}

// default is false - CopyObject is enabled
bool s3_copyobject_disabled(irods::plugin_property_map& _prop_map)
{
    irods::error ret;
    std::string tmp;
    ret = _prop_map.get< std::string >(
        s3_enable_copyobject,
        tmp );
    if (!ret.ok() || tmp != "0") { // Default CopyObject enabled
        return false;
    }
    return true;
} // end s3_copyobject_disabled

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

    std::string resource_name = get_resource_name(_prop_map);

    std::size_t retry_count_limit = get_retry_count(_prop_map);
    std::size_t retry_wait = get_retry_wait_time_sec(_prop_map);
    std::size_t max_retry_wait = get_max_retry_wait_time_sec(_prop_map);

    int cache_fd = -1;
    std::string bucket;
    std::string key;
    ret = parseS3Path(_s3ObjName, bucket, key, _prop_map);
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed parsing the S3 bucket and key from the physical path: \"%s\".", resource_name.c_str(),
                             _s3ObjName.c_str())).ok()) {

        ret = s3InitPerOperation( _prop_map );
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to initialize the S3 system.", resource_name.c_str())).ok()) {

            cache_fd = open(_filename.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
            if((result = ASSERT_ERROR(cache_fd != -1, UNIX_FILE_OPEN_ERR, "[resource_name=%s] Failed to open the cache file: \"%s\".", resource_name.c_str(),
                                      _filename.c_str())).ok()) {

                callback_data_t data;
                S3BucketContext bucketContext{};
                bucketContext.bucketName = bucket.c_str();
                bucketContext.protocol = s3GetProto(_prop_map);
                bucketContext.stsDate = s3GetSTSDate(_prop_map);
                bucketContext.uriStyle = s3_get_uri_request_style(_prop_map);
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();
                std::string authRegionStr = get_region_name(_prop_map);
                bucketContext.authRegion = authRegionStr.c_str();

                std::int64_t chunksize = s3GetMPUChunksize( _prop_map );

                if ( _fileSize < chunksize ) {
                    S3GetObjectHandler getObjectHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &getObjectDataCallback
                    };

                    std::size_t retry_cnt = 0;
                    do {
                        std::memset(&data, 0, sizeof(data));
                        data.prop_map_ptr = &_prop_map;
                        data.fd = cache_fd;
                        data.contentLength = data.originalContentLength = _fileSize;
                        std::uint64_t usStart = usNow();
                        std::string&& hostname = s3GetHostname(_prop_map);
                        bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                        data.pCtx = &bucketContext;
                        S3_get_object (&bucketContext, key.c_str(), NULL, 0, _fileSize, 0, 0, &getObjectHandler, &data);
                        std::uint64_t usEnd = usNow();
                        double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                        rodsLog( LOG_DEBUG, "GETBW=%lf", bw);
                        if (data.status != S3StatusOK) {
                            s3_sleep( retry_wait );
                            retry_wait *= 2;
                            if (retry_wait > max_retry_wait) {
                                retry_wait = max_retry_wait;
                            }
                        }
                    } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt <= retry_count_limit) );
                    if (data.status != S3StatusOK) {

                        auto msg = fmt::format("[resource_name={}]  {} - Error fetching the S3 object: \"{}\"",
                                resource_name,
                                __FUNCTION__,
                                _s3ObjName);

                        if(data.status >= 0) {
                            msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
                        }

                        result = ERROR(S3_GET_ERROR, msg);
                    }
                } else {

                    // Only the FD part of this will be constant
                    std::memset(&data, 0, sizeof(data));
                    data.prop_map_ptr = &_prop_map;
                    data.fd = cache_fd;
                    data.contentLength = data.originalContentLength = _fileSize;

                    // Multirange get
                    g_mrdResult = SUCCESS();

                    std::int64_t seq;
                    std::int64_t totalSeq = (data.contentLength + chunksize - 1) / chunksize;

                    multirange_data_t rangeData;
                    int rangeLength = 0;

                    g_mrdData = (multirange_data_t*)calloc(totalSeq, sizeof(multirange_data_t));
                    if (!g_mrdData) {
                        std::string msg =  boost::str(boost::format("[resource_name=%s] Out of memory error in S3 multirange g_mrdData allocation.") % resource_name.c_str());
                        rodsLog( LOG_ERROR, msg.c_str() );
                        result = ERROR( SYS_MALLOC_ERR, msg.c_str() );
                        return result;
                    }

                    g_mrdNext = 0;
                    g_mrdLast = totalSeq;
                    g_mrdKey = key.c_str();
                    for(seq = 0; seq < totalSeq ; seq ++) {
                        memset(&rangeData, 0, sizeof(rangeData));
                        rangeData.prop_map_ptr = &_prop_map;
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

                    std::uint64_t usStart = usNow();
                    std::list<boost::thread*> threads;
                    for (int thr_id=0; thr_id<nThreads; thr_id++) {
                        boost::thread *thisThread = new boost::thread(mrdWorkerThread, &bucketContext, &_prop_map);
                        threads.push_back(thisThread);
                    }

                    // And wait for them to finish...
                    while (!threads.empty()) {
                        boost::thread *thisThread = threads.front();
                        thisThread->join();
                        delete thisThread;
                        threads.pop_front();
                    }
                    std::uint64_t usEnd = usNow();
                    double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                    rodsLog( LOG_DEBUG, "MultirangeBW=%lf", bw);

                    if (!g_mrdResult.ok()) {
                        // Someone aborted after we started, delete the partial object on S3
                        rodsLog(LOG_ERROR, "[resource_name=%s] Cancelling multipart download", resource_name.c_str());
                        // 0-length the file, it's garbage
                        if (ftruncate( cache_fd, 0 ))
                            rodsLog(LOG_ERROR, "[resource_name=%s] Unable to 0-length the result file", resource_name.c_str());
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
    std::int64_t ret = 0;
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
        rodsLog(LOG_ERROR, "[resource_name=%s] Injecting a XML upload error during S3 callback", get_resource_name(_prop_map).c_str());
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

static void mpuCancel( S3BucketContext *bucketContext, const char *key, const char *upload_id,
    irods::plugin_property_map& _prop_map )
{
    S3AbortMultipartUploadHandler abortHandler = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };
    S3Status status;

    std::string resource_name = get_resource_name(_prop_map);

    irods::log( LOG_ERROR, fmt::format("[resource_name={}] Cancelling multipart upload: key=\"{}\", upload_id = \"{}\"", resource_name, key, upload_id) );
    g_mpuCancelRespCompCB_status = S3StatusOK;
    g_mpuCancelRespCompCB_pCtx = bucketContext;
    S3_abort_multipart_upload(bucketContext, key, upload_id, 0, &abortHandler);
    status = g_mpuCancelRespCompCB_status;
    if (status != S3StatusOK) {

        auto msg = fmt::format("[resource_name={}] {}  - Error cancelling the mulipart upload of S3 object: \"{}\"",
                resource_name,
                __FUNCTION__,
                key);
        if(status >= 0) {
            msg += fmt::format(" - \"{}\"", S3_get_status_name(status));
        }
        irods::log( LOG_ERROR, msg );
    }
}


/* Multipart worker thread, grabs a job from the queue and uploads it */
static void mpuWorkerThread (
    void *bucketContextParam, void *pluginPropertyMapParam)
{
    S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);
    irods::plugin_property_map _prop_map = *((irods::plugin_property_map*)pluginPropertyMapParam);

    std::string resource_name = get_resource_name(_prop_map);

    irods::error result;
    S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

    std::size_t retry_count_limit = get_retry_count(_prop_map);
    std::size_t retry_wait = get_retry_wait_time_sec(_prop_map);
    std::size_t max_retry_wait = get_max_retry_wait_time_sec(_prop_map);

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
        g_mpuNext = g_mpuNext + 1;
        g_mpuLock.unlock();

        multipart_data_t partData;
        std::size_t retry_cnt = 0;
        do {
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            partData = g_mpuData[seq-1];
            partData.put_object_data.pCtx = &bucketContext;

            irods::log( LOG_DEBUG, fmt::format("Multipart:  Start part {}, key \"{}\", uploadid \"{}\", offset {}, len {}",
                        (int)seq,
                        g_mpuKey,
                        g_mpuUploadId,
                        (std::int64_t)partData.put_object_data.offset,
                        (int)partData.put_object_data.contentLength) );

            S3PutProperties *putProps = NULL;
            putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
            putProps->expires = -1;
            std::uint64_t usStart = usNow();
            std::string&& hostname = s3GetHostname(_prop_map);
            bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
            if (partData.mode == S3_COPYOBJECT) {
                std::uint64_t startOffset = partData.put_object_data.offset;

                // TODO -1 below to fix a bug in libs3 code
                std::uint64_t count = partData.put_object_data.contentLength;
                S3ResponseHandler copyResponseHandler = {mpuInitRespPropCB /*Do nothing*/, mpuPartRespCompCB};
                std::int64_t lastModified;
                // The default copy callback tries to set this for us, need to allocate here
                partData.manager->etags[seq-1] = (char *)malloc(512); // TBD - magic #!  Is there a max etag defined?
                S3_copy_object_range(partData.pSrcCtx, partData.srcKey, bucketContext.bucketName, g_mpuKey,
                                     seq, g_mpuUploadId,
                                     startOffset, count,
                                     putProps,
                                     &lastModified, 512 /*TBD - magic # */, partData.manager->etags[seq-1], 0,
                                     0, &copyResponseHandler, &partData);
            } else {
                S3_upload_part(&bucketContext, g_mpuKey, putProps, &putObjectHandler, seq, g_mpuUploadId,
                        partData.put_object_data.contentLength, 0, 0, &partData);
            }
            std::uint64_t usEnd = usNow();
            double bw = (g_mpuData[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / ( (usEnd - usStart) / 1000000.0 );
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            irods::log( LOG_DEBUG, fmt::format(" -- END -- BW={} MB/s", bw) );
            if (partData.status != S3StatusOK) {
                s3_sleep( retry_wait );
                retry_wait *= 2;
                if (retry_wait > max_retry_wait) {
                    retry_wait = max_retry_wait;
                }
            }
        } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt <= retry_count_limit));
        if (partData.status != S3StatusOK) {

            auto msg = fmt::format("[resource_name={}] {} - Error putting the S3 object: \"{}\" part {}",
                    resource_name,
                    __FUNCTION__,
                    g_mpuKey,
                    seq);

            if(partData.status >= 0) {
                msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)partData.status));
            }

            g_mpuResult = ERROR( S3_PUT_ERROR, msg );
            irods::log( LOG_ERROR, msg );
        }
    }
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
    std::int64_t chunksize = s3GetMPUChunksize( _prop_map );
    std::size_t retry_cnt    = 0;
    bool server_encrypt = s3GetServerEncrypt ( _prop_map );

    std::string resource_name = get_resource_name(_prop_map);

    std::size_t retry_count_limit = get_retry_count(_prop_map);
    std::size_t retry_wait = get_retry_wait_time_sec(_prop_map);
    std::size_t max_retry_wait = get_max_retry_wait_time_sec(_prop_map);

    ret = parseS3Path(_s3ObjName, bucket, key, _prop_map);
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed parsing the S3 bucket and key from the physical path: \"%s\".", resource_name.c_str(),
                             _s3ObjName.c_str())).ok()) {

        ret = s3InitPerOperation( _prop_map );
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to initialize the S3 system.", resource_name.c_str())).ok()) {

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
            if((result = ASSERT_ERROR(cache_fd  != -1, err_status, "[resource_name=%s] Failed to open the cache file: \"%s\".", resource_name.c_str(),
                                      _filename.c_str())).ok()) {

                callback_data_t data;
                S3BucketContext bucketContext{};
                bucketContext.bucketName = bucket.c_str();
                bucketContext.protocol = s3GetProto(_prop_map);
                bucketContext.stsDate = s3GetSTSDate(_prop_map);
                bucketContext.uriStyle = s3_get_uri_request_style(_prop_map);
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();
                std::string authRegionStr = get_region_name(_prop_map);
                bucketContext.authRegion = authRegionStr.c_str();


                S3PutProperties *putProps = NULL;
                putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
                if ( putProps && server_encrypt )
                    putProps->useServerSideEncryption = true;
                putProps->expires = -1;

                // HML: add a check to see whether or not multipart upload is enabled.
                bool mpu_enabled = s3GetEnableMultiPartUpload(_prop_map);
                if ((!mpu_enabled) || ( _fileSize < chunksize )) {
                    S3PutObjectHandler putObjectHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &putObjectDataCallback
                    };

                    do {
                        std::memset(&data, 0, sizeof(data));
                        data.prop_map_ptr = &_prop_map;
                        data.fd = cache_fd;
                        data.contentLength = data.originalContentLength = _fileSize;
                        data.pCtx = &bucketContext;

                        std::uint64_t usStart = usNow();
                        std::string&& hostname = s3GetHostname(_prop_map);
                        bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                        S3_put_object (&bucketContext, key.c_str(), _fileSize, putProps, 0, 0, &putObjectHandler, &data);
                        std::uint64_t usEnd = usNow();
                        double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                        rodsLog( LOG_DEBUG, "BW=%lf", bw);
                        if (data.status != S3StatusOK) {
                            s3_sleep( retry_wait );
                            retry_wait *= 2;
                            if (retry_wait > max_retry_wait) {
                                retry_wait = max_retry_wait;
                            }
                        }
                    } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt <= retry_count_limit) );
                    if (data.status != S3StatusOK) {
                        auto msg = fmt::format("[resource_name={}]  - Error putting the S3 object: \"{}\"",
                                resource_name,
                                _s3ObjName);
                        if(data.status >= 0) {
                            msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
                        }
                        result = ERROR(S3_PUT_ERROR, msg);
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

                    std::int64_t seq;
                    std::int64_t totalSeq = (_fileSize + chunksize - 1) / chunksize;

                    multipart_data_t partData;
                    int partContentLength = 0;

                    std::memset(&data, 0, sizeof(data));
                    data.prop_map_ptr = &_prop_map;
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
                        const auto msg = fmt::format("[resource_name={}] Out of memory error in S3 multipart ETags allocation.", resource_name);
                        irods::log( LOG_ERROR, msg );
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
                        const auto msg = fmt::format("[resource_name={}] Out of memory error in S3 multipart g_mpuData allocation.", resource_name);
                        irods::log( LOG_ERROR, msg );
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
                        const auto msg = fmt::format("[resource_name={}] Out of memory error in S3 multipart XML allocation.", resource_name);
                        irods::log( LOG_ERROR, msg );
                        result = ERROR( SYS_MALLOC_ERR, msg );
                        return result;
                    }

                    retry_cnt = 0;
                    // These expect a upload_manager_t* as cbdata
                    S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };
                    do {
                        std::string&& hostname = s3GetHostname(_prop_map);
                        bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                        manager.pCtx = &bucketContext;
                        S3_initiate_multipart(&bucketContext, key.c_str(), putProps, &mpuInitialHandler, NULL, 0, &manager);
                        if (manager.status != S3StatusOK) {
                            s3_sleep( retry_wait );
                            retry_wait *= 2;
                            if (retry_wait > max_retry_wait) {
                                retry_wait = max_retry_wait;
                            }
                        }
                    } while ( (manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt <= retry_count_limit));
                    if (manager.upload_id == NULL || manager.status != S3StatusOK) {
                        // Clear up the S3PutProperties, if it exists
                        if (putProps) {
                            if (putProps->md5) free( (char*)putProps->md5 );
                            free( putProps );
                        }
                        auto msg = fmt::format("[resource_name={}] {} - Error initiating multipart upload of the S3 object: \"{}\"",
                                resource_name,
                                __FUNCTION__,
                                _s3ObjName);

                        if(manager.status >= 0) {
                            msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)manager.status));
                        }
                        irods::log( LOG_ERROR, msg );
                        result = ERROR( S3_PUT_ERROR, msg );
                        return result; // Abort early
                    }

                    // Following used by S3_COPYOBJECT only
                    S3BucketContext srcBucketContext;
                    std::string authRegionStr = get_region_name(_prop_map);
                    if (_mode == S3_COPYOBJECT) {
                        ret = parseS3Path(_filename, srcBucket, srcKey, _prop_map);
                        if(!(result = ASSERT_PASS(ret, "[resource_name=%s] Failed parsing the S3 bucket and key from the physical path: \"%s\".", resource_name.c_str(),
                                                  _filename.c_str())).ok()) {
                            return result;  // Abort early
                        }
                        std::memset(&srcBucketContext, 0, sizeof(srcBucketContext));
                        srcBucketContext.bucketName = srcBucket.c_str();
                        srcBucketContext.protocol = s3GetProto(_prop_map);
                        srcBucketContext.stsDate = s3GetSTSDate(_prop_map);
                        srcBucketContext.uriStyle = s3_get_uri_request_style(_prop_map);
                        srcBucketContext.accessKeyId = _key_id.c_str();
                        srcBucketContext.secretAccessKey = _access_key.c_str();
                        srcBucketContext.authRegion = authRegionStr.c_str();
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
                        partData.server_encrypt = s3GetServerEncrypt( _prop_map );
                        g_mpuData[seq-1] = partData;
                        data.contentLength -= partContentLength;
                    }

                    std::uint64_t usStart = usNow();

                    // Make the worker threads and start
                    int nThreads = s3GetMPUThreads(_prop_map);

                    std::list<boost::thread*> threads;
                    for (int thr_id=0; thr_id<nThreads; thr_id++) {
                        boost::thread *thisThread = new boost::thread(mpuWorkerThread, &bucketContext, &_prop_map);
                        threads.push_back(thisThread);
                    }

                    // And wait for them to finish...
                    while (!threads.empty()) {
                        boost::thread *thisThread = threads.front();
                        thisThread->join();
                        delete thisThread;
                        threads.pop_front();
                    }

                    std::uint64_t usEnd = usNow();
                    double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                    rodsLog( LOG_DEBUG, "MultipartBW=%lf", bw);

                    manager.remaining = 0;
                    manager.offset  = 0;

                    if (g_mpuResult.ok()) { // If someone aborted, don't complete...
                        irods::log( LOG_DEBUG, fmt::format("Multipart:  Completing key \"{}\"", key) );

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
                            std::string&& hostname = s3GetHostname(_prop_map);
                            bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                            manager.pCtx = &bucketContext;
                            S3_complete_multipart_upload(&bucketContext, key.c_str(), &commit_handler, manager.upload_id, manager.remaining, NULL, 0, &manager);
                            if (manager.status != S3StatusOK) {
                                s3_sleep( retry_wait );
                                retry_wait *= 2;
                                if (retry_wait > max_retry_wait) {
                                    retry_wait = max_retry_wait;
                                }
                            }
                        } while ((manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt <= retry_count_limit));
                        if (manager.status != S3StatusOK) {
                            auto msg = fmt::format("[resource_name={}] {} - Error putting the S3 object: \"{}\"",
                                    resource_name,
                                    __FUNCTION__,
                                    _s3ObjName);

                            if(manager.status >= 0) {
                                msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)manager.status));
                            }
                            g_mpuResult = ERROR( S3_PUT_ERROR, msg );
                        }
                    }
                    if ( !g_mpuResult.ok() && manager.upload_id ) {
                        // Someone aborted after we started, delete the partial object on S3
                        rodsLog(LOG_ERROR, "[resource_name=%s] Cancelling multipart upload", resource_name.c_str());
                        mpuCancel( &bucketContext, key.c_str(), manager.upload_id, _prop_map );
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


/// @brief Function to copy the specified src file to the specified dest file
irods::error s3CopyFile(
    irods::plugin_context& _src_ctx,
    const std::string& _src_file,
    const std::string& _dest_file,
    const std::string& _key_id,
    const std::string& _access_key,
    const S3Protocol _proto,
    const S3STSDate _stsDate,
    const S3UriStyle _s3_uri_style)
{
    irods::error result = SUCCESS();
    irods::error ret;
    std::string src_bucket;
    std::string src_key;
    std::string dest_bucket;
    std::string dest_key;

    std::size_t retry_count_limit = get_retry_count(_src_ctx.prop_map());
    std::size_t retry_wait = get_retry_wait_time_sec(_src_ctx.prop_map());
    std::size_t max_retry_wait = get_max_retry_wait_time_sec(_src_ctx.prop_map());

    std::string resource_name = get_resource_name(_src_ctx.prop_map());

    bool mpu_enabled = s3GetEnableMultiPartUpload(_src_ctx.prop_map());

    // Check the size, and if too large punt to the multipart copy/put routine
    struct stat statbuf = {};
    ret = irods_s3::s3_file_stat_operation( _src_ctx, &statbuf );
    if (( result = ASSERT_PASS(ret, "[resource_name=%s] Unable to get original object size for source file name: \"%s\".", resource_name.c_str(),
                               _src_file.c_str())).ok()) {

        // if we are too big for a copy then we must upload
        // however, only do this is mpu is disabled
        if ( mpu_enabled && statbuf.st_size > s3GetMaxUploadSizeMB(_src_ctx.prop_map()) * 1024 * 1024 ) {   // amazon allows copies up to 5 GB
            // Early return for cleaner code...
            return s3PutCopyFile( S3_COPYOBJECT, _src_file, _dest_file, statbuf.st_size, _key_id, _access_key, _src_ctx.prop_map() );
        }

        // Note:  If file size > s3GetMaxUploadSizeMB() but multipart is disabled, it is not clear how to proceed.
        // Go ahead and try a copy.

        // Parse the src file
        ret = parseS3Path(_src_file, src_bucket, src_key, _src_ctx.prop_map());
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to parse the source file name: \"%s\".", resource_name.c_str(),
                                 _src_file.c_str())).ok()) {

            // Parse the dest file
            ret = parseS3Path(_dest_file, dest_bucket, dest_key, _src_ctx.prop_map());
            if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to parse the destination file name: \"%s\".", resource_name.c_str(),
                                     _dest_file.c_str())).ok()) {

                callback_data_t data;
                data.prop_map_ptr = &_src_ctx.prop_map();
                S3BucketContext bucketContext;
                std::int64_t lastModified;
                char eTag[256];

                std::memset(&bucketContext, 0, sizeof(bucketContext));
                bucketContext.bucketName = src_bucket.c_str();
                bucketContext.protocol = _proto;
                bucketContext.stsDate = _stsDate;
                bucketContext.uriStyle = _s3_uri_style;
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();

                std::string authRegionStr = get_region_name(_src_ctx.prop_map());
                bucketContext.authRegion = authRegionStr.c_str();

                S3ResponseHandler responseHandler = {
                    &responsePropertiesCallback,
                    &responseCompleteCallback
                };

                // initialize put properties
                S3PutProperties putProps;
                memset(&putProps, 0, sizeof(S3PutProperties));
                putProps.expires = -1;

                std::size_t retry_cnt = 0;
                do {
                    std::memset(&data, 0, sizeof(data));
                    data.prop_map_ptr = &_src_ctx.prop_map();
                    std::string&& hostname = s3GetHostname(_src_ctx.prop_map());
                    bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                    data.pCtx = &bucketContext;
                    S3_copy_object(&bucketContext, src_key.c_str(), dest_bucket.c_str(), dest_key.c_str(), &putProps, &lastModified, sizeof(eTag), eTag, 0,
                                   0, &responseHandler, &data);
                    if (data.status != S3StatusOK) {
                        s3_sleep( retry_wait );
                        retry_wait *= 2;
                        if (retry_wait > max_retry_wait) {
                            retry_wait = max_retry_wait;
                        }
                    }
                } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt <= retry_count_limit) );
                if (data.status != S3StatusOK) {
                    auto msg = fmt::format("[resource_name={}] {} - Error copying the S3 object: \"{}\" to S3 object \"{}\"",
                            resource_name,
                            __FUNCTION__,
                            _src_file,
                            _dest_file);

                    if(data.status >= 0) {
                        msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
                    }
                    result = ERROR(S3_FILE_COPY_ERR, msg);
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

    std::string resource_name = get_resource_name(_prop_map);

    ret = _prop_map.get<std::string>(s3_key_id, key_id);
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get the S3 access key id property.", resource_name.c_str())).ok()) {

        ret = _prop_map.get<std::string>(s3_access_key, access_key);
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get the S3 secret access key property.", resource_name.c_str())).ok()) {

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

    std::string resource_name = get_resource_name(_ctx.prop_map());

    // =-=-=-=-=-=-=-
    // verify that the resc context is valid
    ret = _ctx.valid();
    result = ASSERT_PASS(ret, "[resource_name=%s] Resource context is invalid", resource_name.c_str());

    return result;

} // Check Params

/// @brief Start up operation - Initialize the S3 library and set the auth fields in the properties.
irods:: error s3StartOperation(irods::plugin_property_map& _prop_map)
{
    irods::error result = SUCCESS();
    irods::error ret;

    std::string resource_name = get_resource_name(_prop_map);

    ret = s3Init( _prop_map );
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to initialize the S3 library.", resource_name.c_str())).ok()) {
        // Retrieve the auth info and set the appropriate fields in the property map
        ret = s3ReadAuthInfo(_prop_map);
        result = ASSERT_PASS(ret, "[resource_name=%s] Failed to read S3 auth info.", resource_name.c_str());
    }

    bool attached_mode = true, cacheless_mode = false;
    std::tie(cacheless_mode, attached_mode) = get_modes_from_properties(_prop_map);

    if (!attached_mode) {

        // update host to new host
        char resource_location[MAX_NAME_LEN];
        gethostname(resource_location, MAX_NAME_LEN);

        bool error = false;
        rodsLong_t resc_id = 0;
        irods::error ret = resc_mgr.hier_to_leaf_id(resource_name, resc_id);
        if( !ret.ok() ) { 
            error = true;
        } else {

            rodsServerHost_t *resource_host = nullptr;
            ret = irods::get_resource_property< rodsServerHost_t*& >(resc_id, irods::RESOURCE_HOST, resource_host);

            if (!ret.ok() || !resource_host) {
                 error = true;
            } else {
                 resource_host->hostName->name = strdup(resource_location);
                 resource_host->localFlag = LOCAL_HOST;
                 ret = irods::set_resource_property< rodsServerHost_t* >( resource_name, irods::RESOURCE_HOST, resource_host);
                 if (!ret.ok()) {
                     error = true;
                 }
            }
        }

        if (error) {
            // log the error but continue
            rodsLog(LOG_ERROR, "[resource_name=%s] Attached mode failed to set RESOURCE_HOST to %s.", resource_name.c_str(), resource_location);
        }
    }

    return result;
}

/// @brief stop operation. Deinitialize the s3 library
/// and remove system resources
irods::error s3StopOperation(irods::plugin_property_map& _prop_map)
{
    irods::error result = SUCCESS();
    if(S3Initialized) {
        S3Initialized = false;

        S3_deinitialize();
    }

    return result;

}

bool determine_unlink_for_repl_policy(
    rsComm_t*          _comm,
    const std::string& _logical_path,
    const std::string& _vault_path) {

    const auto& vps = irods::get_virtual_path_separator();
    std::string::size_type pos = _logical_path.find_last_of(vps);
    if(std::string::npos == pos) {
        THROW(
            SYS_INVALID_INPUT_PARAM,
            boost::str(boost::format("[%s] is not a logical path") %
            _logical_path));
    }

    std::string data_name{_logical_path.substr(pos+1, std::string::npos)};
    std::string coll_name{_logical_path.substr(0, pos)};
    std::string qstr =
        boost::str(boost::format(
        "SELECT DATA_PATH, DATA_RESC_ID WHERE DATA_NAME = '%s' AND COLL_NAME = '%s'") %
        data_name %
        coll_name);
    uint32_t s3_ctr{0};
    for(const auto& row : irods::query<rsComm_t>{_comm, qstr}) {
        const std::string& path = row[0];
        const std::string& id   = row[1];
        if(boost::starts_with(path, _vault_path)) {
            // if it matches check resc type
            // =-=-=-=-=-=-=-
            std::string type;
            irods::error ret = irods::get_resource_property<std::string>(
                                   std::stol(id.c_str()),
                                   irods::RESOURCE_TYPE,
                                   type);
            if(!ret.ok()) {
                irods::log(PASS(ret));
                continue;
            }

            if("s3" == type) {
                s3_ctr++;
            }
        } // if _vault_path

    } // for row

    if(s3_ctr > 0) {
        return false;
    }

    return true;
} // determine_unlink_for_repl_policy

// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectCreate(
    irods::plugin_property_map& _prop_map,
    irods::file_object&         _file_obj,
    const std::string&          _resc_name,
    const std::string&          _curr_host,
    float&                      _out_vote ) {
    irods::error result = SUCCESS();
    irods::error ret;
    int resc_status = 0;
    std::string host_name;


    std::string resource_name = get_resource_name(_prop_map);

    // =-=-=-=-=-=-=-
    // determine if the resource is down
    ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to retrieve status property.", resource_name.c_str())).ok() ) {

        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get location property.", resource_name.c_str())).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
            }

            // =-=-=-=-=-=-=-
            // vote higher if we are on the same host or if we are in detached mode
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
// given a property map and file object, if the object exists in the
// list of replicas then if the reg policy is set assume we have reach
// to the replica and register one for this given archive resource
irods::error register_archive_object(
    rsComm_t*                   _comm,
    irods::plugin_property_map& _prop_map,
    irods::file_object_ptr      _file_obj ) {
    // get the repl policy to determine if we need to check for an archived
    // replica and if so register it, only register if properly set
    std::string repl_policy;
    irods::error ret = _prop_map.get<std::string>(
                           REPL_POLICY_KEY,
                           repl_policy);
    if(!ret.ok()) {
        return SUCCESS();
    }

    if(REPL_POLICY_VAL != repl_policy) {
        return SUCCESS();
    }

    std::string resc_name;
    ret = _prop_map.get<std::string>(
              irods::RESOURCE_NAME,
              resc_name );
    if( !ret.ok() ) {
        return PASSMSG( fmt::format("[{}] {}", get_resource_name(_prop_map), ret.result()), ret );
    }

    // scan for a repl with this resource in the
    // hierarchy, if there is one then no need to continue
    bool repl_found = false;
    using phy_objs_t = std::vector<irods::physical_object>;
    phy_objs_t objs = _file_obj->replicas();
    phy_objs_t::iterator itr = objs.begin();
    for(const auto& obj : objs) {
        irods::hierarchy_parser hp;
        hp.set_string(obj.resc_hier());
        if(!hp.resc_in_hier(resc_name)) {
            continue;
        }

        repl_found = true;
    } // for itr

    if(repl_found) {
        return SUCCESS();
    }

    std::string vault_path;
    ret = _prop_map.get<std::string>(
              irods::RESOURCE_PATH,
              vault_path);

    // =-=-=-=-=-=-=-
    // search for a phypath with the same bucket name
    std::string phy_path;
    for(const auto& obj : objs) {
        if(boost::starts_with(obj.path(), vault_path)) {
            phy_path = itr->path();
            break;
        }
    }

    if(phy_path.empty()) {
        return ERROR(
                   INVALID_OBJECT_NAME,
                   boost::str(boost::format("[resource_name=%s] no matching phy path for [%s], [%s], [%s]") % resc_name.c_str() %
                       _file_obj->logical_path() %
                       vault_path %
                       resc_name));
    }

    // =-=-=-=-=-=-=-
    // get our parent resource
    rodsLong_t resc_id = 0;
    ret = _prop_map.get<rodsLong_t>( irods::RESOURCE_ID, resc_id );
    if( !ret.ok() ) {
        return PASSMSG( fmt::format("[{}] {}", get_resource_name(_prop_map), ret.result()), ret );
    }

    std::string resc_hier;
    ret = resc_mgr.leaf_id_to_hier(resc_id, resc_hier);
    if( !ret.ok() ) {
        return PASSMSG( fmt::format("[{}] {}", get_resource_name(_prop_map), ret.result()), ret );
    }

    // =-=-=-=-=-=-=-
    // get the root resc of the hier
    std::string root_resc;
    irods::hierarchy_parser parser;
    parser.set_string( resc_hier );
    parser.first_resc( root_resc );

    // =-=-=-=-=-=-=-
    // find the highest repl number for this data object
    int max_repl_num = 0;
    for(const auto& obj : objs) {
        if(obj.repl_num() > max_repl_num) {
            max_repl_num = obj.repl_num();
        }
    } // for objs

    // =-=-=-=-=-=-=-
    // grab the first physical object to reference
    // for the various properties in the obj info
    // physical object to mine for various properties
    const auto& obj = objs.front();

    // =-=-=-=-=-=-=-
    // build out a dataObjInfo_t struct for use in the call
    // to rsRegDataObj
    dataObjInfo_t dst_data_obj{};

    resc_mgr.hier_to_leaf_id(resc_hier.c_str(), dst_data_obj.rescId);
    strncpy( dst_data_obj.objPath,       obj.name().c_str(),       MAX_NAME_LEN );
    strncpy( dst_data_obj.rescName,      root_resc.c_str(),         NAME_LEN );
    strncpy( dst_data_obj.rescHier,      resc_hier.c_str(),         MAX_NAME_LEN );
    strncpy( dst_data_obj.dataType,      obj.type_name( ).c_str(), NAME_LEN );
    dst_data_obj.dataSize = obj.size( );
    strncpy( dst_data_obj.chksum,        obj.checksum( ).c_str(),  NAME_LEN );
    strncpy( dst_data_obj.version,       obj.version( ).c_str(),   NAME_LEN );
    strncpy( dst_data_obj.filePath,      phy_path.c_str(),          MAX_NAME_LEN );
    strncpy( dst_data_obj.dataOwnerName, obj.owner_name( ).c_str(),NAME_LEN );
    strncpy( dst_data_obj.dataOwnerZone, obj.owner_zone( ).c_str(),NAME_LEN );
    dst_data_obj.replNum    = max_repl_num+1;
    dst_data_obj.replStatus = obj.replica_status( );
    strncpy( dst_data_obj.statusString,  obj.status( ).c_str(),    NAME_LEN );
    dst_data_obj.dataId = obj.id();
    dst_data_obj.collId = obj.coll_id();
    dst_data_obj.dataMapId = 0;
    dst_data_obj.flags     = 0;
    strncpy( dst_data_obj.dataComments,  obj.r_comment( ).c_str(), MAX_NAME_LEN );
    strncpy( dst_data_obj.dataMode,      obj.mode( ).c_str(),      SHORT_STR_LEN );
    strncpy( dst_data_obj.dataExpiry,    obj.expiry_ts( ).c_str(), TIME_LEN );
    strncpy( dst_data_obj.dataCreate,    obj.create_ts( ).c_str(), TIME_LEN );
    strncpy( dst_data_obj.dataModify,    obj.modify_ts( ).c_str(), TIME_LEN );

    // =-=-=-=-=-=-=-
    // manufacture a src data obj
    dataObjInfo_t src_data_obj;
    memcpy( &src_data_obj, &dst_data_obj, sizeof( dst_data_obj ) );
    src_data_obj.replNum = obj.repl_num();
    strncpy( src_data_obj.filePath, obj.path().c_str(),       MAX_NAME_LEN );
    strncpy( src_data_obj.rescHier, obj.resc_hier().c_str(),  MAX_NAME_LEN );

    // =-=-=-=-=-=-=-
    // repl to an existing copy
    regReplica_t reg_inp{};
    reg_inp.srcDataObjInfo  = &src_data_obj;
    reg_inp.destDataObjInfo = &dst_data_obj;
    int reg_status = rsRegReplica( _comm, &reg_inp );
    if( reg_status < 0 ) {
        std::string error_str =  boost::str(boost::format("[resource_name=%s] failed register data object") % resc_name.c_str());
        return ERROR( reg_status, error_str.c_str() );
    }

    // =-=-=-=-=-=-=-
    // we need to make a physical object and add it to the file_object
    // so it can get picked up for the repl operation
    irods::physical_object phy_obj = obj;
    phy_obj.resc_hier( dst_data_obj.rescHier );
    phy_obj.repl_num( dst_data_obj.replNum );
    objs.push_back( phy_obj );
    _file_obj->replicas( objs );

    // =-=-=-=-=-=-=-
    // repave resc hier in file object as it is
    // what is used to determine hierarchy in
    // the compound resource
    _file_obj->resc_hier( dst_data_obj.rescHier );
    _file_obj->physical_path( dst_data_obj.filePath );

    return SUCCESS();

} // register_archive_object

// =-=-=-=-=-=-=-
// redirect_get - code to determine redirection for get operation
irods::error s3RedirectOpen(
    rsComm_t*                   _comm,
    irods::plugin_property_map& _prop_map,
    irods::file_object_ptr      _file_obj,
    const std::string&          _resc_name,
    const std::string&          _curr_host,
    float&                      _out_vote ) {


    std::string resource_name = get_resource_name(_prop_map);

    irods::error result = SUCCESS();
    irods::error ret;
    int resc_status = 0;
    std::string host_name;
    // =-=-=-=-=-=-=-
    // determine if the resource is down
    ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get status property for resource.", resource_name.c_str())).ok() ) {
        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get the location property.", resource_name.c_str())).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
            }
            else if( _curr_host == host_name) {
                // =-=-=-=-=-=-=-
                // vote higher if we are on the same host
                irods::error get_ret = register_archive_object(
                                           _comm,
                                           _prop_map,
                                           _file_obj);
                if(!get_ret.ok()) {
                    irods::log(get_ret);
                    return PASSMSG( fmt::format("[{}] {}", resource_name, get_ret.result()), get_ret );
                }

                _out_vote = 1.0;
            } else {
                _out_vote = 0.5;
            }
        }
    }

    return result;
} // s3RedirectOpen

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
            irods_s3::s3_file_create_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_OPEN,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_file_open_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_READ,
        std::function<irods::error(irods::plugin_context&,void*,const int)>(
            irods_s3::s3_file_read_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_WRITE,
        std::function<irods::error(irods::plugin_context&,const void*,const int)>(
            irods_s3::s3_file_write_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_CLOSE,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_file_close_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_UNLINK,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_file_unlink_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_STAT,
        std::function<irods::error(irods::plugin_context&, struct stat*)>(
            irods_s3::s3_file_stat_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_MKDIR,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_file_mkdir_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_OPENDIR,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_opendir_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_READDIR,
        std::function<irods::error(irods::plugin_context&,struct rodsDirent**)>(
            irods_s3::s3_readdir_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_RENAME,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            irods_s3::s3_file_rename_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_FREESPACE,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_get_fs_freespace_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_LSEEK,
        std::function<irods::error(irods::plugin_context&, const long long, const int)>(
            irods_s3::s3_file_lseek_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_RMDIR,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_rmdir_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_CLOSEDIR,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_closedir_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_STAGETOCACHE,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            irods_s3::s3_stage_to_cache_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_SYNCTOARCH,
        std::function<irods::error(irods::plugin_context&, const char*)>(
            irods_s3::s3_sync_to_arch_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_REGISTERED,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_registered_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_UNREGISTERED,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_unregistered_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_MODIFIED,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_modified_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_RESOLVE_RESC_HIER,
        std::function<irods::error(irods::plugin_context&,const std::string*, const std::string*, irods::hierarchy_parser*, float*)>(
            irods_s3::s3_resolve_resc_hier_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_REBALANCE,
        std::function<irods::error(irods::plugin_context&)>(
            irods_s3::s3_rebalance_operation ) );

    resc->add_operation(
        irods::RESOURCE_OP_NOTIFY,
        std::function<irods::error(irods::plugin_context&, const std::string*)>(
            irods_s3::s3_notify_operation ) );

    // set some properties necessary for backporting to iRODS legacy code
    resc->set_property< int >( irods::RESOURCE_CHECK_PATH_PERM, DO_CHK_PATH_PERM );
    resc->set_property< int >( irods::RESOURCE_CREATE_PATH,     CREATE_PATH );
    resc->set_property< int >( "category",        FILE_CAT );
    resc->set_property< bool >( irods::RESOURCE_SKIP_VAULT_PATH_CHECK_ON_UNLINK, true);

    return static_cast<irods::resource*>( resc );

} // plugin_factory
