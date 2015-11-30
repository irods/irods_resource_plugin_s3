/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#define linux_platform
#define _USE_FILE_OFFSET64

#include "libirods_s3.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include <msParam.hpp>
#include <reGlobalsExtern.hpp>
#include <rcConnect.hpp>
#include <rodsLog.hpp>
#include <rodsErrorTable.hpp>
#include <objInfo.hpp>

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
#include "irods_stacktrace.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <ctime>

#include <boost/lexical_cast.hpp>

// =-=-=-=-=-=-=-
// system includes
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
#if defined(linux_platform)
#include <sys/vfs.h>
#include <pthread.h>
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
const std::string s3_mpu_chunk = "S3_MPU_CHUNK";
const std::string s3_mpu_threads = "S3_MPU_THREADS";

const size_t RETRY_COUNT = 100;

static void s3_sleep(
        int _s,
        int _ms ) {
    useconds_t us = ( _s * 1000000 ) + ( _ms * 1000 );
    usleep( us );
}


extern "C" {


    //////////////////////////////////////////////////////////////////////
    // s3 specific functionality
    static bool S3Initialized = false; // so we only initialize the s3 library once
    static std::vector<char *> g_hostname;
    static int g_hostnameIdx = 0;
#if defined(linux_platform)
    static pthread_mutex_t g_hostnameIdxLock; // Init'd in s3Init
#endif


    // Increment through all specified hostnames in the list, locking in the case
    // where we may be multithreaded
    static const char *s3GetHostname()
    {
        if (g_hostname.empty())
            return NULL; // Short-circuit default case
#if defined(linux_platform)
        pthread_mutex_lock(&g_hostnameIdxLock);
#endif
        char *ret = g_hostname[g_hostnameIdx];
        g_hostnameIdx = (g_hostnameIdx + 1) % g_hostname.size();
#if defined(linux_platform)
        pthread_mutex_unlock(&g_hostnameIdxLock);
#endif
        return ret;
    }


    // Callbacks for S3
    static void StoreAndLogStatus (
        S3Status status,
        const S3ErrorDetails *error, 
        S3Status *pStatus )
    {
        int i;

        *pStatus = status;
        if( status != S3StatusOK ) {
                rodsLog( LOG_ERROR, "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
        }
        if (error && error->message) {
            rodsLog( LOG_ERROR, "  Message: %s\n", error->message);
        }
        if (error && error->resource) {
            rodsLog( LOG_ERROR, "  Resource: %s\n", error->resource);
        }
        if (error && error->furtherDetails) {
            rodsLog( LOG_ERROR, "  Further Details: %s\n", error->furtherDetails);
        }
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
        S3Status *pStatus = &(((callback_data_t*)callbackData)->status);
        StoreAndLogStatus( status, error, pStatus );
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
        irods::error result = ASSERT_ERROR(bufferSize != 0 && buffer != NULL && callbackData != NULL,
                                            SYS_INVALID_INPUT_PARAM, "Invalid input parameter.");
        if(!result.ok()) {
            irods::log(result);
        }

        int outfd = ((callback_data_t *)callbackData)->fd;

        ssize_t wrote = write(outfd, buffer, bufferSize);

        return ((wrote < (ssize_t) bufferSize) ?
                S3StatusAbortedByCallback : S3StatusOK);
    }

    static int putObjectDataCallback(
        int bufferSize,
        char *buffer,
        void *callbackData)
    {
        callback_data_t *data = (callback_data_t *) callbackData;
        long length;    
        long ret = 0;
        
        if (data->contentLength) {
            int length = ((data->contentLength > (unsigned) bufferSize) ?
                          (unsigned) bufferSize : data->contentLength);
            ret = pread(data->fd, buffer, length, data->offset);
        }
        data->contentLength -= ret;
        data->offset += ret;
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
        char *tmpPtr;

        if (!S3Initialized) {
            // First, parse the default hostname (if present) into a list of hostnames separated on the definition line by commas (,)
            std::string hostname_list;
            irods::error ret = _prop_map.get< std::string >( 
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
            }
#if defined(linux_platform)
            pthread_mutex_init(&g_hostnameIdxLock, NULL);
#endif

            size_t retry_count = 10;
            std::string retry_count_str;
            ret = _prop_map.get< std::string >( 
                                   s3_retry_count,
                                   retry_count_str );
            if( ret.ok() ) {
                try {
                    retry_count = boost::lexical_cast<int>( retry_count_str );
                } catch ( const boost::bad_lexical_cast& ) {
                    rodsLog(
                        LOG_ERROR,
                        "failed to cast retry count [%s] to an int",
                        retry_count_str.c_str() );
                }
            }

            size_t wait_time_sec = 3;
            std::string wait_time_str;
            ret = _prop_map.get< std::string >( 
                                   s3_wait_time_sec,
                                   wait_time_str );
            if( ret.ok() ) {
                try {
                    wait_time_sec = boost::lexical_cast<int>( wait_time_str );
                } catch ( const boost::bad_lexical_cast& ) {
                    rodsLog(
                        LOG_ERROR,
                        "failed to cast wait time [%s] to an int",
                        retry_count_str.c_str() );
                }
            }

            size_t ctr = 0;
            while( ctr < retry_count ) {
                int status = 0;

                const char* host_name = s3GetHostname();
                status = S3_initialize( "s3", S3_INIT_ALL, host_name );

                int err_status = S3_INIT_ERROR - status;
                std::stringstream msg;
                if( status >= 0 ) {
                    msg << " - \"";
                    msg << S3_get_status_name((S3Status)status);
                    msg << "\"";
                }

                result = ASSERT_ERROR(status == S3StatusOK, status, "Error initializing the S3 library. Status = %d.",
                                      status, msg.str().c_str());
                if( result.ok() ) {
                    break;
                }

                ctr++;

                s3_sleep( wait_time_sec, 0 );

                rodsLog(
                    LOG_NOTICE,
                    "s3Init - Error in connection, retry count %d",
                    retry_count );

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

    static long s3GetMPUChunksize (
        irods::plugin_property_map& _prop_map )
    {
        irods::error ret;
        std::string chunk_str;
        long chunk = 5L * 1024L * 1024L; // 5MB default
        ret = _prop_map.get< std::string >(
                                   s3_mpu_chunk,
                                   chunk_str );
        if (ret.ok()) {
            int parse = atol(chunk_str.c_str());
            if ( (parse >= 5) && (parse <= 5000) )
                chunk = parse * 1024L * 1024L;
        }
        return chunk;
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

                    callback_data_t data;
                    S3BucketContext bucketContext;

                    bzero (&data, sizeof (data));
                    data.fd = cache_fd;
                    data.contentLength = data.originalContentLength = _fileSize;
                    bzero (&bucketContext, sizeof (bucketContext));
                    bucketContext.hostName = s3GetHostname();
                    bucketContext.bucketName = bucket.c_str();
                    bucketContext.protocol = s3GetProto(_prop_map);
                    bucketContext.uriStyle = S3UriStylePath;
                    bucketContext.accessKeyId = _key_id.c_str();
                    bucketContext.secretAccessKey = _access_key.c_str();

                    S3GetObjectHandler getObjectHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &getObjectDataCallback
                    };

                    S3_get_object (&bucketContext, key.c_str(), NULL, 0, _fileSize, 0, &getObjectHandler, &data);
                    if (data.status != S3StatusOK) {
                        int status = data.status;
                        std::stringstream msg;
                        msg << __FUNCTION__;
                        msg << " - Error fetching the S3 object: \"";
                        msg << _s3ObjName;
                        msg << "\"";
                        if(status >= 0) {
                            msg << " - \"";
                            msg << S3_get_status_name((S3Status)status);
                            msg << "\"";
                            status = S3_INIT_ERROR - status;
                        }
                        result = ERROR(status, msg.str());
                    }
                    close(cache_fd);
                }
            }
        }
        return result;
    }


#if defined(linux_platform)
/* Multipart upload has a mutex-protected global work queue */
    static pthread_mutex_t mpuLock;
#endif
/* Other entries used by both multi and single threads */
    static volatile int mpuNext = 0;
    static int mpuLast = -1;
    static multipart_data_t *mpuData = NULL; 
    static char *mpuUploadId = NULL;
    static const char *mpuKey = NULL;
    static volatile int mpuAbort = FALSE;   // TBD: On part upload failure, abort and destroy upload

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
        S3Status *pStatus = &(((upload_manager_t*)callbackData)->status);
        StoreAndLogStatus( status, error, pStatus );
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
        S3Status *pStatus = &(((multipart_data_t *)callbackData)->status);
        StoreAndLogStatus( status, error, pStatus );
        if (status != S3StatusOK) {
            mpuAbort = TRUE;
        }
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
        S3Status *pStatus = &(((upload_manager_t*)callbackData)->status);
        StoreAndLogStatus( status, error, pStatus );
    }

    static S3Status mpuCancelRespPropCB (
        const S3ResponseProperties *properties,
        void *callbackData)
    {
        return S3StatusOK;
    }

    static void mpuCancelRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        S3Status *pStatus = (S3Status*)callbackData;
        StoreAndLogStatus( status, error, pStatus );
    }

    static void mpuCancel( S3BucketContext *bucketContext, const char *key, const char *upload_id )
    {
        S3AbortMultipartUploadHandler abortHandler = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };
        S3Status status;

        char buff[256];
        snprintf(buff, 255, "Aborting multipart upload:  key=%s, id=%s\n", key, upload_id);
        rodsLog(LOG_ERROR, buff);
        S3_abort_multipart_upload(bucketContext, key, upload_id, &abortHandler);
        if (status != S3StatusOK) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Error cancelling the multipart upload of S3 object: \"";
            msg << key;
            msg << "\"";
            if (status >= 0) {
                msg << " - \"";
                msg << S3_get_status_name(status);
                msg << "\"";
            }
            rodsLog(LOG_ERROR,msg.str().c_str() );
        }
    }


#if defined(linux_platform)
    /* Multipart worker thread, grabs a job from the queue and uploads it */
    static void *mpuWorkerThread (
        void *param )
    {
        S3BucketContext bucketContext = *((S3BucketContext*)param);
        irods::error result;
    	
        S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };
        rodsLog( LOG_NOTICE, "Starting thread");
        /* Will break out when no work detected */
        while (!mpuAbort) {
            int seq;
            pthread_mutex_lock(&mpuLock);
            if (mpuNext >= mpuLast) {
                pthread_mutex_unlock(&mpuLock);
                break;
            }
            seq = mpuNext + 1;
            mpuNext++;
            multipart_data_t *partData = &mpuData[seq-1];
            pthread_mutex_unlock(&mpuLock);

            int retry_cnt = 0;
            do {
                char buff[256];
                snprintf(buff, 255, "Multipart:  Start part %d, key %s, uploadid %s, offset %ld, len %d", (int)seq, mpuKey, mpuUploadId, (long)partData->put_object_data.offset, (int)partData->put_object_data.contentLength);
                rodsLog( LOG_NOTICE, buff );
                bucketContext.hostName = s3GetHostname(); // Safe to do, this is a local copy of the data structure
                S3_upload_part(&bucketContext, mpuKey, NULL, &putObjectHandler, seq, mpuUploadId, partData->put_object_data.contentLength, 0, partData);
                retry_cnt++;
                snprintf(buff, 255, "Multipart:  End part %d, key %s, uploadid %s, offset %ld", (int)seq, mpuKey, mpuUploadId, (long)partData->put_object_data.offset);
                rodsLog( LOG_NOTICE, buff);
            } while ((partData->status != S3StatusOK) && (retry_cnt < RETRY_COUNT) && !mpuAbort);
            if (partData->status != S3StatusOK) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error putting the S3 object: \"";
                msg << mpuKey;
                msg << "\"";
                msg << " part ";
                msg << seq;
                if(partData->status >= 0) {
                    msg << " - \"";
                    msg << S3_get_status_name(partData->status);
                    msg << "\"";
                }
                result = ERROR(partData->status, msg.str());
                rodsLog(LOG_ERROR,msg.str().c_str() );
                mpuAbort = TRUE;
            }
        }
        pthread_exit(0);
    }
#endif

    static void s3ErrorLog( const char *function, const char *object, const char *message, S3Status status )
    {
        std::stringstream msg;
        msg << function << message << ": \"" << object << "\"";
        if (status >= 0) {
            msg << " - \"" << S3_get_status_name(status) << "\"";
        }
        rodsLog( LOG_ERROR, msg.str().c_str() );
    }


    irods::error s3PutFile(
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
        int err_status = 0;
        long chunksize = s3GetMPUChunksize( _prop_map );
        size_t retry_cnt    = 0;
        
        ret = parseS3Path(_s3ObjName, bucket, key);
        if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                                 _s3ObjName.c_str())).ok()) {

            ret = s3Init( _prop_map );
            if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

                cache_fd = open(_filename.c_str(), O_RDONLY);
                err_status = UNIX_FILE_OPEN_ERR - errno;
                if((result = ASSERT_ERROR(cache_fd  != -1, err_status, "Failed to open the cache file: \"%s\".",
                                          _filename.c_str())).ok()) {

                    callback_data_t data;
                    S3BucketContext bucketContext;

                    bzero (&data, sizeof (data));
                    data.fd = cache_fd;
                    data.contentLength = data.originalContentLength = _fileSize;
                
                    bzero (&bucketContext, sizeof (bucketContext));
                    bucketContext.hostName = s3GetHostname();
                    bucketContext.bucketName = bucket.c_str();
                    bucketContext.protocol = s3GetProto(_prop_map);
                    bucketContext.uriStyle = S3UriStylePath;
                    bucketContext.accessKeyId = _key_id.c_str();
                    bucketContext.secretAccessKey = _access_key.c_str();

                    if ( data.contentLength < chunksize ) {
                        S3PutObjectHandler putObjectHandler = {
                            { &responsePropertiesCallback, &responseCompleteCallback },
                            &putObjectDataCallback
                        };

                        bool   put_done_flg = false;

                        while( !put_done_flg && ( retry_cnt < RETRY_COUNT ) ) {
                            S3_put_object (&bucketContext, key.c_str(), _fileSize, NULL, 0, &putObjectHandler, &data);
                            if (data.status != S3StatusOK) {
                                int status = data.status;
                                std::stringstream msg;
                                msg << __FUNCTION__;
                                msg << " - Error putting the S3 object: \"";
                                msg << _s3ObjName;
                                msg << "\"";
                                if(status >= 0) {
                                    msg << " - \"";
                                    msg << S3_get_status_name((S3Status)status);
                                    msg << "\"";
                                    status = S3_INIT_ERROR - status;
                                }
                                result = ERROR(status, msg.str());
                            }

                            if( S3StatusInternalError != data.status ) {
                                put_done_flg = true;
                            } else {
                                retry_cnt++;
                            }
                        
                        } // while

                    } else {
                    	// Multi-part upload time, baby!
                        upload_manager_t manager;
                        memset(&manager, 0, sizeof(manager));

                        manager.upload_id = NULL;
                        manager.remaining = 0;
                        manager.offset  = 0;
                        manager.xml = NULL;
                        
                        mpuAbort = FALSE;

                        char buff[256];
                        snprintf(buff, 255, "Multipart:  Begin key %s", key.c_str());
                        rodsLog( LOG_NOTICE, buff );
                        
                        long seq;
                        long totalSeq = (data.contentLength + chunksize- 1)/ chunksize;

                        multipart_data_t partData;
                        int partContentLength = 0;

                        manager.etags = (char**)calloc(sizeof(char*) * totalSeq, 1);

                        retry_cnt = 0;
                        do {
                            retry_cnt++;
                            // These expect a upload_manager_t* as cbdata
                            S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };
                            S3_initiate_multipart(&bucketContext, key.c_str(), NULL, &mpuInitialHandler, NULL, &manager);
                        } while (S3_status_is_retryable(manager.status) && ( retry_cnt < RETRY_COUNT ));
                        if (manager.upload_id == NULL || manager.status != S3StatusOK) {
                            std::stringstream msg;
                            msg << __FUNCTION__;
                            msg << " - Error initiating multipart upload of the S3 object: \"";
                            msg << _s3ObjName;
                            msg << "\"";
                            if(manager.status >= 0) {
                                msg << " - \"";
                                msg << S3_get_status_name(manager.status);
                                msg << "\"";
                            }
                            rodsLog( LOG_ERROR, msg.str().c_str() );
                            result = ERROR(manager.status, msg.str());
                            mpuAbort = TRUE;
                        }
                   
                        rodsLog( LOG_NOTICE, "Multipart: Ready to upload" );

                        mpuNext = 0;
                        mpuLast = totalSeq;
                        mpuData = (multipart_data_t*)calloc(totalSeq, sizeof(multipart_data_t));
                        if (!mpuData) {
                        }
                        mpuUploadId = manager.upload_id;
                        mpuKey = key.c_str();
                        for(seq = 1; seq <= totalSeq ; seq ++) {
                            memset(&partData, 0, sizeof(callback_data_t));
                            partData.manager = &manager;
                            partData.seq = seq;
                            partData.put_object_data = data;
                            partContentLength = (data.contentLength > chunksize)?chunksize:data.contentLength;
                            partData.put_object_data.contentLength = partContentLength;
                            partData.put_object_data.offset = (seq-1) * chunksize;
                            mpuData[seq-1] = partData;
                            data.contentLength -= partContentLength;
                        }
#if defined(linux_platform)
                        if (!mpuAbort) {
                            // Make the worker threads and start
                            pthread_mutex_init(&mpuLock, NULL);

                            int threads = s3GetMPUThreads(_prop_map);
                            pthread_t *thread = (pthread_t *)calloc(threads, sizeof(pthread_t));
                            if (thread==NULL) {
                            }
                            for (int thr_id=0; thr_id<threads; thr_id++) {
                                pthread_create(&thread[thr_id], NULL, mpuWorkerThread, &bucketContext);
                            }
                        
                            // And wait for them to finish...
                            for (int thr_id=0; thr_id<threads; thr_id++) {
                                  pthread_join(thread[thr_id], NULL);
                            }

                            free(thread);
                        }
#else                        
                        // No threads, so sequentially upload the parts one at a time
                        for(seq = 1; !mpuAbort && seq <= totalSeq ; seq ++) {
                            partData = mpuData[seq-1];
                            retry_cnt = 0;
                            do {
                                char buff[256];
                                snprintf(buff, 255, "Multipart:  Start part %d", (int)seq);
                                rodsLog( LOG_NOTICE, buff );
                                // Pass in multipart_data_t as CBData
                                bucketContext.hostName = s3GetHostname(); // Rotate through hosts for each part
                                S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };
                                S3_upload_part(&bucketContext, key.c_str(), NULL, &putObjectHandler, seq, mpuUploadId, partData.put_object_data.contentLength, 0, &partData);
                                retry_cnt++;
                                snprintf(buff, 255, "Multipart:  End part %d", (int)seq);
                                rodsLog( LOG_NOTICE, buff);
                            } while (S3_status_is_retryable(partData.status) && ( retry_cnt < RETRY_COUNT ));
                            if (partData.status != S3StatusOK) {
                                std::stringstream msg;
                                msg << __FUNCTION__;
                                msg << " - Error putting the S3 object: \"";
                                msg << _s3ObjName;
                                msg << "\"";
                                if(partData.status >= 0) {
                                    msg << " - \"";
                                    msg << S3_get_status_name((S3Status)partData.status);
                                    msg << "\"";
                                }
                                result = ERROR(partData.status, msg.str());
                                mpuAbort = TRUE;
                            }
                        }
#endif

                        manager.remaining = 0;
                        manager.offset  = 0;
                        // Maximum length with extra for the <complete...></complete...> tag
                        manager.xml = (char *)malloc((totalSeq+2) * 256);
                        if (manager.xml == NULL) {
                        }

                        if (!mpuAbort) {
                            char buff[256];
                            snprintf(buff, 255, "Multipart:  Completing key %s", key.c_str());
                            rodsLog( LOG_NOTICE, buff );

                            int i;
                            strcpy(manager.xml, "<CompleteMultipartUpload>\n");
                            manager.remaining = strlen(manager.xml);
                            char buf[256];
                            int n;
                            for(i=0;i<totalSeq;i++) {
                                n = snprintf(buf,256,"<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n",
                                                i + 1,manager.etags[i]);
                                strcpy(manager.xml+manager.remaining, buf);
                                manager.remaining += n;
                            }
                            strcat(manager.xml+manager.remaining, "</CompleteMultipartUpload>\n");
                            manager.remaining += strlen(manager.xml+manager.remaining); //size;
                            manager.offset = 0;
                            retry_cnt = 0;
                            do {
                                S3MultipartCommitHandler commit_handler = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, NULL };
                                S3_complete_multipart_upload(&bucketContext, key.c_str(), &commit_handler, manager.upload_id, manager.remaining, NULL, &manager); 
                                retry_cnt++;
                            } while (S3_status_is_retryable(manager.status) && ( retry_cnt < RETRY_COUNT ));
                            if (manager.status != S3StatusOK) {
                                std::stringstream msg;
                                msg << __FUNCTION__;
                                msg << " - Error putting the S3 object: \"";
                                msg << _s3ObjName;
                                msg << "\"";
                                if(manager.status >= 0) {
                                    msg << " - \"";
                                    msg << S3_get_status_name(manager.status);
                                    msg << "\"";
                                }
                                result = ERROR(manager.status, msg.str());
                                mpuAbort = TRUE;
                            }
                        }
                        if (mpuAbort && manager.upload_id) {
                            rodsLog(LOG_ERROR, "Cancelling multipart upload");
                            mpuCancel( &bucketContext, key.c_str(), manager.upload_id );
                        }
                        // Clean up memory
                        if (manager.xml) free(manager.xml);
                        if (manager.upload_id) free(manager.upload_id);
                        for (int i=0; manager.etags && i<totalSeq; i++) {
                            if (manager.etags[i]) free(manager.etags[i]);
                        }
                        if (manager.etags) free(manager.etags);
                        if (mpuData) free(mpuData);
                    }


                    close(cache_fd);
                }
            }
        }
        return result;
    }

    /// @brief Function to copy the specifed src file to the specified dest file
    irods::error s3CopyFile(
        const std::string& _src_file,
        const std::string& _dest_file,
        const std::string& _key_id,
        const std::string& _access_key,
        const S3Protocol _proto)
    {
        irods::error result = SUCCESS();
        irods::error ret;
        std::string src_bucket;
        std::string src_key;
        std::string dest_bucket;
        std::string dest_key;

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

                bzero (&data, sizeof (data));
                bzero (&bucketContext, sizeof (bucketContext));
                bucketContext.hostName = s3GetHostname();
                bucketContext.bucketName = src_bucket.c_str();
                bucketContext.protocol = _proto;
                bucketContext.uriStyle = S3UriStylePath;
                bucketContext.accessKeyId = _key_id.c_str();
                bucketContext.secretAccessKey = _access_key.c_str();
   

                S3ResponseHandler responseHandler = {
                    &responsePropertiesCallback,
                    &responseCompleteCallback
                };

                S3_copy_object(&bucketContext, src_key.c_str(), dest_bucket.c_str(), dest_key.c_str(), NULL, &lastModified, sizeof(eTag), eTag, 0,
                               &responseHandler, &data);
                if (data.status != S3StatusOK) {
                    int status = data.status;
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Error copying the S3 object: \"";
                    msg << _src_file;
                    msg << "\" to S3 object: \"";
                    msg << _dest_file;
                    msg << "\"";
                    if(status >= 0) {
                        msg << " - \"";
                        msg << S3_get_status_name((S3Status)status);
                        msg << "\"";
                        status = S3_INIT_ERROR - status;
                    }
                    result = ERROR(status, msg.str());
                }
                else if( data.status != S3StatusOK ) {
                    std::stringstream msg;
                    msg << "Error copying the S3 Object \""
                        << _src_file << "\" to \""
                        << _dest_file 
                        << "\" with S3Status \""
                        << S3_get_status_name( data.status )
                        << "\"";
                    result = ERROR( S3_INIT_ERROR - data.status, msg.str() );
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
    irods::error s3CheckParams(irods::resource_plugin_context& _ctx ) {

        irods::error result = SUCCESS();
        irods::error ret;

        // =-=-=-=-=-=-=-
        // verify that the resc context is valid
        ret = _ctx.valid();
        result = ASSERT_PASS(ret, "Resource context is invalid");

        return result;

    } // Check Params

    /// @brief Start up operation - Initialize the S3 library and set the auth fields in the properties.
    irods:: error s3StartOperation(
        irods::plugin_property_map& _prop_map,
        irods::resource_child_map& _child_map)
    {
        irods::error result = SUCCESS();
        irods::error ret;

        // Initialize the S3 library
        ret = s3Init( _prop_map );
        if((result = ASSERT_PASS(ret, "Failed to initialize the S3 library.")).ok()) {

            // Retrieve the auth info and set the appropriate fields in the property map
            ret = s3ReadAuthInfo(_prop_map);
            result = ASSERT_PASS(ret, "Failed to read S3 auth info.");
        }

        return result;
    }

    /// @brief stop operation. All this does is deinitialize the s3 library
    irods::error s3StopOperation(
        irods::plugin_property_map& _prop_map,
        irods::resource_child_map& _child_map)
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
    irods::error s3RegisteredPlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3UnregisteredPlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3ModifiedPlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create
    irods::error s3FileCreatePlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3FileOpenPlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3FileReadPlugin( irods::resource_plugin_context& _ctx,
                                    void*               _buf,
                                    int                 _len ) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3FileWritePlugin( irods::resource_plugin_context& _ctx,
                                     void*               _buf,
                                     int                 _len ) {
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3FileClosePlugin(  irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3FileUnlinkPlugin(
        irods::resource_plugin_context& _ctx )
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

                        bzero (&data, sizeof (data));

                        bzero (&bucketContext, sizeof (bucketContext));
                        bucketContext.hostName = s3GetHostname();
                        bucketContext.bucketName = bucket.c_str();
                        bucketContext.protocol = s3GetProto(_ctx.prop_map());
                        bucketContext.uriStyle = S3UriStylePath;
                        bucketContext.accessKeyId = key_id.c_str();
                        bucketContext.secretAccessKey = access_key.c_str();

                        S3ResponseHandler responseHandler = {
                            0, &responseCompleteCallback
                        };

                        S3_delete_object(&bucketContext, key.c_str(), 0, &responseHandler, &data);

                        if (data.status != S3StatusOK) {
                            int status = data.status;
                            std::stringstream msg;
                            msg << __FUNCTION__;
                            msg << " - Error unlinking the S3 object: \"";
                            msg << _object->physical_path();
                            msg << "\"";
                            if(status >= 0) {
                                msg << " - \"";
                                msg << S3_get_status_name((S3Status)status);
                                msg << "\"";
                                status = S3_INIT_ERROR - status;
                            }
                            result = ERROR(status, msg.str());
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
        irods::resource_plugin_context& _ctx,
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

                            bzero (&data, sizeof (data));

                            bzero (&bucketContext, sizeof (bucketContext));
                            bucketContext.bucketName = bucket.c_str();
                            bucketContext.protocol = s3GetProto(_ctx.prop_map());
                            bucketContext.uriStyle = S3UriStylePath;
                            bucketContext.accessKeyId = key_id.c_str();
                            bucketContext.secretAccessKey = access_key.c_str();

                            S3ListBucketHandler listBucketHandler = {
                                { &responsePropertiesCallback, &responseCompleteCallback },
                                &listBucketCallback
                            };

                            data.keyCount = 0;

                            S3_list_bucket(&bucketContext, key.c_str(), NULL,
                                           NULL, 1, 0, &listBucketHandler, &data);

                            if (data.status != S3StatusOK) {
                                int status = data.status;
                                std::stringstream msg;
                                msg << __FUNCTION__;
                                msg << " - Error stat'ing the S3 object: \"";
                                msg << _object->physical_path();
                                msg << "\"";
                                if(status >= 0) {
                                    msg << " - \"";
                                    msg << S3_get_status_name((S3Status)status);
                                    msg << "\"";
                                    status = S3_FILE_STAT_ERR - status;
                                }
                                result = ERROR(status, msg.str());
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
        return result;
    }
        
    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::resource_plugin_context& _ctx,
                                      struct stat*        _statbuf ) {
        return ERROR( SYS_NOT_SUPPORTED, "s3FileFstatPlugin" );
                                   
    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3FileLseekPlugin(  irods::resource_plugin_context& _ctx, 
                                      size_t              _offset, 
                                      int                 _whence ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileLseekPlugin" );
                                       
    } // wosFileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX fsync
    irods::error s3FileFsyncPlugin(  irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileFsyncPlugin" );

    } // s3FileFsyncPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileMkdirPlugin(  irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileMkdirPlugin" );

    } // s3FileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileChmodPlugin(  irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileChmodPlugin" );
    } // s3FileChmodPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileRmdirPlugin(  irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileRmdirPlugin" );
    } // s3FileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3FileOpendirPlugin( irods::resource_plugin_context& _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileOpendirPlugin" );
    } // s3FileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3FileClosedirPlugin( irods::resource_plugin_context& _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileClosedirPlugin" );
    } // s3FileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3FileReaddirPlugin( irods::resource_plugin_context& _ctx,
                                       struct rodsDirent**     _dirent_ptr ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileReaddirPlugin" );
    } // s3FileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3FileRenamePlugin( irods::resource_plugin_context& _ctx,
                                      const char*         _new_file_name )
    {
        irods::error result = SUCCESS();
        irods::error ret;
        std::string key_id;
        std::string access_key;

        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

            irods::data_object_ptr object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());
        
            // copy the file to the new location
            ret = s3CopyFile(object->physical_path(), _new_file_name, key_id, access_key, s3GetProto(_ctx.prop_map()));
            if((result = ASSERT_PASS(ret, "Failed to copy file from: \"%s\" to \"%s\".",
                                     object->physical_path().c_str(), _new_file_name)).ok()) {
        
                // delete the old file
                ret = s3FileUnlinkPlugin(_ctx);
                result = ASSERT_PASS(ret, "FAiled to unlink old S3 file: \"%s\".",
                                     object->physical_path().c_str());
            }
        }
        
        return result;
    } // s3FileRenamePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin( 
        irods::resource_plugin_context& _ctx )
    { 
        return ERROR( SYS_NOT_SUPPORTED, "s3FileTruncatePlugin" );
    } // s3FileTruncatePlugin

    
    // interface to determine free space on a device given a path
    irods::error s3FileGetFsFreeSpacePlugin(
        irods::resource_plugin_context& _ctx )
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
        irods::resource_plugin_context& _ctx,
        char*                               _cache_file_name )
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

                    if((result = ASSERT_ERROR(object->size() <= 0 || object->size() == statbuf.st_size, SYS_COPY_LEN_ERR,
                                              "Error for file: \"%s\" inp data size: %ld does not match stat size: %ld.",
                                              object->physical_path().c_str(), object->size(), statbuf.st_size)).ok()) {

                        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                        if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                            rodsLong_t mySize = statbuf.st_size;
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
        irods::resource_plugin_context& _ctx,
        char*                               _cache_file_name )
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

                        rodsLong_t data_size = statbuf.st_size;
                        ret = s3PutFile(_cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
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
        irods::resource_plugin_context& _ctx,
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
        irods::resource_plugin_context& _ctx ) {
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
            rodsLog(
                LOG_DEBUG,
                "context: %s",
                _context.c_str());
            irods::string_tokenize( _context, ";", props );

            // =-=-=-=-=-=-=-
            // parse key/property pairs using = as a separator and
            // add them to the property list
            std::vector< std::string >::iterator itr = props.begin();
            for( ; itr != props.end(); ++itr ) {
                // =-=-=-=-=-=-=-
                // break up key and value into two strings
                std::vector< std::string > vals;
                irods::string_tokenize( *itr, "=", vals );

                // =-=-=-=-=-=-=-
                // break up key and value into two strings
                rodsLog(
                    LOG_DEBUG,
                    "vals: %s %s",
                    vals[0].c_str(),
                    vals[1].c_str());

                properties_[ vals[0] ] = vals[1];

            } // for itr 

            // Add start and stop operations
            set_start_operation( "s3StartOperation" );
            set_stop_operation( "s3StopOperation" );
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


    // =-=-=-=-=-=-=-
    // Create the plugin factory function which will return a microservice
    // table entry containing the microservice function pointer, the number
    // of parameters that the microservice takes and the name of the micro
    // service.  this will be called by the plugin loader in the irods server
    // to create the entry to the table when the plugin is requested.
    irods::resource* plugin_factory(const std::string& _inst_name, const std::string& _context) {
        s3_resource* resc = new s3_resource(_inst_name, _context);

        resc->add_operation( irods::RESOURCE_OP_CREATE,       "s3FileCreatePlugin" );
        resc->add_operation( irods::RESOURCE_OP_OPEN,         "s3FileOpenPlugin" );
        resc->add_operation( irods::RESOURCE_OP_READ,         "s3FileReadPlugin" );
        resc->add_operation( irods::RESOURCE_OP_WRITE,        "s3FileWritePlugin" );
        resc->add_operation( irods::RESOURCE_OP_CLOSE,        "s3FileClosePlugin" );
        resc->add_operation( irods::RESOURCE_OP_UNLINK,       "s3FileUnlinkPlugin" );
        resc->add_operation( irods::RESOURCE_OP_STAT,         "s3FileStatPlugin" );
        resc->add_operation( irods::RESOURCE_OP_FSTAT,        "s3FileFstatPlugin" );
        resc->add_operation( irods::RESOURCE_OP_FSYNC,        "s3FileFsyncPlugin" );
        resc->add_operation( irods::RESOURCE_OP_MKDIR,        "s3FileMkdirPlugin" );
        resc->add_operation( irods::RESOURCE_OP_CHMOD,        "s3FileChmodPlugin" );
        resc->add_operation( irods::RESOURCE_OP_OPENDIR,      "s3FileOpendirPlugin" );
        resc->add_operation( irods::RESOURCE_OP_READDIR,      "s3FileReaddirPlugin" );
        resc->add_operation( irods::RESOURCE_OP_RENAME,       "s3FileRenamePlugin" );
        resc->add_operation( irods::RESOURCE_OP_FREESPACE,    "s3FileGetFsFreeSpacePlugin" );
        resc->add_operation( irods::RESOURCE_OP_LSEEK,        "s3FileLseekPlugin" );
        resc->add_operation( irods::RESOURCE_OP_RMDIR,        "s3FileRmdirPlugin" );
        resc->add_operation( irods::RESOURCE_OP_CLOSEDIR,     "s3FileClosedirPlugin" );
        resc->add_operation( irods::RESOURCE_OP_TRUNCATE,     "s3FileTruncatePlugin" );
        resc->add_operation( irods::RESOURCE_OP_STAGETOCACHE, "s3StageToCachePlugin" );
        resc->add_operation( irods::RESOURCE_OP_SYNCTOARCH,   "s3SyncToArchPlugin" );
        resc->add_operation( irods::RESOURCE_OP_REGISTERED,   "s3RegisteredPlugin" );
        resc->add_operation( irods::RESOURCE_OP_UNREGISTERED, "s3UnregisteredPlugin" );
        resc->add_operation( irods::RESOURCE_OP_MODIFIED,     "s3ModifiedPlugin" );
        resc->add_operation( irods::RESOURCE_OP_RESOLVE_RESC_HIER, "s3RedirectPlugin" );
        resc->add_operation( irods::RESOURCE_OP_REBALANCE,         "s3FileRebalance" );

        // set some properties necessary for backporting to iRODS legacy code
        resc->set_property< int >( "check_path_perm", DO_CHK_PATH_PERM );
        resc->set_property< int >( "create_path",     NO_CREATE_PATH );
        resc->set_property< int >( "category",        FILE_CAT );

        //return dynamic_cast<irods::resource*>( resc );
        return dynamic_cast<irods::resource *> (resc);
    } // plugin_factory


}; // extern "C" 



