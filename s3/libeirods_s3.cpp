/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "libeirods_s3.h"

// =-=-=-=-=-=-=-
// irods includes
#include <msParam.h>
#include <reGlobalsExtern.h>
#include <rcConnect.h>
#include <rodsLog.h>
#include <rodsErrorTable.h>
#include <objInfo.h>

#ifdef USING_JSON
#include <json/json.h>
#endif

// =-=-=-=-=-=-=-
// eirods includes
#include "eirods_resource_plugin.h"
#include "eirods_file_object.h"
#include "eirods_physical_object.h"
#include "eirods_collection_object.h"
#include "eirods_string_tokenize.h"
#include "eirods_hierarchy_parser.h"
#include "eirods_resource_redirect.h"
#include "eirods_stacktrace.h"

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>

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
#endif
#include <sys/stat.h>

#include <string.h>

const std::string s3_auth_file = "S3_AUTH_FILE";
const std::string s3_key_id = "S3_ACCESS_KEY_ID";
const std::string s3_access_key = "S3_SECRET_ACCESS_KEY";

extern "C" {


    // =-=-=-=-=-=-=-
    // Define plugin Version Variable
    double EIRODS_PLUGIN_INTERFACE_VERSION=1.0;

    //////////////////////////////////////////////////////////////////////
    // s3 specific functionality
    static bool S3Initialized = false; // so we only initialize the s3 library once
//    static s3Auth_t S3Auth;            // authorization credentials for s3 operations?
    static int statusG = 0;

    // Callbacks for S3
    static void responseCompleteCallback(
        S3Status status,
        const S3ErrorDetails *error, 
        void *callbackData)
    {
        int i;
        
        statusG = status;
        if (error && error->message) {
            printf("  Message: %s\n", error->message);
        }
        if (error && error->resource) {
            printf("  Resource: %s\n", error->resource);
        }
        if (error && error->furtherDetails) {
            printf("  Further Details: %s\n", error->furtherDetails);
        }
        if (error && error->extraDetailsCount) {
            printf("%s", "  Extra Details:\n");
            
            for (i = 0; i < error->extraDetailsCount; i++) {
                printf("    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
            }
        }
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
        FILE *outfile = (FILE *) callbackData;

        size_t wrote = fwrite(buffer, 1, bufferSize, outfile);

        return ((wrote < (size_t) bufferSize) ?
                S3StatusAbortedByCallback : S3StatusOK);
    }

    static int putObjectDataCallback(
        int bufferSize,
        char *buffer,
        void *callbackData)
    {
        put_object_callback_data *data = (put_object_callback_data *) callbackData;
        int length;    
        int ret = 0;
        
        if (data->contentLength) {
            int length = ((data->contentLength > (unsigned) bufferSize) ?
                          (unsigned) bufferSize : data->contentLength);
            ret = fread(buffer, 1, length, data->infile);
        }
        data->contentLength -= ret;
        return ret;
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
    eirods::error parseS3Path (
        const std::string& _s3ObjName,
        std::string& _bucket,
        std::string& _key)
    {
        eirods::error result = SUCCESS();
        size_t start_pos = 0;
        size_t slash_pos = 0;
        slash_pos = _s3ObjName.find_first_of("/");
        // skip a leading slash
        if(slash_pos == 0) {
            start_pos = 1;
            slash_pos = _s3ObjName.find_first_of("/", 1);
        }
        // have to have at least one slash to separate bucket from key
        if(slash_pos == std::string::npos) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - problem parsing \"";
            msg << _s3ObjName;
            msg << "\"";
            result = ERROR(SYS_INVALID_FILE_PATH, msg.str());
        } else {
            _bucket = _s3ObjName.substr(start_pos, slash_pos - start_pos);
            _key = _s3ObjName.substr(slash_pos + 1);
        }
        return result;
    }

    eirods::error readS3AuthInfo (
        const std::string& _filename,
        std::string& _rtn_key_id,
        std::string& _rtn_access_key)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        FILE *fptr;
        char inbuf[MAX_NAME_LEN];
        int lineLen, bytesCopied;
        int linecnt = 0;
        char access_key_id[S3_MAX_KEY_SIZE];
        char secret_access_key[S3_MAX_KEY_SIZE];
        
        fptr = fopen (_filename.c_str(), "r");

        if (fptr == NULL) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to open S3 auth file: \"";
            msg << _filename;
            msg << "\" - ";
            msg << strerror(errno);
            result = ERROR(SYS_CONFIG_FILE_ERR, msg.str());
        } else {
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
            if (linecnt != 2)  {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error: read ";
                msg << linecnt;
                msg << " lines in the auth file. Expected 2.";
                result = ERROR(SYS_CONFIG_FILE_ERR, msg.str());
            }
            else {
                _rtn_key_id = access_key_id;
                _rtn_access_key = secret_access_key;
            }
            return result;
        }
    }

    /// @brief Retrieves the auth info from either the environment or the resource's specified auth file and set the appropriate
    /// fields in the property map
    eirods::error s3ReadAuthInfo(
        eirods::resource_property_map& _prop_map)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
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
            if(!(ret = _prop_map.get<std::string>(s3_auth_file, auth_file)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to retrieve S3 auth filename property.";
                result = PASSMSG(msg.str(), ret);
            }
            
            else if (!(ret = readS3AuthInfo(auth_file, key_id, access_key)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed reading the authorization credentials file.";
                result = PASSMSG(msg.str(), ret);
            }
        }
        if(result.ok()) {
            if(!(ret = _prop_map.set<std::string>(s3_key_id, key_id)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to set the " << s3_key_id << " property.";
                result = PASSMSG(msg.str(), ret);
            }

            else if(!(ret = _prop_map.set<std::string>(s3_access_key, access_key)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to set the " << s3_access_key << " property.";
                result = PASSMSG(msg.str(), ret);
            }
        }
        return result;
    }
    
    eirods::error s3Init (void)
    {
        eirods::error result = SUCCESS();
        char *tmpPtr;
        
        if (!S3Initialized) {
            
            S3Initialized = true;
            int status = 0;
#ifdef libs3_3_1_4
            status = S3_initialize ("s3", S3_INIT_ALL);
#else
            status = S3_initialize ("s3", S3_INIT_ALL, NULL);
#endif
            if(status != S3StatusOK) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error initializing the S3 library. status = ";
                msg << status << " ";
                if(status >= 0) {
                    msg << " - \"";
                    msg << S3_get_status_name((S3Status)status);
                    msg << "\"";
                    status = S3_INIT_ERROR - status;
                }
                result = ERROR(status, msg.str());
            }
        }

        return result;
    }
        
    eirods::error s3GetFile(
        const std::string& _filename,
        const std::string& _s3ObjName,
        rodsLong_t _fileSize,
        const std::string& _key_id,
        const std::string& _access_key)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        FILE* cache_file = NULL;
        std::string bucket;
        std::string key;
        if(!(ret = parseS3Path(_s3ObjName, bucket, key)).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed parsing the S3 bucket and key from the physical path: \"";
            msg << _s3ObjName;
            msg << "\"";
            result = PASSMSG(msg.str(), ret);
        }
        else if(!(ret = s3Init()).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to initialize the S3 system.";
            result = PASSMSG(msg.str(), ret);
        }
        else if((cache_file = fopen(_filename.c_str(), "w+")) == NULL) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to open the cache file: \"";
            msg << _filename;
            msg << "\"";
            result = ERROR(UNIX_FILE_OPEN_ERR - errno, msg.str());
        }
        else {
            callback_data_t data;
            S3BucketContext bucketContext;

            bzero (&data, sizeof (data));
            data.fd = cache_file;
            data.contentLength = data.originalContentLength = _fileSize;
            bzero (&bucketContext, sizeof (bucketContext));
            bucketContext.bucketName = bucket.c_str();
            bucketContext.protocol = S3ProtocolHTTPS;
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
        }
        return result;
    }

    eirods::error s3PutFile(
        const std::string& _filename,
        const std::string& _s3ObjName,
        rodsLong_t _fileSize,
        const std::string& _key_id,
        const std::string& _access_key)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        FILE* cache_file = NULL;
        std::string bucket;
        std::string key;
        if(!(ret = parseS3Path(_s3ObjName, bucket, key)).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed parsing the S3 bucket and key from the physical path: \"";
            msg << _s3ObjName;
            msg << "\"";
            result = PASSMSG(msg.str(), ret);
        }
        else if(!(ret = s3Init()).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to initialize the S3 system.";
            result = PASSMSG(msg.str(), ret);
        }
        else if((cache_file = fopen(_filename.c_str(), "r")) == NULL) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to open the cache file: \"";
            msg << _filename;
            msg << "\"";
            result = ERROR(UNIX_FILE_OPEN_ERR - errno, msg.str());
        }
        else {
            callback_data_t data;
            S3BucketContext bucketContext;

            bzero (&data, sizeof (data));
            data.fd = cache_file;
            data.contentLength = data.originalContentLength = _fileSize;
                
            bzero (&bucketContext, sizeof (bucketContext));
            bucketContext.bucketName = bucket.c_str();
            bucketContext.protocol = S3ProtocolHTTPS;
            bucketContext.uriStyle = S3UriStylePath;
            bucketContext.accessKeyId = _key_id.c_str();
            bucketContext.secretAccessKey = _access_key.c_str();

            S3PutObjectHandler putObjectHandler = {
                { &responsePropertiesCallback, &responseCompleteCallback },
                &putObjectDataCallback
            };

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
        }
        return result;
    }
    
    eirods::error s3GetAuthCredentials(
        eirods::resource_property_map& _prop_map,
        std::string& _rtn_key_id,
        std::string& _rtn_access_key)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        std::string key_id;
        std::string access_key;
        if(!(ret = _prop_map.get<std::string>(s3_key_id, key_id)).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to get the S3 access key id property.";
            result = PASSMSG(msg.str(), ret);
        }

        else if(!(ret = _prop_map.get<std::string>(s3_access_key, access_key)).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to get the S3 secret access key property.";
            result = PASSMSG(msg.str(), ret);
        }

        else {
            _rtn_key_id = key_id;
            _rtn_access_key = access_key;
        }
        
        return result;
    }
    //
    //////////////////////////////////////////////////////////////////////
        
    // =-=-=-=-=-=-=-
    /// @brief Checks the basic operation parameters and updates the physical path in the file object
    eirods::error s3CheckParams(eirods::resource_operation_context* _ctx ) {

        eirods::error result = SUCCESS();
        eirods::error ret;

        // =-=-=-=-=-=-=-
        // check incoming parameters
        if( !_ctx ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - null resource context";
            result = ERROR( SYS_INVALID_INPUT_PARAM, msg.str() );
        }

        // =-=-=-=-=-=-=-
        // verify that the resc context is valid 
        ret = _ctx->valid();
        if( !ret.ok() ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - resource context is invalid";
            result = PASSMSG( msg.str(), ret );
        } 

        return result;

    } // Check Params
    
    /// @brief Start up operation - Initialize the S3 library and set the auth fields in the properties.
    eirods:: error s3StartOperation(
        eirods::resource_property_map& _prop_map,
        eirods::resource_child_map& _child_map)
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        
        // Initialize the S3 library
        if(!(ret = s3Init()).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to initialize the S3 library.";
            result = PASSMSG(msg.str(), ret);
        }

        else {
            // Retrieve the auth info and set the appropriate fields in the property map
            if(!(ret = s3ReadAuthInfo(_prop_map)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to read S3 auth info.";
                result = PASSMSG(msg.str(), ret);
            }
        }
        
        return result;
    }

    /// @brief stop operation. All this does is deinitialize the s3 library
    eirods::error s3StopOperation(
        eirods::resource_property_map& _prop_map,
        eirods::resource_child_map& _child_map)
    {
        eirods::error result = SUCCESS();
        if(S3Initialized) {
            S3Initialized = false;

            S3_deinitialize();
        }
        return result;
    }
    
    // =-=-=-=-=-=-=-
    // interface for file registration
    eirods::error s3RegisteredPlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    eirods::error s3UnregisteredPlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    } 

    // =-=-=-=-=-=-=-
    // interface for file modification
    eirods::error s3ModifiedPlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    } 
    
    // =-=-=-=-=-=-=-
    // interface for POSIX create
    eirods::error s3FileCreatePlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    } 

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    eirods::error s3FileOpenPlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    eirods::error s3FileReadPlugin( eirods::resource_operation_context* _ctx,
                                    void*               _buf, 
                                    int                 _len ) {
                                      
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    eirods::error s3FileWritePlugin( eirods::resource_operation_context* _ctx,
                                     void*               _buf, 
                                     int                 _len ) {
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    eirods::error s3FileClosePlugin(  eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
        
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    eirods::error s3FileUnlinkPlugin(
        eirods::resource_operation_context* _ctx )
    {
        eirods::error result = SUCCESS();
        
        // =-=-=-=-=-=-=-
        // check incoming parameters
        eirods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            result = PASSMSG(msg.str(), ret);  
        }
        else {
            
            // =-=-=-=-=-=-=-
            // get ref to fco
            eirods::first_class_object& _object = _ctx->fco();

            eirods::error ret;
            std::string bucket;
            std::string key;
            if(!(ret = parseS3Path(_object.physical_path(), bucket, key)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed parsing the S3 bucket and key from the physical path: \"";
                msg << _object.physical_path();
                msg << "\"";
                result = PASSMSG(msg.str(), ret);
            }

            else if(!(ret = s3Init()).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to initialize the S3 system.";
                result = PASSMSG(msg.str(), ret);
            } else {
                callback_data_t data;
                S3BucketContext bucketContext;

                bzero (&data, sizeof (data));

                S3ResponseHandler responseHandler = {
                    0, &responseCompleteCallback
                };

                S3_delete_object(&bucketContext, key.c_str(), 0, &responseHandler, &data);

                if (data.status != S3StatusOK) {
                    int status = data.status;
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Error unlinking the S3 object: \"";
                    msg << _object.physical_path();
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
        return result;
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    eirods::error s3FileStatPlugin(
        eirods::resource_operation_context* _ctx,
        struct stat* _statbuf )
    { 

        eirods::error result = SUCCESS();
        
        // =-=-=-=-=-=-=-
        // check incoming parameters
        eirods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            result = PASSMSG(msg.str(), ret);  
        }
        else {
            
            // =-=-=-=-=-=-=-
            // get ref to fco
            eirods::first_class_object& _object = _ctx->fco();

            bzero (_statbuf, sizeof (struct stat));

            if(_object.physical_path().find("/", _object.physical_path().size()) != std::string::npos) {
                // A directory
                _statbuf->st_mode = S_IFDIR;
            } else {
                    
                eirods::error ret;
                std::string bucket;
                std::string key;
                std::string key_id;
                std::string access_key;
                
                if(!(ret = parseS3Path(_object.physical_path(), bucket, key)).ok()) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Failed parsing the S3 bucket and key from the physical path: \"";
                    msg << _object.physical_path();
                    msg << "\"";
                    result = PASSMSG(msg.str(), ret);
                }

                else if(!(ret = s3Init()).ok()) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Failed to initialize the S3 system.";
                    result = PASSMSG(msg.str(), ret);
                }

                else if(!(ret = s3GetAuthCredentials(_ctx->prop_map(), key_id, access_key)).ok()) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Failed to get the S3 credentials properties.";
                    result = PASSMSG(msg.str(), ret);
                }
                
                else {
                    callback_data_t data;
                    S3BucketContext bucketContext;

                    bzero (&data, sizeof (data));

                    bzero (&bucketContext, sizeof (bucketContext));
                    bucketContext.bucketName = bucket.c_str();
                    bucketContext.protocol = S3ProtocolHTTPS;
                    bucketContext.uriStyle = S3UriStylePath;
                    bucketContext.accessKeyId = key_id.c_str();
                    bucketContext.secretAccessKey = access_key.c_str();

                    S3ListBucketHandler listBucketHandler = {
                        { &responsePropertiesCallback, &responseCompleteCallback },
                        &listBucketCallback
                    };

                    data.keyCount = 0;
                    data.allDetails = 1;

                    S3_list_bucket(&bucketContext, key.c_str(), NULL,
                                   NULL, 1, 0, &listBucketHandler, &data);

                    if (data.status != S3StatusOK) {
                        int status = data.status;
                        std::stringstream msg;
                        msg << __FUNCTION__;
                        msg << " - Error stat'ing the S3 object: \"";
                        msg << _object.physical_path();
                        msg << "\"";
                        if(status >= 0) {
                            msg << " - \"";
                            msg << S3_get_status_name((S3Status)status);
                            msg << "\"";
                            status = S3_FILE_STAT_ERR - status;
                        }
                        result = ERROR(status, msg.str());
                    } else {
                        _statbuf->st_mode = S_IFREG;
                        _statbuf->st_nlink = 1;
                        _statbuf->st_uid = getuid ();
                        _statbuf->st_gid = getgid ();
                        _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = data.s3Stat.lastModified;
                        _statbuf->st_size = data.s3Stat.size;
                    }
                }
            }
        }
        return result;
    }
        
    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    eirods::error s3FileFstatPlugin(  eirods::resource_operation_context* _ctx,
                                      struct stat*        _statbuf ) {
        return ERROR( SYS_NOT_SUPPORTED, "s3FileFstatPlugin" );
                                   
    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    eirods::error s3FileLseekPlugin(  eirods::resource_operation_context* _ctx, 
                                      size_t              _offset, 
                                      int                 _whence ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileLseekPlugin" );
                                       
    } // wosFileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX fsync
    eirods::error s3FileFsyncPlugin(  eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "wosFileFsyncPlugin" );

    } // s3FileFsyncPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    eirods::error s3FileMkdirPlugin(  eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileMkdirPlugin" );

    } // s3FileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    eirods::error s3FileChmodPlugin(  eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileChmodPlugin" );
    } // s3FileChmodPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    eirods::error s3FileRmdirPlugin(  eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileRmdirPlugin" );
    } // s3FileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    eirods::error s3FileOpendirPlugin( eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileOpendirPlugin" );
    } // s3FileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    eirods::error s3FileClosedirPlugin( eirods::resource_operation_context* _ctx) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileClosedirPlugin" );
    } // s3FileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    eirods::error s3FileReaddirPlugin( eirods::resource_operation_context* _ctx,
                                       struct rodsDirent**     _dirent_ptr ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileReaddirPlugin" );
    } // s3FileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    eirods::error s3FileStagePlugin( eirods::resource_operation_context* _ctx ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileStagePlugin" );
    } // s3FileStagePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    eirods::error s3FileRenamePlugin( eirods::resource_operation_context* _ctx,
                                      const char*         _new_file_name ) {

        return ERROR( SYS_NOT_SUPPORTED, "s3FileRenamePlugin" );
    } // s3FileRenamePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    eirods::error s3FileTruncatePlugin( 
        eirods::resource_operation_context* _ctx )
    { 
        return ERROR( SYS_NOT_SUPPORTED, "s3FileTruncatePlugin" );
    } // s3FileTruncatePlugin

    
    // interface to determine free space on a device given a path
    eirods::error s3FileGetFsFreeSpacePlugin(
        eirods::resource_operation_context* _ctx )
    {
        return ERROR(SYS_NOT_SUPPORTED, "s3FileGetFsFreeSpacePlugin");
            
    } // s3FileGetFsFreeSpacePlugin

    eirods::error s3FileCopyPlugin( int mode, const char *srcFileName, 
                                    const char *destFileName)
    {
        return ERROR( SYS_NOT_SUPPORTED, "s3FileCopyPlugin" );
    }


    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    eirods::error s3StageToCachePlugin(
        eirods::resource_operation_context* _ctx,
        char*                               _cache_file_name )
    {
        eirods::error result = SUCCESS();
        // =-=-=-=-=-=-=-
        // check incoming parameters
        eirods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            result = PASSMSG(msg.str(), ret);  
        }
        else {
            eirods::error ret;
            struct stat statbuf;
            std::string key_id;
            std::string access_key;
            
            eirods::file_object* object = dynamic_cast<eirods::file_object*>(&(_ctx->fco()));
            if(object == NULL) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to cast first class object to file object for file: \"";
                msg << _ctx->fco().physical_path();
                msg << "\"";
                result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
            }
            else if(!(ret = s3FileStatPlugin(_ctx, &statbuf)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed stat'ing the file: \"";
                msg << object->physical_path();
                msg << "\"";
                result = PASSMSG(msg.str(), ret);
            }
            else if((statbuf.st_mode & S_IFREG) == 0) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error stat'ing the file: \"";
                msg << object->physical_path();
                msg << "\"";
                result = ERROR(S3_FILE_STAT_ERR, msg.str());
            }
            else if(object->size() > 0 && object->size() != statbuf.st_size) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error for file: \"";
                msg << object->physical_path();
                msg << "\" inp data size: ";
                msg << object->size();
                msg << " does not match stat size: ";
                msg << statbuf.st_size;
                result = ERROR(SYS_COPY_LEN_ERR, msg.str());
            }

            else if(!(ret = s3GetAuthCredentials(_ctx->prop_map(), key_id, access_key)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to get S3 credential properties.";
                result = PASSMSG(msg.str(), ret);
            }
            
            else {
                rodsLong_t mySize = statbuf.st_size;
                ret = s3GetFile(_cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key);
                if(!ret.ok()) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Failed to copy the S3 object: \"";
                    msg << object->physical_path();
                    msg << "\" to the cache: \"";
                    msg << _cache_file_name;
                    msg << "\"";
                    result = PASSMSG(msg.str(), ret);
                }
            }
        }
        return result;
    } // s3StageToCachePlugin

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    eirods::error s3SyncToArchPlugin( 
        eirods::resource_operation_context* _ctx,
        char*                               _cache_file_name )
    {
        eirods::error result = SUCCESS();
        // =-=-=-=-=-=-=-
        // check incoming parameters
        eirods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            result = PASSMSG(msg.str(), ret);  
        }
        else {
            struct stat statbuf;
            int status;
            std::string key_id;
            std::string access_key;
            
            eirods::file_object* object = dynamic_cast<eirods::file_object*>(&(_ctx->fco()));
            if(object == NULL) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to cast first class object to file object for file: \"";
                msg << _ctx->fco().physical_path();
                msg << "\"";
                result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
            }

            else if((status = stat(_cache_file_name, &statbuf)) < 0) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to stat cache file: \"";
                msg << _cache_file_name;
                msg << "\"";
                result = ERROR(UNIX_FILE_STAT_ERR - errno, msg.str());
            }

            else if((statbuf.st_mode & S_IFREG) == 0) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Cache file: \"";
                msg << _cache_file_name;
                msg << "\" is not a file.";
                result = ERROR(UNIX_FILE_STAT_ERR, msg.str());
            }

            // This check is overridden since apparently the size in the inp is not initialized for a put - harry
            else if(false && (object->size() > 0 & object->size() != statbuf.st_size)) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Cache file: \"";
                msg << _cache_file_name;
                msg << "\" inp size: ";
                msg << object->size();
                msg << " does not match actual size: ";
                msg << statbuf.st_size;
                result = ERROR(SYS_COPY_LEN_ERR, msg.str());
            }

            else if(!(ret = s3GetAuthCredentials(_ctx->prop_map(), key_id, access_key)).ok()) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to get S3 credential properties.";
                result = PASSMSG(msg.str(), ret);
            }
            
            else {
                rodsLong_t data_size = statbuf.st_size;
                ret = s3PutFile(_cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key);
                if(!ret.ok()) {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Failed to copy the cache file: \"";
                    msg << _cache_file_name;
                    msg << "\" to the S3 object: \"";
                    msg << object->physical_path();
                    msg << "\"";
                    result = PASSMSG(msg.str(), ret);
                }
            }
        }
        return result;
    } // s3SyncToArchPlugin

    // =-=-=-=-=-=-=-
    // redirect_get - code to determine redirection for get operation
    eirods::error s3RedirectCreate( 
        eirods::resource_property_map& _prop_map,
        eirods::file_object&           _file_obj,
        const std::string&             _resc_name, 
        const std::string&             _curr_host, 
        float&                         _out_vote )
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        int resc_status = 0;
        std::string host_name;
            
        // =-=-=-=-=-=-=-
        // determine if the resource is down 
        if( !(ret = _prop_map.get< int >( "status", resc_status )).ok() ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to retrieve status property.";
            result = PASSMSG(msg.str(), ret);
        }

        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        else if( !(ret = _prop_map.get< std::string >( "location", host_name )).ok() ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to get location property.";
            result = PASSMSG(msg.str(), ret);
        } 

        else {
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

        return result;
    } // s3RedirectCreate

    // =-=-=-=-=-=-=-
    // redirect_get - code to determine redirection for get operation
    eirods::error s3RedirectOpen( 
        eirods::resource_property_map& _prop_map,
        eirods::file_object&           _file_obj,
        const std::string&             _resc_name, 
        const std::string&             _curr_host, 
        float&                         _out_vote )
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
        int resc_status = 0;
        std::string host_name;
            
        // =-=-=-=-=-=-=-
        // determine if the resource is down 
        if( !(ret = _prop_map.get< int >( "status", resc_status )).ok() ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to get status property for resource.";
            result = PASSMSG(msg.str(), ret);
        }

        // =-=-=-=-=-=-=-
        // get the resource host for comparison to curr host
        else if( !(ret = _prop_map.get< std::string >( "location", host_name )).ok() ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Failed to get the location property.";
            result = PASSMSG(msg.str(), ret);
        }

        else {
                
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
        return result;
    } // s3RedirectOpen

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    eirods::error s3RedirectPlugin( 
        eirods::resource_operation_context* _ctx,
        const std::string*                  _opr,
        const std::string*                  _curr_host,
        eirods::hierarchy_parser*           _out_parser,
        float*                              _out_vote )
    {
        eirods::error result = SUCCESS();
        eirods::error ret;
            
        // =-=-=-=-=-=-=-
        // check the context pointer
        if( !_ctx ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Invalid resource context.";
            result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
        }
         
        // =-=-=-=-=-=-=-
        // check the context validity
        else if(!(ret = _ctx->valid< eirods::file_object >()).ok()) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Invalid resource context.";
            result = PASSMSG(msg.str(), ret);
        }
 
        // =-=-=-=-=-=-=-
        // check incoming parameters
        else if( !_opr ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Null operation pointer.";
            result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
        }
        else if( !_curr_host ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Null current host pointer.";
            result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
        }
        else if( !_out_parser ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Null output hierarchy parser.";
            result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
        }
        else if( !_out_vote ) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Null output vote.";
            result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
        }
        else {
            std::string resc_name;
                
            // =-=-=-=-=-=-=-
            // cast down the chain to our understood object type
            eirods::file_object* file_obj = dynamic_cast< eirods::file_object* >( &(_ctx->fco()) );
            if(file_obj == NULL) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Error casting first class object to file object.";
                result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
            }
                
            // =-=-=-=-=-=-=-
            // get the name of this resource
            else if( !(ret = _ctx->prop_map().get< std::string >( "name", resc_name )).ok() ) {
                std::stringstream msg;
                msg << __FUNCTION__;
                msg << " - Failed to get resource name property.";
                result = PASSMSG(msg.str(), ret);
            }
            else {
                // =-=-=-=-=-=-=-
                // add ourselves to the hierarchy parser by default
                _out_parser->add_child( resc_name );

                // =-=-=-=-=-=-=-
                // test the operation to determine which choices to make
                if( eirods::EIRODS_OPEN_OPERATION == (*_opr) ) {
                    // =-=-=-=-=-=-=-
                    // call redirect determination for 'get' operation
                    result = s3RedirectOpen( _ctx->prop_map(), *file_obj, resc_name, (*_curr_host), (*_out_vote)  );

                } else if( eirods::EIRODS_CREATE_OPERATION == (*_opr) ) {
                    // =-=-=-=-=-=-=-
                    // call redirect determination for 'create' operation
                    result = s3RedirectCreate( _ctx->prop_map(), *file_obj, resc_name, (*_curr_host), (*_out_vote)  );
                }
                else {
                    std::stringstream msg;
                    msg << __FUNCTION__;
                    msg << " - Unknown redirect operation: \"";
                    msg << *_opr;
                    msg << "\"";
                    result = ERROR(SYS_INVALID_INPUT_PARAM, msg.str());
                }
            }
        }
        return result;
    } // s3RedirectPlugin

    class s3_resource : public eirods::resource {
    public:
        s3_resource( const std::string& _inst_name,
                     const std::string& _context ) :
            eirods::resource( _inst_name, _context ) {
            
            // =-=-=-=-=-=-=-
            // parse context string into property pairs assuming a ; as a separator
            std::vector< std::string > props;
            rodsLog( LOG_NOTICE, "context: %s", _context.c_str());
            eirods::string_tokenize( _context, ";", props );

            // =-=-=-=-=-=-=-
            // parse key/property pairs using = as a separator and
            // add them to the property list
            std::vector< std::string >::iterator itr = props.begin();
            for( ; itr != props.end(); ++itr ) {
                // =-=-=-=-=-=-=-
                // break up key and value into two strings
                std::vector< std::string > vals;
                eirods::string_tokenize( *itr, "=", vals );

                // =-=-=-=-=-=-=-
                // break up key and value into two strings
                rodsLog( LOG_NOTICE, "vals: %s %s", vals[0].c_str(), vals[1].c_str());
                properties_[ vals[0] ] = vals[1];
            } // for itr 

            // Add start and stop operations
            set_start_operation( "s3StartOperation" );
            set_stop_operation( "s3StopOperation" );
        } // ctor

        eirods::error need_post_disconnect_maintenance_operation( bool& _b ) {
            _b = false;
            return SUCCESS();
        }


        // =-=-=-=-=-=-=-
        // 3b. pass along a functor for maintenance work after
        //     the client disconnects, uncomment the first two lines for effect.
        eirods::error post_disconnect_maintenance_operation( eirods::pdmo_type& _op  ) {
            return SUCCESS();
        }

    }; // class s3_resource


    // =-=-=-=-=-=-=-
    // Create the plugin factory function which will return a microservice
    // table entry containing the microservice function pointer, the number
    // of parameters that the microservice takes and the name of the micro
    // service.  this will be called by the plugin loader in the irods server
    // to create the entry to the table when the plugin is requested.
    eirods::resource* plugin_factory(const std::string& _inst_name, const std::string& _context) {
        s3_resource* resc = new s3_resource(_inst_name, _context);

        resc->add_operation( eirods::RESOURCE_OP_CREATE,       "s3FileCreatePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_OPEN,         "s3FileOpenPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_READ,         "s3FileReadPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_WRITE,        "s3FileWritePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_CLOSE,        "s3FileClosePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_UNLINK,       "s3FileUnlinkPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_STAT,         "s3FileStatPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_FSTAT,        "s3FileFstatPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_FSYNC,        "s3FileFsyncPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_MKDIR,        "s3FileMkdirPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_CHMOD,        "s3FileChmodPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_OPENDIR,      "s3FileOpendirPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_READDIR,      "s3FileReaddirPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_STAGE,        "s3FileStagePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_RENAME,       "s3FileRenamePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_FREESPACE,    "s3FileGetFsFreeSpacePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_LSEEK,        "s3FileLseekPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_RMDIR,        "s3FileRmdirPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_CLOSEDIR,     "s3FileClosedirPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_TRUNCATE,     "s3FileTruncatePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_STAGETOCACHE, "s3StageToCachePlugin" );
        resc->add_operation( eirods::RESOURCE_OP_SYNCTOARCH,   "s3SyncToArchPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_REGISTERED,   "s3RegisteredPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_UNREGISTERED, "s3UnregisteredPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_MODIFIED,     "s3ModifiedPlugin" );
        resc->add_operation( eirods::RESOURCE_OP_RESOLVE_RESC_HIER, "s3RedirectPlugin" );

        // set some properties necessary for backporting to iRODS legacy code
        resc->set_property< int >( "check_path_perm", DO_CHK_PATH_PERM );
        resc->set_property< int >( "create_path",     NO_CREATE_PATH );
        resc->set_property< int >( "category",        FILE_CAT );

        //return dynamic_cast<eirods::resource*>( resc );
        return dynamic_cast<eirods::resource *> (resc);
    } // plugin_factory


}; // extern "C" 



