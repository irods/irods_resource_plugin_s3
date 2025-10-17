/** **************************************************************************
 * object.c
 *
 * Copyright 2008 Bryan Ischo <bryan@ischo.com>
 *
 * This file is part of libs3.
 *
 * libs3 is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, version 3 or above of the License.  You can also
 * redistribute and/or modify it under the terms of the GNU General Public
 * License, version 2 or above of the License.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of this library and its programs with the
 * OpenSSL library, and distribute linked combinations including the two.
 *
 * libs3 is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * version 3 along with libs3, in a file named COPYING.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * You should also have received a copy of the GNU General Public License
 * version 2 along with libs3, in a file named COPYING-GPLv2.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 ************************************************************************** **/

#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include "libs3/libs3.h"
#include "libs3/request.h"

// put object ----------------------------------------------------------------

void S3_put_object(const S3BucketContext* bucketContext,
                   const char* key,
                   uint64_t contentLength,
                   const S3PutProperties* putProperties,
                   S3RequestContext* requestContext,
                   int timeoutMs,
                   const S3PutObjectHandler* handler,
                   void* callbackData)
{
	// Set up the RequestParams
	RequestParams params = {
		HttpRequestTypePUT,                          // httpRequestType
		{bucketContext->hostName,                    // hostName
	     bucketContext->bucketName,                  // bucketName
	     bucketContext->protocol,                    // protocol
	     bucketContext->uriStyle,                    // uriStyle
	     bucketContext->accessKeyId,                 // accessKeyId
	     bucketContext->secretAccessKey,             // secretAccessKey
	     bucketContext->securityToken,               // securityToken
	     bucketContext->authRegion,                  // authRegion
	     bucketContext->stsDate},                    // stsDate
		key,                                         // key
		0,                                           // queryParams
		0,                                           // subResource
		0,                                           // copySourceBucketName
		0,                                           // copySourceKey
		0,                                           // getConditions
		0,                                           // startByte
		0,                                           // byteCount
		putProperties,                               // putProperties
		handler->responseHandler.propertiesCallback, // propertiesCallback
		handler->putObjectDataCallback,              // toS3Callback
		contentLength,                               // toS3CallbackTotalSize
		0,                                           // fromS3Callback
		handler->responseHandler.completeCallback,   // completeCallback
		callbackData,                                // callbackData
		timeoutMs,                                   // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}

// copy object ---------------------------------------------------------------

typedef struct CopyObjectData
{
	SimpleXml simpleXml;

	S3ResponsePropertiesCallback* responsePropertiesCallback;
	S3ResponseCompleteCallback* responseCompleteCallback;
	void* callbackData;

	int64_t* lastModifiedReturn;
	int eTagReturnSize;
	char* eTagReturn;
	int eTagReturnLen;

	string_buffer(lastModified, 256);
} CopyObjectData;

static S3Status copyObjectXmlCallback(const char* elementPath, const char* data, int dataLen, void* callbackData)
{
	CopyObjectData* coData = (CopyObjectData*) callbackData;

	int fit;

	if (data) {
		if (!strcmp(elementPath, "CopyObjectResult/LastModified")) {
			string_buffer_append(coData->lastModified, data, dataLen, fit);
		}
		else if (!strcmp(elementPath, "CopyObjectResult/ETag") || !strcmp(elementPath, "CopyPartResult/ETag")) {
			if (coData->eTagReturnSize && coData->eTagReturn) {
				coData->eTagReturnLen += snprintf(&(coData->eTagReturn[coData->eTagReturnLen]),
				                                  coData->eTagReturnSize - coData->eTagReturnLen - 1,
				                                  "%.*s",
				                                  dataLen,
				                                  data);
				if (coData->eTagReturnLen >= coData->eTagReturnSize) {
					return S3StatusXmlParseFailure;
				}
			}
		}
	}

	/* Avoid compiler error about variable set but not used */
	(void) fit;

	return S3StatusOK;
}

static S3Status copyObjectPropertiesCallback(const S3ResponseProperties* responseProperties, void* callbackData)
{
	CopyObjectData* coData = (CopyObjectData*) callbackData;

	return (*(coData->responsePropertiesCallback))(responseProperties, coData->callbackData);
}

static S3Status copyObjectDataCallback(int bufferSize, const char* buffer, void* callbackData)
{
	CopyObjectData* coData = (CopyObjectData*) callbackData;

	return simplexml_add(&(coData->simpleXml), buffer, bufferSize);
}

static void copyObjectCompleteCallback(S3Status requestStatus, const S3ErrorDetails* s3ErrorDetails, void* callbackData)
{
	CopyObjectData* coData = (CopyObjectData*) callbackData;

	if (coData->lastModifiedReturn) {
		time_t lastModified = -1;
		if (coData->lastModifiedLen) {
			lastModified = parseIso8601Time(coData->lastModified);
		}

		*(coData->lastModifiedReturn) = lastModified;
	}

	(*(coData->responseCompleteCallback))(requestStatus, s3ErrorDetails, coData->callbackData);

	simplexml_deinitialize(&(coData->simpleXml));

	free(coData);
}

void S3_copy_object(const S3BucketContext* bucketContext,
                    const char* key,
                    const char* destinationBucket,
                    const char* destinationKey,
                    const S3PutProperties* putProperties,
                    int64_t* lastModifiedReturn,
                    int eTagReturnSize,
                    char* eTagReturn,
                    S3RequestContext* requestContext,
                    int timeoutMs,
                    const S3ResponseHandler* handler,
                    void* callbackData)
{
	/* Use the range copier with 0 length */
	S3_copy_object_range(bucketContext,
	                     key,
	                     destinationBucket,
	                     destinationKey,
	                     0,
	                     NULL, // No multipart
	                     0,
	                     0, // No length => std. copy of < 5GB
	                     putProperties,
	                     lastModifiedReturn,
	                     eTagReturnSize,
	                     eTagReturn,
	                     requestContext,
	                     timeoutMs,
	                     handler,
	                     callbackData);
}

void S3_copy_object_range(const S3BucketContext* bucketContext,
                          const char* key,
                          const char* destinationBucket,
                          const char* destinationKey,
                          const int partNo,
                          const char* uploadId,
                          const unsigned long startOffset,
                          const unsigned long count,
                          const S3PutProperties* putProperties,
                          int64_t* lastModifiedReturn,
                          int eTagReturnSize,
                          char* eTagReturn,
                          S3RequestContext* requestContext,
                          int timeoutMs,
                          const S3ResponseHandler* handler,
                          void* callbackData)
{
	// Create the callback data
	CopyObjectData* data = (CopyObjectData*) malloc(sizeof(CopyObjectData));
	if (!data) {
		(*(handler->completeCallback))(S3StatusOutOfMemory, 0, callbackData);
		return;
	}

	simplexml_initialize(&(data->simpleXml), &copyObjectXmlCallback, data);

	data->responsePropertiesCallback = handler->propertiesCallback;
	data->responseCompleteCallback = handler->completeCallback;
	data->callbackData = callbackData;

	data->lastModifiedReturn = lastModifiedReturn;
	data->eTagReturnSize = eTagReturnSize;
	data->eTagReturn = eTagReturn;
	if (data->eTagReturnSize && data->eTagReturn) {
		data->eTagReturn[0] = 0;
	}
	data->eTagReturnLen = 0;
	string_buffer_initialize(data->lastModified);

	// If there's a sequence ID > 0 then add a subResource, OTW pass in NULL
	char queryParams[512];
	char* qp = NULL;
	if (partNo > 0) {
		snprintf(queryParams, 512, "partNumber=%d&uploadId=%s", partNo, uploadId);
		qp = queryParams;
	}

	// Set up the RequestParams
	RequestParams params = {
		HttpRequestTypeCOPY,                                                // httpRequestType
		{bucketContext->hostName,                                           // hostName
	     destinationBucket ? destinationBucket : bucketContext->bucketName, // bucketName
	     bucketContext->protocol,                                           // protocol
	     bucketContext->uriStyle,                                           // uriStyle
	     bucketContext->accessKeyId,                                        // accessKeyId
	     bucketContext->secretAccessKey,                                    // secretAccessKey
	     bucketContext->securityToken,                                      // securityToken
	     bucketContext->authRegion,                                         // authRegion
	     bucketContext->stsDate},                                           // stsDate
		destinationKey ? destinationKey : key,                              // key
		qp,                                                                 // queryParams
		0,                                                                  // subResource
		bucketContext->bucketName,                                          // copySourceBucketName
		key,                                                                // copySourceKey
		0,                                                                  // getConditions
		startOffset,                                                        // startByte
		count,                                                              // byteCount
		putProperties,                                                      // putProperties
		&copyObjectPropertiesCallback,                                      // propertiesCallback
		0,                                                                  // toS3Callback
		0,                                                                  // toS3CallbackTotalSize
		&copyObjectDataCallback,                                            // fromS3Callback
		&copyObjectCompleteCallback,                                        // completeCallback
		data,                                                               // callbackData
		timeoutMs,                                                          // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}

// get object ----------------------------------------------------------------

void S3_get_object(const S3BucketContext* bucketContext,
                   const char* key,
                   const S3GetConditions* getConditions,
                   uint64_t startByte,
                   uint64_t byteCount,
                   S3RequestContext* requestContext,
                   int timeoutMs,
                   const S3GetObjectHandler* handler,
                   void* callbackData)
{
	// Set up the RequestParams
	RequestParams params = {
		HttpRequestTypeGET,                          // httpRequestType
		{bucketContext->hostName,                    // hostName
	     bucketContext->bucketName,                  // bucketName
	     bucketContext->protocol,                    // protocol
	     bucketContext->uriStyle,                    // uriStyle
	     bucketContext->accessKeyId,                 // accessKeyId
	     bucketContext->secretAccessKey,             // secretAccessKey
	     bucketContext->securityToken,               // securityToken
	     bucketContext->authRegion,                  // authRegion
	     bucketContext->stsDate},                    // stsDate
		key,                                         // key
		0,                                           // queryParams
		0,                                           // subResource
		0,                                           // copySourceBucketName
		0,                                           // copySourceKey
		getConditions,                               // getConditions
		startByte,                                   // startByte
		byteCount,                                   // byteCount
		0,                                           // putProperties
		handler->responseHandler.propertiesCallback, // propertiesCallback
		0,                                           // toS3Callback
		0,                                           // toS3CallbackTotalSize
		handler->getObjectDataCallback,              // fromS3Callback
		handler->responseHandler.completeCallback,   // completeCallback
		callbackData,                                // callbackData
		timeoutMs,                                   // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}

// head object ---------------------------------------------------------------

void S3_head_object(const S3BucketContext* bucketContext,
                    const char* key,
                    S3RequestContext* requestContext,
                    int timeoutMs,
                    const S3ResponseHandler* handler,
                    void* callbackData)
{
	// Set up the RequestParams
	RequestParams params = {
		HttpRequestTypeHEAD,             // httpRequestType
		{bucketContext->hostName,        // hostName
	     bucketContext->bucketName,      // bucketName
	     bucketContext->protocol,        // protocol
	     bucketContext->uriStyle,        // uriStyle
	     bucketContext->accessKeyId,     // accessKeyId
	     bucketContext->secretAccessKey, // secretAccessKey
	     bucketContext->securityToken,   // securityToken
	     bucketContext->authRegion,      // authRegion
	     bucketContext->stsDate},        // stsDate
		key,                             // key
		0,                               // queryParams
		0,                               // subResource
		0,                               // copySourceBucketName
		0,                               // copySourceKey
		0,                               // getConditions
		0,                               // startByte
		0,                               // byteCount
		0,                               // putProperties
		handler->propertiesCallback,     // propertiesCallback
		0,                               // toS3Callback
		0,                               // toS3CallbackTotalSize
		0,                               // fromS3Callback
		handler->completeCallback,       // completeCallback
		callbackData,                    // callbackData
		timeoutMs,                       // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}

// delete object --------------------------------------------------------------

void S3_delete_object(const S3BucketContext* bucketContext,
                      const char* key,
                      S3RequestContext* requestContext,
                      int timeoutMs,
                      const S3ResponseHandler* handler,
                      void* callbackData)
{
	// Set up the RequestParams
	RequestParams params = {
		HttpRequestTypeDELETE,           // httpRequestType
		{bucketContext->hostName,        // hostName
	     bucketContext->bucketName,      // bucketName
	     bucketContext->protocol,        // protocol
	     bucketContext->uriStyle,        // uriStyle
	     bucketContext->accessKeyId,     // accessKeyId
	     bucketContext->secretAccessKey, // secretAccessKey
	     bucketContext->securityToken,   // securityToken
	     bucketContext->authRegion,      // authRegion
	     bucketContext->stsDate},        // stsDate
		key,                             // key
		0,                               // queryParams
		0,                               // subResource
		0,                               // copySourceBucketName
		0,                               // copySourceKey
		0,                               // getConditions
		0,                               // startByte
		0,                               // byteCount
		0,                               // putProperties
		handler->propertiesCallback,     // propertiesCallback
		0,                               // toS3Callback
		0,                               // toS3CallbackTotalSize
		0,                               // fromS3Callback
		handler->completeCallback,       // completeCallback
		callbackData,                    // callbackData
		timeoutMs,                       // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}

// restore object --------------------------------------------------------------

typedef struct RestoreObjectData
{
	SimpleXml simplexml;
	void* userdata;
	S3RestoreObjectHandler* handler;

	string_buffer(location, 128);
} RestoreObjectData;

static S3Status restoreObjectCallback(int bufferSize, const char* buffer, void* callbackData)
{
	RestoreObjectData* data = (RestoreObjectData*) callbackData;
	return simplexml_add(&(data->simplexml), buffer, bufferSize);
}

static S3Status restoreObjectPropertiesCallback(const S3ResponseProperties* responseProperties, void* callbackData)
{
	RestoreObjectData* data = (RestoreObjectData*) callbackData;

	if (data->handler->responseHandler.propertiesCallback) {
		(*(data->handler->responseHandler.propertiesCallback))(responseProperties, data->userdata);
	}
	return S3StatusOK;
}

static void restoreObjectCompleteCallback(S3Status requestStatus,
                                          const S3ErrorDetails* s3ErrorDetails,
                                          void* callbackData)
{
	RestoreObjectData* data = (RestoreObjectData*) callbackData;
	if (data->handler->responseHandler.completeCallback) {
		(*(data->handler->responseHandler.completeCallback))(requestStatus, s3ErrorDetails, data->userdata);
	}
	simplexml_deinitialize(&(data->simplexml));
	free(data);
}

static int restoreObjectPutCallback(int bufferSize, char* buffer, void* callbackData)
{
	RestoreObjectData* data = (RestoreObjectData*) callbackData;
	if (data->handler->putObjectDataCallback) {
		return data->handler->putObjectDataCallback(bufferSize, buffer, data->userdata);
	}
	else {
		return -1;
	}
}

void S3_restore_object(S3BucketContext* bucketContext,
                       const char* key,
                       S3RestoreObjectHandler* handler,
                       int contentLength,
                       S3RequestContext* requestContext,
                       int timeoutMs,
                       void* callbackData)
{
	RestoreObjectData* data = (RestoreObjectData*) malloc(sizeof(RestoreObjectData));
	data->userdata = callbackData;
	data->handler = handler;

	simplexml_initialize(&(data->simplexml), NULL, data);

	RequestParams params = {
		HttpRequestTypePOST,             // httpRequestType
		{bucketContext->hostName,        // hostName
	     bucketContext->bucketName,      // bucketName
	     bucketContext->protocol,        // protocol
	     bucketContext->uriStyle,        // uriStyle
	     bucketContext->accessKeyId,     // accessKeyId
	     bucketContext->secretAccessKey, // secretAccessKey
	     bucketContext->securityToken,   // securityToken
	     bucketContext->authRegion,      // authRegion
	     bucketContext->stsDate},        // stsDate
		key,                             // key
		0,                               // queryParams
		"restore",                       // subResource
		0,                               // copySourceBucketName
		0,                               // copySourceKey
		0,                               // getConditions
		0,                               // startByte
		0,                               // byteCount
		0,                               // putProperties
		restoreObjectPropertiesCallback, // propertiesCallback
		restoreObjectPutCallback,        // toS3Callback
		contentLength,                   // toS3CallbackTotalSize
		restoreObjectCallback,           // fromS3Callback
		restoreObjectCompleteCallback,   // completeCallback
		data,                            // callbackData
		timeoutMs,                       // timeoutMs
		0,                               // xAmzObjectAttributes
		0                                // chunkedState
	};

	request_perform(&params, requestContext);
}

// get object attributes ----------------------------------------------------------------
typedef struct GetObjectAttributesData
{
	SimpleXml simpleXml;
	S3GetObjectAttributesHandler* handler;

    // 64 is longer than any of these need to be
    string_buffer(checksumCRC32, 64);
    string_buffer(checksumCRC32C, 64);
    string_buffer(checksumCRC64NVME, 64);
    string_buffer(checksumSHA1, 64);
    string_buffer(checksumSHA256, 64);
    string_buffer(checksumType, 64);
    string_buffer(storageClass, 64);
    string_buffer(objectSize, 64);
	void* userdata;

    // Part info not implemented
} GetObjectAttributesData;

static S3Status GetObjectAttributesCallback(int bufferSize, const char* buffer, void* callbackData)
{
	GetObjectAttributesData* goaData = (GetObjectAttributesData*) callbackData;
	return simplexml_add(&(goaData->simpleXml), buffer, bufferSize);
}

static void GetObjectAttributesCompleteCallback(S3Status requestStatus,
                                             const S3ErrorDetails* s3ErrorDetails,
                                             void* callbackData)
{
	GetObjectAttributesData* goaData = (GetObjectAttributesData*) callbackData;

	if (goaData->handler->responseHandler.completeCallback) {
		(*goaData->handler->responseHandler.completeCallback)(requestStatus, s3ErrorDetails, goaData->userdata);
	}

	if (goaData->handler->responseXmlCallback) {
		(*goaData->handler->responseXmlCallback)(goaData->checksumCRC32,
                goaData->checksumCRC32C,
                goaData->checksumCRC64NVME,
                goaData->checksumSHA1,
                goaData->checksumSHA256,
                goaData->checksumType,
                goaData->storageClass,
                goaData->objectSize,
                goaData->userdata);
	}

	simplexml_deinitialize(&(goaData->simpleXml));
	free(goaData);
}

static S3Status getObjectAttributesXmlCallback(const char* elementPath, const char* data, int dataLen, void* callbackData)
{
	GetObjectAttributesData* goaData = (GetObjectAttributesData*) callbackData;
	int fit = 1;
	if (!data) {
		return S3StatusOK;
	}

    // Note that elementPath is a 512 character array.
    // Use strncasecmp because MinIO returns <getObjectAttributesResponse> (lowercase 'g')
    // while AWS returns <GetObjectAttributesResponse> (uppercase 'G').
    if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumCRC32", 511)) {
        string_buffer_append(goaData->checksumCRC32, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumCRC32C", 511)) {
        string_buffer_append(goaData->checksumCRC32C, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumCRC64NVME", 511)) {
        string_buffer_append(goaData->checksumCRC64NVME, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumSHA1", 511)) {
        string_buffer_append(goaData->checksumSHA1, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumSHA256", 511)) {
        string_buffer_append(goaData->checksumSHA256, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/Checksum/ChecksumType", 511)) {
        string_buffer_append(goaData->checksumType, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/StorageClass", 511)) {
        string_buffer_append(goaData->storageClass, data, dataLen, fit);
    }
    else if (0 == strncasecmp(elementPath, "GetObjectAttributesResponse/ObjectSize", 511)) {
        // Keep this as a string.  User can parse it as a long long if desired.
        string_buffer_append(goaData->objectSize, data, dataLen, fit);
    }
    // Note: not parsing/handling <ObjectParts> return values

	(void) fit;
	return S3StatusOK;
}

static S3Status GetObjectAttributesPropertiesCallback(const S3ResponseProperties* properties, void* callbackData)
{
	GetObjectAttributesData* goaData = (GetObjectAttributesData*) callbackData;
	return goaData->handler->responseHandler.propertiesCallback(properties, goaData->userdata);
}

void S3_get_object_attributes(S3BucketContext* bucketContext,
                           const char* key,
                           S3PutProperties* putProperties,
                           S3GetObjectAttributesHandler* handler,
                           S3RequestContext* requestContext,
                           const char *xAmzObjectAttributes,
                           int timeoutMs,
                           void* callbackData)
{
	GetObjectAttributesData* goaData = (GetObjectAttributesData*) malloc(sizeof(GetObjectAttributesData));
	simplexml_initialize(&(goaData->simpleXml), &getObjectAttributesXmlCallback, goaData);
	string_buffer_initialize(goaData->checksumCRC32);
	string_buffer_initialize(goaData->checksumCRC32C);
	string_buffer_initialize(goaData->checksumCRC64NVME);
	string_buffer_initialize(goaData->checksumSHA1);
	string_buffer_initialize(goaData->checksumSHA256);
	string_buffer_initialize(goaData->checksumType);
	string_buffer_initialize(goaData->storageClass);
	string_buffer_initialize(goaData->objectSize);

	goaData->handler = handler;
	goaData->userdata = callbackData;

	RequestParams params = {
		HttpRequestTypeGET,                    // httpRequestType
		{bucketContext->hostName,              // hostName
	     bucketContext->bucketName,            // bucketName
	     bucketContext->protocol,              // protocol
	     bucketContext->uriStyle,              // uriStyle
	     bucketContext->accessKeyId,           // accessKeyId
	     bucketContext->secretAccessKey,       // secretAccessKey
	     bucketContext->securityToken,         // securityToken
	     bucketContext->authRegion,            // authRegion
	     bucketContext->stsDate},              // stsDate
		key,                                   // key
		0,                                     // queryParams
		"attributes",                          // subResource
		0,                                     // copySourceBucketName
		0,                                     // copySourceKey
		0,                                     // getConditions
		0,                                     // startByte
		0,                                     // byteCount
		putProperties,                         // putProperties
		GetObjectAttributesPropertiesCallback, // propertiesCallback
		0,                                     // toS3Callback
		0,                                     // toS3CallbackTotalSize
		GetObjectAttributesCallback,           // fromS3Callback
		GetObjectAttributesCompleteCallback,   // completeCallback
		goaData,                               // callbackData
		timeoutMs,                             // timeoutMs
		xAmzObjectAttributes,                  // xAmzObjectAttributes
		0                                      // chunkedState
	};

	// Perform the request
	request_perform(&params, requestContext);
}
