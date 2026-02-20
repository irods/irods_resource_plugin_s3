/**
 * object_chunked.c - Chunked Object Upload API Implementation
 *
 * This file implements the high-level S3_put_object_chunked() API function
 * that allows applications to upload objects using HTTP chunked transfer
 * encoding with optional trailing headers.
 *
 * Copyright 2024
 * Based on original libs3 code Copyright 2008 Bryan Ischo <bryan@ischo.com>
 *
 * Licensed under LGPL v3 or GPL v2 (same as original libs3)
 */

#include <string.h>
#include <stdio.h>
#include "libs3/libs3_chunked.h"
#include "libs3/request.h"

/**
 * Internal context for chunked PUT operation
 */
typedef struct ChunkedPutContext
{
    /* User callbacks and data */
    const S3PutObjectHandlerChunked *handler;
    void *callbackData;

    /* Chunked request state */
    ChunkedRequestState *chunkedState;

    /* Status */
    S3Status status;

} ChunkedPutContext;

/**
 * Wrapper callback for chunked data
 *
 * This wrapper adapts the chunked data callback to the standard
 * S3PutObjectDataCallback signature expected by RequestParams.
 */
static int chunked_data_wrapper(int bufferSize, char *buffer, void *callbackData)
{
    ChunkedPutContext *context = (ChunkedPutContext *)callbackData;

    if (!context || !context->handler || !context->handler->chunkedDataCallback) {
        return -1;
    }

    return context->handler->chunkedDataCallback(bufferSize, buffer, context->callbackData);
}

/**
 * Response properties callback wrapper
 *
 * Forwards response properties to user's callback if provided.
 */
static S3Status chunked_put_properties_callback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    ChunkedPutContext *context = (ChunkedPutContext *)callbackData;

    if (context->handler &&
        context->handler->responseHandler.propertiesCallback) {
        return context->handler->responseHandler.propertiesCallback(
            properties, context->callbackData);
    }

    return S3StatusOK;
}

/**
 * Response complete callback wrapper
 *
 * Cleans up chunked state and forwards completion to user's callback.
 */
static void chunked_put_complete_callback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    ChunkedPutContext *context = (ChunkedPutContext *)callbackData;

    /* Check if chunked transfer had errors */
    if (status == S3StatusOK && context->chunkedState) {
        if (S3_chunked_request_has_error(context->chunkedState)) {
            status = S3StatusChunkEncodingError;
        }
    }

    /* Store final status */
    context->status = status;

    /* Call user's completion callback */
    if (context->handler &&
        context->handler->responseHandler.completeCallback) {
        context->handler->responseHandler.completeCallback(
            status, error, context->callbackData);
    }

    /* Cleanup chunked state */
    if (context->chunkedState) {
        S3_destroy_chunked_request_state(context->chunkedState);
        context->chunkedState = NULL;
    }
}

/**
 * S3_put_object_chunked - Upload object using chunked transfer encoding
 *
 * This function uploads an object to S3 using HTTP chunked transfer encoding,
 * which allows streaming uploads without knowing the content length in advance.
 * Optionally, trailing headers can be sent after the body content.
 *
 * @param bucketContext Bucket context with credentials and settings
 * @param key Object key (name) in the bucket
 * @param putProperties Properties for the PUT operation (content-type, metadata, etc.)
 * @param requestContext Request context for async operations (NULL for synchronous)
 * @param timeoutMs Operation timeout in milliseconds (0 = no timeout)
 * @param handler Handler with callbacks for data and trailing headers
 * @param callbackData User data passed to all callbacks
 */
void S3_put_object_chunked(const S3BucketContext *bucketContext,
                           const char *key,
                           const S3PutProperties *putProperties,
                           S3RequestContext *requestContext,
                           int timeoutMs,
                           const S3PutObjectHandlerChunked *handler,
                           void *callbackData)
{
    ChunkedPutContext context;  /* Stack-allocated for thread safety */
    S3Status status;

    /* Validate inputs */
    if (!bucketContext || !key || !handler || !handler->chunkedDataCallback) {
        if (handler && handler->responseHandler.completeCallback) {
            handler->responseHandler.completeCallback(
                S3StatusInvalidChunkCallback, NULL, callbackData);
        }
        return;
    }

    /* Initialize context */
    memset(&context, 0, sizeof(ChunkedPutContext));
    context.handler = handler;
    context.callbackData = callbackData;
    context.status = S3StatusOK;

    /* Create chunked request state
     * AWS S3 DOES support trailing headers, but they must be declared using x-amz-trailer header
     */
    status = S3_create_chunked_request_state(
        &context.chunkedState,
        handler->chunkedDataCallback,
        handler->trailingHeadersCallback,  // Re-enable trailing headers support
        callbackData
    );

    if (status != S3StatusOK) {
        if (handler->responseHandler.completeCallback) {
            handler->responseHandler.completeCallback(status, NULL, callbackData);
        }
        return;
    }

    /* Set up the RequestParams - use Transfer-Encoding: chunked (toS3CallbackTotalSize = -1) */
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
        chunked_put_properties_callback,             // propertiesCallback
        chunked_data_wrapper,                        // toS3Callback
        -1,                                          // toS3CallbackTotalSize (-1 = chunked encoding)
        0,                                           // fromS3Callback
        chunked_put_complete_callback,               // completeCallback
        &context,                                    // callbackData
        timeoutMs,                                   // timeoutMs
        0,                                           // xAmzObjectAttributes
        context.chunkedState                         // chunkedState (for trailing headers)
    };

    /* Perform the request
     * The request infrastructure will call our chunked_data_wrapper callback
     * to get data, which will in turn call the user's chunkedDataCallback.
     */
    request_perform(&params, requestContext);
}

/**
 * S3_upload_part_chunked - Upload multipart part using chunked transfer encoding
 *
 * This function uploads a single part of a multipart upload using HTTP chunked
 * transfer encoding, which allows streaming uploads with trailing headers for
 * checksums computed during upload.
 *
 * @param bucketContext Bucket context with credentials and settings
 * @param key Object key (name) in the bucket
 * @param putProperties Properties for the PUT operation
 * @param seq Part sequence number (1-based)
 * @param uploadId Multipart upload ID
 * @param requestContext Request context for async operations (NULL for synchronous)
 * @param timeoutMs Operation timeout in milliseconds (0 = no timeout)
 * @param handler Handler with callbacks for data and trailing headers
 * @param callbackData User data passed to all callbacks
 */
void S3_upload_part_chunked(const S3BucketContext *bucketContext,
                            const char *key,
                            const S3PutProperties *putProperties,
                            int seq,
                            const char *uploadId,
                            S3RequestContext *requestContext,
                            int timeoutMs,
                            const S3PutObjectHandlerChunked *handler,
                            void *callbackData)
{
    ChunkedPutContext context;  /* Stack-allocated for thread safety */
    S3Status status;
    char queryParams[512];

    /* Build query parameters for multipart upload */
    snprintf(queryParams, sizeof(queryParams), "partNumber=%d&uploadId=%s", seq, uploadId);

    /* Validate inputs */
    if (!bucketContext || !key || !handler || !handler->chunkedDataCallback || !uploadId) {
        if (handler && handler->responseHandler.completeCallback) {
            handler->responseHandler.completeCallback(
                S3StatusInvalidChunkCallback, NULL, callbackData);
        }
        return;
    }

    /* Initialize context */
    memset(&context, 0, sizeof(ChunkedPutContext));
    context.handler = handler;
    context.callbackData = callbackData;
    context.status = S3StatusOK;

    /* Create chunked request state with trailing headers support */
    status = S3_create_chunked_request_state(
        &context.chunkedState,
        handler->chunkedDataCallback,
        handler->trailingHeadersCallback,
        callbackData
    );

    if (status != S3StatusOK) {
        if (handler->responseHandler.completeCallback) {
            handler->responseHandler.completeCallback(status, NULL, callbackData);
        }
        return;
    }

    /* Set up the RequestParams - use Transfer-Encoding: chunked (toS3CallbackTotalSize = -1) */
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
        queryParams,                                 // queryParams (partNumber & uploadId)
        0,                                           // subResource
        0,                                           // copySourceBucketName
        0,                                           // copySourceKey
        0,                                           // getConditions
        0,                                           // startByte
        0,                                           // byteCount
        putProperties,                               // putProperties
        chunked_put_properties_callback,             // propertiesCallback
        chunked_data_wrapper,                        // toS3Callback
        -1,                                          // toS3CallbackTotalSize (-1 = chunked encoding)
        0,                                           // fromS3Callback
        chunked_put_complete_callback,               // completeCallback
        &context,                                    // callbackData
        timeoutMs,                                   // timeoutMs
        0,                                           // xAmzObjectAttributes
        context.chunkedState                         // chunkedState (for trailing headers)
    };

    /* Perform the request */
    request_perform(&params, requestContext);
}
