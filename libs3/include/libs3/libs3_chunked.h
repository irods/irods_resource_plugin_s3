/**
 * libs3_chunked.h - Chunked Encoding and Trailing Headers Extension for libs3
 *
 * This header extends libs3.h with support for HTTP chunked transfer encoding
 * and trailing headers.
 *
 */

#ifndef LIBS3_CHUNKED_H
#define LIBS3_CHUNKED_H

/* Include the base libs3 header */
#include "libs3/libs3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * CHUNKED ENCODING STATUS CODES (additions to S3Status enum)
 * ======================================================================== */

#ifndef S3StatusChunkEncodingError
#define S3StatusChunkEncodingError ((S3Status)79)
#endif

#ifndef S3StatusTrailingHeadersError
#define S3StatusTrailingHeadersError ((S3Status)80)
#endif

#ifndef S3StatusInvalidChunkCallback
#define S3StatusInvalidChunkCallback ((S3Status)81)
#endif

/* ========================================================================
 * CHUNKED ENCODING AND TRAILING HEADERS CALLBACK TYPES
 * ======================================================================== */

/**
 * Chunked data callback
 *
 * This callback is invoked repeatedly to retrieve chunks of data for upload.
 *
 * @param bufferSize Maximum number of bytes that can be written to buffer
 * @param buffer Buffer to write chunk data into
 * @param callbackData User-provided callback data
 *
 * @return Number of bytes written (0 = end of stream, -1 = error)
 */
typedef int (*S3ChunkedDataCallback)(
    int bufferSize,
    char *buffer,
    void *callbackData);

/**
 * Trailing headers callback
 *
 * This callback is invoked after all data chunks have been sent, allowing
 * the application to provide additional HTTP headers (trailers) that are
 * sent after the body content. This is useful for checksums or metadata
 * that can only be computed after the entire payload has been processed.
 *
 * @param maxHeaders Maximum number of headers that can be set
 * @param headers Array to write trailing headers into
 * @param callbackData User-provided callback data
 *
 * @return Number of headers set (0 = no trailers, -1 = error)
 */
typedef int (*S3TrailingHeadersCallback)(
    int maxHeaders,
    S3NameValue *headers,
    void *callbackData);

/**
 * Chunked PUT object handler
 *
 * This handler structure supports chunked transfer encoding and trailing
 * headers for streaming uploads where the content length is not known
 * in advance.
 */
typedef struct S3PutObjectHandlerChunked
{
    S3ResponseHandler responseHandler;

    /* Callback to retrieve data chunks */
    S3ChunkedDataCallback chunkedDataCallback;

    /* Optional callback to set trailing headers after all chunks sent */
    S3TrailingHeadersCallback trailingHeadersCallback;

} S3PutObjectHandlerChunked;

/* ========================================================================
 * FUNCTION DECLARATIONS
 * ======================================================================== */

/**
 * PUT object with chunked encoding
 *
 * This function performs a PUT operation using HTTP chunked transfer encoding.
 * The content length is not specified in advance; instead, data is provided
 * through the chunkedDataCallback which is called repeatedly until it returns 0.
 *
 * Optionally, trailing headers can be sent after all chunks by providing a
 * trailingHeadersCallback. This is useful for checksums or metadata computed
 * during the upload process.
 *
 * @param bucketContext S3 bucket context
 * @param key Object key
 * @param putProperties PUT properties (content-type, metadata, etc.)
 * @param requestContext Request context (NULL for synchronous operation)
 * @param timeoutMs Operation timeout in milliseconds (0 = no timeout)
 * @param handler Chunked PUT handler with callbacks
 * @param callbackData User data passed to callbacks
 */
void S3_put_object_chunked(const S3BucketContext *bucketContext,
                           const char *key,
                           const S3PutProperties *putProperties,
                           S3RequestContext *requestContext,
                           int timeoutMs,
                           const S3PutObjectHandlerChunked *handler,
                           void *callbackData);

/**
 * Upload multipart part with chunked encoding
 *
 * This function uploads a single part of a multipart upload using HTTP chunked
 * transfer encoding with optional trailing headers. This is useful for streaming
 * uploads where trailing checksums are needed.
 *
 * @param bucketContext S3 bucket context
 * @param key Object key
 * @param putProperties PUT properties (content-type, metadata, etc.)
 * @param seq Part sequence number (1-based)
 * @param uploadId Multipart upload ID from initiate multipart upload
 * @param requestContext Request context (NULL for synchronous operation)
 * @param timeoutMs Operation timeout in milliseconds (0 = no timeout)
 * @param handler Chunked PUT handler with callbacks
 * @param callbackData User data passed to callbacks
 */
void S3_upload_part_chunked(const S3BucketContext *bucketContext,
                            const char *key,
                            const S3PutProperties *putProperties,
                            int seq,
                            const char *uploadId,
                            S3RequestContext *requestContext,
                            int timeoutMs,
                            const S3PutObjectHandlerChunked *handler,
                            void *callbackData);

/* ========================================================================
 * INTERNAL CHUNKED ENCODING FUNCTIONS
 * ======================================================================== */

/**
 * Opaque chunked request state structure
 * Implementation details are hidden in request_chunked.c
 */
typedef struct ChunkedRequestState ChunkedRequestState;

/**
 * Create chunked request state
 *
 * @param statePtr Output parameter for created state
 * @param chunkedCallback Callback to retrieve data chunks
 * @param trailingCallback Optional callback for trailing headers
 * @param callbackData User data passed to callbacks
 * @return S3StatusOK on success, error code otherwise
 */
S3Status S3_create_chunked_request_state(ChunkedRequestState **statePtr,
                                        S3ChunkedDataCallback chunkedCallback,
                                        S3TrailingHeadersCallback trailingCallback,
                                        void *callbackData);

/**
 * Destroy chunked request state
 *
 * @param state State to destroy
 */
void S3_destroy_chunked_request_state(ChunkedRequestState *state);

/**
 * Check if chunked request has error
 *
 * @param state Chunked request state
 * @return 1 if error occurred, 0 otherwise
 */
int S3_chunked_request_has_error(const ChunkedRequestState *state);

/**
 * Setup CURL for chunked encoding
 *
 * @param curl CURL handle (void* to avoid exposing CURL* in public API)
 * @param state Chunked request state
 * @return S3StatusOK on success, error code otherwise
 */
S3Status request_setup_chunked_encoding(void *curl, ChunkedRequestState *state);

/**
 * Set signature information for trailing header signing
 *
 * This must be called from request.c after computing the request signature
 * to enable x-amz-trailer-signature calculation for STREAMING-UNSIGNED-PAYLOAD-TRAILER.
 *
 * @param state Chunked request state
 * @param seedSignature Hex signature from Authorization header (64 chars + null)
 * @param timestamp Request timestamp in ISO8601 format
 * @param credentialScope Credential scope (date/region/service/aws4_request)
 * @param signingKey AWS4 signing key (32 bytes)
 * @return S3StatusOK on success, error code otherwise
 */
S3Status chunked_set_signature_info(ChunkedRequestState *state,
                                   const char *seedSignature,
                                   const char *timestamp,
                                   const char *credentialScope,
                                   const unsigned char *signingKey);

#ifdef __cplusplus
}
#endif

#endif /* LIBS3_CHUNKED_H */
