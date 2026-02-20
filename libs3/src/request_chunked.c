/**
 * request_chunked.c - Chunked Encoding Implementation for libs3
 *
 * This file implements HTTP chunked transfer encoding and trailing headers
 * support for the libs3 library.
 *
 * Copyright 2024
 * Based on original libs3 code Copyright 2008 Bryan Ischo <bryan@ischo.com>
 *
 * Licensed under LGPL v3 or GPL v2 (same as original libs3)
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <curl/curl.h>
#include "libs3/libs3_chunked.h"
#include "libs3/request.h"

#include <openssl/hmac.h>
#include <openssl/sha.h>
#define S3_SHA256_DIGEST_LENGTH SHA256_DIGEST_LENGTH

/* Default chunk buffer size (64KB) */
#define DEFAULT_CHUNK_BUFFER_SIZE (64 * 1024)

/* Maximum trailing header line length */
#define MAX_TRAILER_LINE_LENGTH 512

/**
 * Chunked request state structure
 *
 * This structure tracks the state of a chunked transfer encoding request,
 * including buffers, callbacks, and trailing header information.
 */
typedef enum {
    CHUNK_STATE_HEADER,      /* Sending chunk size header: "XXXX\r\n" */
    CHUNK_STATE_DATA,        /* Sending chunk data */
    CHUNK_STATE_DATA_END,    /* Sending "\r\n" after chunk data */
    CHUNK_STATE_FINAL,       /* Sending final "0\r\n" */
    CHUNK_STATE_TRAILERS,    /* Sending trailing headers */
    CHUNK_STATE_DONE         /* All done */
} ChunkPhase;

typedef struct ChunkedRequestState
{
    /* Callbacks */
    S3ChunkedDataCallback chunkedDataCallback;
    S3TrailingHeadersCallback trailingHeadersCallback;
    void *callbackData;

    /* Chunk buffer */
    char *chunkBuffer;
    int chunkBufferSize;
    int chunkBufferUsed;    /* Bytes currently in buffer */
    int chunkBufferOffset;  /* Read position in buffer */

    /* Manual chunking state */
    ChunkPhase phase;
    char chunkHeader[32];    /* Buffer for chunk size header like "1000\r\n" */
    int chunkHeaderLen;
    int chunkHeaderOffset;
    char trailerBuffer[1024]; /* Buffer for formatted trailing headers */
    int trailerBufferLen;
    int trailerBufferOffset;

    /* State flags */
    int allChunksSent;
    int trailingHeadersGenerated;
    int errorOccurred;

    /* Trailing headers */
    S3NameValue trailingHeaders[S3_MAX_METADATA_COUNT];
    int trailingHeadersCount;

    /* Statistics */
    uint64_t totalBytesSent;

    /* Signature information for trailing headers */
    char seedSignature[65];           /* Hex signature from Authorization header */
    char requestDateISO8601[64];      /* Request timestamp */
    char credentialScope[128];        /* Date/region/service/aws4_request */
    unsigned char signingKey[32];     /* AWS4 signing key */
    int hasSignatureInfo;             /* 1 if signature info is populated */

} ChunkedRequestState;

/**
 * Compute SHA256 hash and return as hex string
 *
 * @param data Data to hash
 * @param len Length of data
 * @param hexOutput Buffer for hex output (must be at least 65 bytes)
 */
static void compute_sha256_hex(const unsigned char *data, size_t len, char *hexOutput)
{
    unsigned char hash[S3_SHA256_DIGEST_LENGTH];

    SHA256(data, len, hash);

    for (int i = 0; i < S3_SHA256_DIGEST_LENGTH; i++) {
        sprintf(hexOutput + (i * 2), "%02x", hash[i]);
    }
    hexOutput[S3_SHA256_DIGEST_LENGTH * 2] = '\0';
}

/**
 * Compute HMAC-SHA256 signature
 *
 * @param key Signing key
 * @param keyLen Length of signing key
 * @param data Data to sign
 * @param dataLen Length of data
 * @param output Buffer for output (must be at least S3_SHA256_DIGEST_LENGTH bytes)
 */
static void compute_hmac_sha256(const unsigned char *key, size_t keyLen,
                                const unsigned char *data, size_t dataLen,
                                unsigned char *output)
{
    HMAC(EVP_sha256(), key, (int)keyLen, data, dataLen, output, NULL);
}

/**
 * Compute HMAC-SHA256 signature and return as hex string
 *
 * @param key Signing key
 * @param keyLen Length of signing key
 * @param data Data to sign
 * @param dataLen Length of data
 * @param hexOutput Buffer for hex output (must be at least 65 bytes)
 */
static void compute_hmac_sha256_hex(const unsigned char *key, size_t keyLen,
                                    const unsigned char *data, size_t dataLen,
                                    char *hexOutput)
{
    unsigned char signature[S3_SHA256_DIGEST_LENGTH];
    compute_hmac_sha256(key, keyLen, data, dataLen, signature);

    for (int i = 0; i < S3_SHA256_DIGEST_LENGTH; i++) {
        sprintf(hexOutput + (i * 2), "%02x", signature[i]);
    }
    hexOutput[S3_SHA256_DIGEST_LENGTH * 2] = '\0';
}

/**
 * Calculate AWS4-HMAC-SHA256-TRAILER signature for trailing headers
 *
 * @param state Chunked request state with signature information
 * @param trailingHeadersStr Formatted trailing headers string
 * @param signatureHex Output buffer for hex signature (must be at least 65 bytes)
 * @return S3StatusOK on success, error otherwise
 */
static S3Status calculate_trailer_signature(ChunkedRequestState *state,
                                            const char *trailingHeadersStr,
                                            char *signatureHex)
{
    if (!state->hasSignatureInfo) {
        return S3StatusInternalError;
    }

    /* Compute SHA256 hash of trailing headers */
    char hashedTrailers[65];
    compute_sha256_hex((const unsigned char *)trailingHeadersStr,
                      strlen(trailingHeadersStr),
                      hashedTrailers);

    /* Build string to sign:
     * AWS4-HMAC-SHA256-TRAILER\n
     * <timestamp>\n
     * <credential-scope>\n
     * <previous-signature>\n
     * <hashed-trailer-headers>
     */
    char stringToSign[1024];
    int len = snprintf(stringToSign, sizeof(stringToSign),
                      "AWS4-HMAC-SHA256-TRAILER\n%s\n%s\n%s\n%s",
                      state->requestDateISO8601,
                      state->credentialScope,
                      state->seedSignature,
                      hashedTrailers);

    if (len >= (int)sizeof(stringToSign)) {
        return S3StatusInternalError;
    }

    /* Compute HMAC-SHA256 using signing key */
    compute_hmac_sha256_hex(state->signingKey, S3_SHA256_DIGEST_LENGTH,
                           (const unsigned char *)stringToSign, len,
                           signatureHex);

    return S3StatusOK;
}

/**
 * Initialize chunked request state
 *
 * @param state Pointer to state structure to initialize
 * @param chunkedCallback Callback for retrieving chunk data
 * @param trailingCallback Optional callback for trailing headers
 * @param callbackData User data passed to callbacks
 * @param bufferSize Size of chunk buffer to allocate
 *
 * @return S3StatusOK on success, error code otherwise
 */
static S3Status chunked_state_initialize(ChunkedRequestState *state,
                                        S3ChunkedDataCallback chunkedCallback,
                                        S3TrailingHeadersCallback trailingCallback,
                                        void *callbackData,
                                        int bufferSize)
{
    if (!state || !chunkedCallback) {
        return S3StatusInvalidChunkCallback;
    }

    memset(state, 0, sizeof(ChunkedRequestState));

    state->chunkedDataCallback = chunkedCallback;
    state->trailingHeadersCallback = trailingCallback;
    state->callbackData = callbackData;
    state->chunkBufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_CHUNK_BUFFER_SIZE;

    state->chunkBuffer = (char *)malloc(state->chunkBufferSize);
    if (!state->chunkBuffer) {
        return S3StatusOutOfMemory;
    }

    state->chunkBufferUsed = 0;
    state->chunkBufferOffset = 0;
    state->phase = CHUNK_STATE_HEADER;  /* Start with chunk header */
    state->chunkHeaderLen = 0;
    state->chunkHeaderOffset = 0;
    state->trailerBufferLen = 0;
    state->trailerBufferOffset = 0;
    state->allChunksSent = 0;
    state->trailingHeadersGenerated = 0;
    state->errorOccurred = 0;
    state->trailingHeadersCount = 0;
    state->totalBytesSent = 0;

    return S3StatusOK;
}

/**
 * Cleanup chunked request state
 *
 * @param state Pointer to state structure to cleanup
 */
static void chunked_state_cleanup(ChunkedRequestState *state)
{
    if (!state) {
        return;
    }

    if (state->chunkBuffer) {
        free(state->chunkBuffer);
        state->chunkBuffer = NULL;
    }

    memset(state, 0, sizeof(ChunkedRequestState));
}

/**
 * Read callback for libcurl - provides MANUALLY formatted AWS chunked data
 *
 * This callback manually formats data in AWS chunked encoding format:
 * <size-hex>\r\n<data>\r\n...<size-hex>\r\n<data>\r\n0\r\n<trailers>\r\n\r\n
 *
 * @param ptr Buffer to write data into
 * @param size Size of each element
 * @param nmemb Number of elements
 * @param userdata Pointer to ChunkedRequestState
 *
 * @return Number of bytes written, 0 for EOF, CURL_READFUNC_ABORT for error
 */
static size_t chunked_read_callback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    ChunkedRequestState *state = (ChunkedRequestState *)userdata;
    size_t maxBytes = size * nmemb;
    size_t totalWritten = 0;
    char *output = (char *)ptr;

    if (!state || state->errorOccurred) {
        return CURL_READFUNC_ABORT;
    }

    while (totalWritten < maxBytes && state->phase != CHUNK_STATE_DONE) {
        switch (state->phase) {
            case CHUNK_STATE_HEADER: {
                /* Need to read data and format chunk header */
                if (state->chunkBufferOffset >= state->chunkBufferUsed) {
                    /* Buffer empty, read more data */
                    int bytesRead = state->chunkedDataCallback(
                        state->chunkBufferSize,
                        state->chunkBuffer,
                        state->callbackData
                    );

                    if (bytesRead < 0) {
                        state->errorOccurred = 1;
                        return CURL_READFUNC_ABORT;
                    }

                    if (bytesRead == 0) {
                        /* EOF - move to final chunk */
                        state->phase = CHUNK_STATE_FINAL;
                        continue;
                    }

                    state->chunkBufferUsed = bytesRead;
                    state->chunkBufferOffset = 0;
                }

                /* Format chunk header: "<size-hex>\r\n" */
                int dataSize = state->chunkBufferUsed - state->chunkBufferOffset;
                state->chunkHeaderLen = snprintf(state->chunkHeader, sizeof(state->chunkHeader),
                                                 "%x\r\n", dataSize);
                state->chunkHeaderOffset = 0;
                state->phase = CHUNK_STATE_DATA;
                break;
            }

            case CHUNK_STATE_DATA: {
                /* First send the chunk header if not done yet */
                if (state->chunkHeaderOffset < state->chunkHeaderLen) {
                    size_t headerRemaining = state->chunkHeaderLen - state->chunkHeaderOffset;
                    size_t toCopy = (maxBytes - totalWritten < headerRemaining) ?
                                    maxBytes - totalWritten : headerRemaining;
                    memcpy(output + totalWritten,
                           state->chunkHeader + state->chunkHeaderOffset, toCopy);
                    state->chunkHeaderOffset += toCopy;
                    totalWritten += toCopy;
                    if (state->chunkHeaderOffset < state->chunkHeaderLen) {
                        break; /* Need more space, return what we have */
                    }
                }

                /* Send chunk data */
                size_t dataRemaining = state->chunkBufferUsed - state->chunkBufferOffset;
                size_t toCopy = (maxBytes - totalWritten < dataRemaining) ?
                                maxBytes - totalWritten : dataRemaining;

                if (toCopy > 0) {
                    memcpy(output + totalWritten,
                           state->chunkBuffer + state->chunkBufferOffset, toCopy);
                    state->chunkBufferOffset += toCopy;
                    state->totalBytesSent += toCopy;
                    totalWritten += toCopy;
                }

                if (state->chunkBufferOffset >= state->chunkBufferUsed) {
                    /* Done with this chunk data, send trailing \r\n */
                    state->phase = CHUNK_STATE_DATA_END;
                }
                break;
            }

            case CHUNK_STATE_DATA_END: {
                /* Send "\r\n" after chunk data */
                const char *end = "\r\n";
                size_t endLen = 2;
                size_t toCopy = (maxBytes - totalWritten < endLen) ? maxBytes - totalWritten : endLen;
                memcpy(output + totalWritten, end, toCopy);
                totalWritten += toCopy;

                /* Move back to header for next chunk */
                state->phase = CHUNK_STATE_HEADER;
                state->chunkBufferOffset = 0;
                state->chunkBufferUsed = 0;
                break;
            }

            case CHUNK_STATE_FINAL: {
                /* Send final "0\r\n" chunk */
                const char *final = "0\r\n";
                size_t finalLen = 3;
                size_t toCopy = (maxBytes - totalWritten < finalLen) ? maxBytes - totalWritten : finalLen;
                memcpy(output + totalWritten, final, toCopy);
                totalWritten += toCopy;

                /* Generate trailing headers */
                if (state->trailingHeadersCallback && !state->trailingHeadersGenerated) {
                    state->trailingHeadersCount = state->trailingHeadersCallback(
                        S3_MAX_METADATA_COUNT,
                        state->trailingHeaders,
                        state->callbackData
                    );

                    if (state->trailingHeadersCount < 0) {
                        state->errorOccurred = 1;
                        return CURL_READFUNC_ABORT;
                    }

                    /* Format trailing headers into buffer (for signature calculation) */
                    char trailersForSigning[512];
                    int signingLen = 0;

                    state->trailerBufferLen = 0;
                    for (int i = 0; i < state->trailingHeadersCount; i++) {
                        /* Format for buffer (NO space after colon - AWS format) */
                        int written = snprintf(state->trailerBuffer + state->trailerBufferLen,
                                             sizeof(state->trailerBuffer) - state->trailerBufferLen,
                                             "%s:%s\r\n",
                                             state->trailingHeaders[i].name,
                                             state->trailingHeaders[i].value);
                        if (written > 0) {
                            state->trailerBufferLen += written;
                        }

                        /* Format for signing (same format, just \n instead of \r\n) */
                        int sigWritten = snprintf(trailersForSigning + signingLen,
                                                 sizeof(trailersForSigning) - signingLen,
                                                 "%s:%s\n",
                                                 state->trailingHeaders[i].name,
                                                 state->trailingHeaders[i].value);
                        if (sigWritten > 0) {
                            signingLen += sigWritten;
                        }
                    }

                    /* FOR UNSIGNED PAYLOAD TRAILERS: Don't send x-amz-trailer-signature */
                    /* AWS expects trailer signature only for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER */
                    /* For STREAMING-UNSIGNED-PAYLOAD-TRAILER, only send the declared checksum trailer */

                    /* Add final \r\n */
                    if (state->trailerBufferLen + 2 < (int)sizeof(state->trailerBuffer)) {
                        state->trailerBuffer[state->trailerBufferLen++] = '\r';
                        state->trailerBuffer[state->trailerBufferLen++] = '\n';
                    }
                    state->trailerBufferOffset = 0;
                    state->trailingHeadersGenerated = 1;
                }

                state->phase = CHUNK_STATE_TRAILERS;
                break;
            }

            case CHUNK_STATE_TRAILERS: {
                /* Send trailing headers */
                if (state->trailerBufferOffset < state->trailerBufferLen) {
                    size_t remaining = state->trailerBufferLen - state->trailerBufferOffset;
                    size_t toCopy = (maxBytes - totalWritten < remaining) ?
                                    maxBytes - totalWritten : remaining;
                    memcpy(output + totalWritten,
                           state->trailerBuffer + state->trailerBufferOffset, toCopy);
                    state->trailerBufferOffset += toCopy;
                    totalWritten += toCopy;
                } else {
                    /* All done */
                    state->phase = CHUNK_STATE_DONE;
                }
                break;
            }

            case CHUNK_STATE_DONE:
                return totalWritten > 0 ? totalWritten : 0;
        }
    }

    return totalWritten;
}

#if LIBCURL_VERSION_NUM >= 0x074000  /* curl 7.64.0+ */

/**
 * Trailing headers callback for libcurl
 *
 * This callback is invoked by libcurl to retrieve trailing headers after
 * all body content has been sent. It appends headers to the curl_slist.
 *
 * @param list Pointer to curl_slist pointer to append headers to
 * @param userdata Pointer to ChunkedRequestState
 *
 * @return CURL_TRAILERFUNC_OK (0) on success, CURL_TRAILERFUNC_ABORT (1) on error
 */
static int trailing_headers_callback(struct curl_slist **list, void *userdata)
{
    ChunkedRequestState *state = (ChunkedRequestState *)userdata;

    if (!state || !list) {
        return 1; /* CURL_TRAILERFUNC_ABORT */
    }

    if (!state->trailingHeadersGenerated || state->trailingHeadersCount <= 0) {
        return 0; /* CURL_TRAILERFUNC_OK - no headers */
    }

    /* Append each trailing header to the list */
    for (int i = 0; i < state->trailingHeadersCount; i++) {
        const char *name = state->trailingHeaders[i].name;
        const char *value = state->trailingHeaders[i].value;

        if (!name || !value) {
            continue; /* Skip invalid headers */
        }

        /* Format header as "Name: Value" */
        char header[512];
        int written = snprintf(header, sizeof(header), "%s: %s", name, value);

        if (written < 0 || (size_t)written >= sizeof(header)) {
            return 1; /* CURL_TRAILERFUNC_ABORT */
        }

        /* Append to curl's list */
        struct curl_slist *new_list = curl_slist_append(*list, header);
        if (!new_list) {
            return 1; /* CURL_TRAILERFUNC_ABORT */
        }
        *list = new_list;
    }

    return 0; /* CURL_TRAILERFUNC_OK */
}

/**
 * Setup curl handle for trailing headers (DISABLED - using manual formatting)
 *
 * NOTE: We manually format trailing headers in the data stream now,
 * so we DON'T use CURLOPT_TRAILERFUNCTION anymore.
 *
 * @param curl CURL handle
 * @param state Chunked request state
 *
 * @return S3StatusOK on success, error code otherwise
 */
static S3Status setup_trailing_headers(CURL *curl, ChunkedRequestState *state)
{
    (void)curl;
    (void)state;
    /* DISABLED: We manually format trailers in the chunked_read_callback now */
    return S3StatusOK;
}

#else /* curl < 7.64.0 */

static S3Status setup_trailing_headers(CURL *curl, ChunkedRequestState *state)
{
    if (state && state->trailingHeadersCallback) {
        /* Trailing headers not supported in this curl version */
        return S3StatusTrailingHeadersError;
    }
    return S3StatusOK;
}

#endif /* LIBCURL_VERSION_NUM */

/**
 * Set signature information for trailer signing
 *
 * This must be called from request.c after computing the request signature
 * to enable x-amz-trailer-signature calculation.
 *
 * @param state Chunked request state
 * @param seedSignature Hex signature from Authorization header
 * @param timestamp Request timestamp (ISO8601 format)
 * @param credentialScope Credential scope (date/region/service/aws4_request)
 * @param signingKey AWS4 signing key (32 bytes)
 *
 * @return S3StatusOK on success, error otherwise
 */
S3Status chunked_set_signature_info(ChunkedRequestState *state,
                                   const char *seedSignature,
                                   const char *timestamp,
                                   const char *credentialScope,
                                   const unsigned char *signingKey)
{
    if (!state || !seedSignature || !timestamp || !credentialScope || !signingKey) {
        return S3StatusInternalError;
    }

    /* Copy signature info */
    strncpy(state->seedSignature, seedSignature, sizeof(state->seedSignature) - 1);
    state->seedSignature[sizeof(state->seedSignature) - 1] = '\0';

    strncpy(state->requestDateISO8601, timestamp, sizeof(state->requestDateISO8601) - 1);
    state->requestDateISO8601[sizeof(state->requestDateISO8601) - 1] = '\0';

    strncpy(state->credentialScope, credentialScope, sizeof(state->credentialScope) - 1);
    state->credentialScope[sizeof(state->credentialScope) - 1] = '\0';

    memcpy(state->signingKey, signingKey, S3_SHA256_DIGEST_LENGTH);

    state->hasSignatureInfo = 1;

    return S3StatusOK;
}

/**
 * Setup curl handle for chunked transfer encoding
 *
 * Configures libcurl for chunked encoding and sets up the necessary callbacks.
 *
 * @param curl CURL handle
 * @param state Chunked request state
 *
 * @return S3StatusOK on success, error code otherwise
 */
S3Status request_setup_chunked_encoding(CURL *curl, ChunkedRequestState *state)
{
    CURLcode code;

    if (!curl || !state) {
        return S3StatusInternalError;
    }

    /* Enable UPLOAD mode to use PUT method with read callback
     * Our read callback will provide MANUALLY formatted AWS chunks
     * (not curl's automatic chunking)
     */
    code = curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    if (code != CURLE_OK) {
        return S3StatusFailedToInitializeRequest;
    }

    /* Set read callback for providing MANUALLY formatted chunked data */
    code = curl_easy_setopt(curl, CURLOPT_READFUNCTION, chunked_read_callback);
    if (code != CURLE_OK) {
        return S3StatusFailedToInitializeRequest;
    }

    code = curl_easy_setopt(curl, CURLOPT_READDATA, state);
    if (code != CURLE_OK) {
        return S3StatusFailedToInitializeRequest;
    }

    /* DON'T set CURLOPT_INFILESIZE_LARGE - we'll send manually chunked data
     * The Transfer-Encoding: chunked header is already set in request.c
     * Our callback formats data as: <size-hex>\r\n<data>\r\n...0\r\n<trailers>\r\n\r\n
     */

    /* Setup trailing headers (currently disabled for manual format) */
    S3Status status = setup_trailing_headers(curl, state);
    if (status != S3StatusOK) {
        return status;
    }

    return S3StatusOK;
}

/**
 * Create and initialize a chunked request state
 *
 * Public API function to create a chunked request state structure.
 *
 * @param statePtr Pointer to receive allocated state structure
 * @param chunkedCallback Callback for retrieving chunk data
 * @param trailingCallback Optional callback for trailing headers (may be NULL)
 * @param callbackData User data passed to callbacks
 *
 * @return S3StatusOK on success, error code otherwise
 */
S3Status S3_create_chunked_request_state(ChunkedRequestState **statePtr,
                                        S3ChunkedDataCallback chunkedCallback,
                                        S3TrailingHeadersCallback trailingCallback,
                                        void *callbackData)
{
    ChunkedRequestState *state;
    S3Status status;

    if (!statePtr || !chunkedCallback) {
        return S3StatusInvalidChunkCallback;
    }

    state = (ChunkedRequestState *)malloc(sizeof(ChunkedRequestState));
    if (!state) {
        return S3StatusOutOfMemory;
    }

    status = chunked_state_initialize(state, chunkedCallback, trailingCallback,
                                     callbackData, DEFAULT_CHUNK_BUFFER_SIZE);

    if (status != S3StatusOK) {
        free(state);
        return status;
    }

    *statePtr = state;
    return S3StatusOK;
}

/**
 * Destroy a chunked request state
 *
 * Public API function to cleanup and free a chunked request state structure.
 *
 * @param state State structure to destroy
 */
void S3_destroy_chunked_request_state(ChunkedRequestState *state)
{
    if (!state) {
        return;
    }

    chunked_state_cleanup(state);
    free(state);
}

/**
 * Get total bytes sent in chunked request
 *
 * @param state Chunked request state
 *
 * @return Total bytes sent, or 0 if state is invalid
 */
uint64_t S3_get_chunked_bytes_sent(const ChunkedRequestState *state)
{
    return state ? state->totalBytesSent : 0;
}

/**
 * Check if chunked request encountered an error
 *
 * @param state Chunked request state
 *
 * @return 1 if error occurred, 0 otherwise
 */
int S3_chunked_request_has_error(const ChunkedRequestState *state)
{
    return state ? state->errorOccurred : 1;
}
