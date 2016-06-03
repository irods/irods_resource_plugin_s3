/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef _LIBIRODS_S3_HPP_
#define _LIBIRODS_S3_HPP_

#include <rodsType.h>
#include <rodsDef.h>
#include <libs3.h>

#define S3_AUTH_FILE "s3Auth"
#define ARCHIVE_NAMING_POLICY_KW    "ARCHIVE_NAMING_POLICY"
#define CONSISTENT_NAMING           "consistent"
#define DECOUPLED_NAMING            "decoupled"

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


#endif // _LIBIRODS_S3_HPP_
