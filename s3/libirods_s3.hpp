/* -*- mode: c++; fill-column: 132; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef _LIBEIRODS_S3_H_
#define _LIBEIRODS_S3_H_

#include <rodsType.hpp>
#include <rodsDef.h>
#include <libs3.h>

#define S3_AUTH_FILE "s3Auth"

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
    FILE *fd;
    rodsLong_t contentLength, originalContentLength;
    int isTruncated;
    char nextMarker[1024];
    int keyCount;
    int allDetails;
    s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */
    int status;
} callback_data_t;

typedef struct put_object_callback_data
{
    FILE *infile;
    uint64_t contentLength, originalContentLength;
} put_object_callback_data;

#endif // _LIBEIRODS_S3_H_
