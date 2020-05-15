#ifndef S3_TRANSPORT_TYPES_HPP
#define S3_TRANSPORT_TYPES_HPP

#include "circular_buffer.hpp"
#include <libs3.h>

namespace irods::experimental::io::s3_transport
{

    struct libs3_types
    {
        using status = S3Status;
        const static status status_ok = status::S3StatusOK;
        using bucket_context = S3BucketContext;
        using char_type   = char;
        using buffer_type = char_type*;
        using error_details = S3ErrorDetails;
        using response_properties = S3ResponseProperties;
    };

    enum class error_codes
    {
        SUCCESS,
        OUT_OF_DISK_SPACE,
        BAD_ALLOC,
        BYTES_TRANSFERRED_MISMATCH,
        INITIATE_MULTIPART_UPLOAD_ERROR,
        COMPLETE_MULTIPART_UPLOAD_ERROR,
        UPLOAD_FILE_ERROR,
        DOWNLOAD_FILE_ERROR
    };

    enum class cache_file_download_status
    {
        NOT_STARTED,
        STARTED,
        SUCCESS,
        FAILED
    };

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_TYPES_HPP

