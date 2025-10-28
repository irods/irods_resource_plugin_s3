#ifndef IRODS_S3_RESOURCE_OPERATIONS_HPP
#define IRODS_S3_RESOURCE_OPERATIONS_HPP

// =-=-=-=-=-=-=-
// irods includes
#include <irods/msParam.h>
#include <irods/rcConnect.h>
#include <irods/rodsErrorTable.h>
#include <irods/objInfo.h>
#include <irods/rsRegReplica.hpp>
#include <irods/dataObjOpr.hpp>
#include <irods/irods_hierarchy_parser.hpp>

namespace irods_s3 {

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3_registered_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3_unregistered_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3_modified_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for POSIX create
    irods::error s3_file_create_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3_file_open_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3_file_read_operation( irods::plugin_context& _ctx, void* _buf, const int _len );

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3_file_write_operation( irods::plugin_context& _ctx,
                                    const void*         _buf,
                                    const int           _len );
    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3_file_close_operation(  irods::plugin_context& _ctx );

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3_file_unlink_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3_file_stat_operation( irods::plugin_context& _ctx, struct stat* _statbuf );

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx, struct stat* _statbuf );

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3_file_lseek_operation(  irods::plugin_context& _ctx, const long long _offset, const int _whence );

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3_file_mkdir_operation(  irods::plugin_context& _ctx );

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3_rmdir_operation(  irods::plugin_context& _ctx );

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3_opendir_operation( irods::plugin_context& _ctx );

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3_closedir_operation( irods::plugin_context& _ctx);

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3_readdir_operation( irods::plugin_context& _ctx, struct rodsDirent** _dirent_ptr );

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3_file_rename_operation( irods::plugin_context& _ctx, const char* _new_file_name );

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin( irods::plugin_context& _ctx );


    // interface to determine free space on a device given a path
    irods::error s3_get_fs_freespace_operation( irods::plugin_context& _ctx );

    irods::error s3FileCopyPlugin( int mode, const char *srcFileName, const char *destFileName);

    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3_stage_to_cache_operation( irods::plugin_context& _ctx, const char* _cache_file_name );

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3_sync_to_arch_operation( irods::plugin_context& _ctx, const char* _cache_file_name );

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    irods::error s3_resolve_resc_hier_operation( irods::plugin_context& _ctx,
        const std::string* _opr, const std::string* _curr_host,
        irods::hierarchy_parser* _out_parser, float* _out_vote );

    // =-=-=-=-=-=-=-
    // code which would rebalance the resource, S3 does not rebalance.
    irods::error s3_rebalance_operation( irods::plugin_context& _ctx );

    irods::error s3_notify_operation( irods::plugin_context& _ctx, const std::string* str );

    irods::error s3_read_checksum_from_storage_device(irods::plugin_context& _ctx,
            const std::string* checksum_scheme,
            std::string* returned_checksum);
}

#endif
