
// =-=-=-=-=-=-=-
// local includes
#include "s3_archive_operations.hpp"
#include "libirods_s3.hpp"
#include "s3fs/curl.h"
#include "s3fs/cache.h"
#include "s3fs/fdcache.h"
#include "s3fs/s3fs.h"
#include "s3fs/s3fs_util.h"
#include "s3fs/s3fs_auth.h"


// =-=-=-=-=-=-=-
// irods includes
#include <msParam.h>
#include <rcConnect.h>
#include <rodsLog.h>
#include <rodsErrorTable.h>
#include <objInfo.h>
#include <rsRegReplica.hpp>
#include <dataObjOpr.hpp>
#include <irods_string_tokenize.hpp>
#include <irods_resource_plugin.hpp>
#include <irods_resource_redirect.hpp>
#include <irods_stacktrace.hpp>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

// =-=-=-=-=-=-=-
// other includes
#include <string>
#include <fcntl.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>


extern size_t g_retry_count;
extern size_t g_retry_wait;

extern S3ResponseProperties savedProperties;

namespace irods_s3_cacheless {

    irods::error set_s3_configuration_from_context(irods::plugin_property_map& _prop_map) {

        // this is taken from s3fs.cpp - main() with adjustments
        
        irods::error ret = s3Init( _prop_map );
        if (!ret.ok()) {
            return PASS(ret);
        }
        
        // get keys
        std::string key_id, access_key;
        ret = _prop_map.get< std::string >(s3_key_id, key_id);
        if (!ret.ok()) {
            S3fsCurl::DestroyS3fsCurl();
            s3fs_destroy_global_ssl();
            return ret;
        }
    
        ret = _prop_map.get< std::string >(s3_access_key, access_key);
        if (!ret.ok()) {
            S3fsCurl::DestroyS3fsCurl();
            s3fs_destroy_global_ssl();
            return ret;
        }
    
        // save keys
        if(!S3fsCurl::SetAccessKey(key_id.c_str(), access_key.c_str())){
            S3fsCurl::DestroyS3fsCurl();
            s3fs_destroy_global_ssl();
            std::string error_str =  "failed to set internal data for access key/secret key.";
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
        S3fsCurl::InitUserAgent();
    
        ret = _prop_map.get< std::string >(s3_proto, s3_protocol_str);
        if (!ret.ok()) {
            S3fsCurl::DestroyS3fsCurl();
            s3fs_destroy_global_ssl();
            std::string error_str =  "S3_PROTO is not defined for resource.";
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
    
        if (boost::iequals(s3_protocol_str, "https")) {
            s3_protocol_str = "https";
        } else if (boost::iequals(s3_protocol_str, "http")) {
            s3_protocol_str = "http";
        } else {
            s3_protocol_str = "";
        }
    
        S3SignatureVersion signature_version = s3GetSignatureVersion(_prop_map);
    
        if (signature_version == S3SignatureV4) {
            S3fsCurl::SetSignatureV4(true);
        } else {
            S3fsCurl::SetSignatureV4(false);
        }
    
        // set multipart size
        //    Note:  SetMultipartSize takes value in MB so need to convert back from bytes to MB.
        S3fsCurl::SetMultipartSize(s3GetMPUChunksize(_prop_map) / (1024ULL * 1024ULL));
    
        // set number of simultaneous threads
        S3fsCurl::SetMaxParallelCount(s3GetMPUThreads(_prop_map));
    
        service_path = "";
        host = std::string(s3GetHostname());
    
        return SUCCESS();
    }

    int create_file_object(std::string& path) 
    {

        headers_t meta;
        meta["Content-Type"]     = S3fsCurl::LookupMimeType(path);
        //meta["x-amz-meta-uid"]   = "999";
        //meta["x-amz-meta-gid"]   = "999";
        //meta["x-amz-meta-mode"]  = "33204";
        //meta["x-amz-meta-mtime"] = std::string(time(NULL));

        S3fsCurl s3fscurl(true);
        return s3fscurl.PutRequest(path.c_str(), meta, -1);    // fd=-1 means for creating zero byte object.
    }

    void flush_buffer(std::string& path, int fh) {
        FdEntity* ent;
        if (NULL != (ent = FdManager::get()->ExistOpen(path.c_str(), fh))) {
            //ent->UpdateMtime();
            ent->Flush(false);
            //FdManager::get()->Close(ent);
        }
        S3FS_MALLOCTRIM(0);
        return;
    }

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3RegisteredPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3UnregisteredPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3ModifiedPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create

    irods::error s3FileCreatePlugin( irods::plugin_context& _ctx) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        service_path = "";

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        int result;

        // TODO this doesn't really do anything
        std::string bucket;
        std::string key;
        parseS3Path(fco->physical_path(), bucket, key);

        result = create_file_object(path);
        StatCache::getStatCacheData()->DelStat(path.c_str());
        if(result != 0){
          return ERROR(S3_PUT_ERROR, (boost::format("Error in %s.  Code is %d") % __FUNCTION__ % result).str());
        }


        FdEntity*   ent;
        headers_t   meta;
        get_object_attribute(path.c_str(), NULL, &meta, true, NULL, true);    // no truncate cache
        if(NULL == (ent = FdManager::get()->Open(path.c_str(), &meta, 0, -1, false, true))){
          StatCache::getStatCacheData()->DelStat(path.c_str());
          return ERROR(S3_PUT_ERROR, (boost::format("Error in %s.  Code is EIO") % __FUNCTION__));
        }

        // create an iRODS file descriptor
        int irods_fd = FileOffsetManager::get()->create_entry(ent->GetFd());

        // save iRODS file descriptor 
        fco->file_descriptor(irods_fd);

        S3FS_MALLOCTRIM(0);

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3FileOpenPlugin( irods::plugin_context& _ctx) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        bool needs_flush = false;

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        // clear stat for reading fresh stat.
        // (if object stat is changed, we refresh it. then s3fs gets always
        // stat when s3fs open the object).
        StatCache::getStatCacheData()->DelStat(path.c_str());

        int flags = fco->flags();

        // get file size 
        struct stat st;

        headers_t meta;
        int returnVal = //get_object_attribute(path.c_str(), &st, &meta);
                    get_object_attribute(path.c_str(), &st, &meta, true, NULL, true);    // no truncate cache
        if (0 != returnVal) {
            StatCache::getStatCacheData()->DelStat(path.c_str());
            return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Failed to perform a stat of %s") % __FUNCTION__ % path.c_str()));
        }

        if((unsigned int)flags & O_TRUNC){
            if(0 != st.st_size){
                st.st_size = 0; 
                needs_flush = true;
            }    
        }

        FdEntity*   ent;
        //headers_t   meta;
        //get_object_attribute(path.c_str(), NULL, &meta, true, NULL, true);    // no truncate cache
        if(NULL == (ent = FdManager::get()->Open(path.c_str(), &meta, st.st_size, -1, false, true))){
          StatCache::getStatCacheData()->DelStat(path.c_str());

          // TODO create S3_OPEN_ERROR
          return ERROR(S3_FILE_STAT_ERR, (boost::format("%s:  Error opening %s.") % __FUNCTION__ % path.c_str()));
        }

        if (needs_flush){
            if(0 != (returnVal = ent->RowFlush(path.c_str(), true))){
                S3FS_PRN_ERR("could not upload file(%s): result=%d", path.c_str(), returnVal);

                FdManager::get()->Close(ent);
                StatCache::getStatCacheData()->DelStat(path.c_str());

                // TODO create S3_OPEN_ERROR
                return ERROR(S3_FILE_STAT_ERR, (boost::format("%s:  Error opening %s.") % __FUNCTION__ % path.c_str()));
            }
        }

        // create an iRODS file descriptor
        int irods_fd = FileOffsetManager::get()->create_entry(ent->GetFd());

        // save iRODS file descriptor 
        fco->file_descriptor(irods_fd);

        S3FS_MALLOCTRIM(0);

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3FileReadPlugin( irods::plugin_context& _ctx,
                                   void*               _buf,
                                   int                 _len ) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not look up file descriptor [irods_fd=%d]") % __FUNCTION__ % irods_fd));
        }

        ssize_t readReturnVal;

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(path.c_str(), fd))) {
          S3FS_PRN_ERR("could not find opened fd(%d) for %s", fd, path.c_str());
          return ERROR(S3_GET_ERROR, (boost::format("%s: Could not find opened fd(%s) for %s") % __FUNCTION__ % fd % path.c_str()));
        }
        if(ent->GetFd() != fd){
          S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), (unsigned long long)(fd));
        }

        // read the offset from the cache
        off_t offset = 0;
        if (!(FileOffsetManager::get()->getOffset(irods_fd, offset))) {
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not read offset for read (%llu)") % __FUNCTION__ % offset));
        }
        S3FS_PRN_DBG("[path=%s][size=%zu][offset=%jd][fd=%llu]", path.c_str(), _len, (intmax_t)offset, (unsigned long long)(fd));
      
        // check real file size
        size_t realsize;
        if(!ent->GetSize(realsize) || realsize <= 0){
          S3FS_PRN_DBG("file size is 0, so break to read.");
          FdManager::get()->Close(ent);
          result.code(0);
          return result;
        }

        // read the file size into st.st_size to mimic posix read semantics
        // TODO check performance of this.
        struct stat st;
        headers_t meta;
        int returnVal = //get_object_attribute(path.c_str(), &st, &meta, true, NULL, true);    // no truncate cache
                        get_object_attribute(path.c_str(), &st, &meta);
        if (0 != returnVal) {
            return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Failed to perform a stat of %s") % __FUNCTION__ % path.c_str()));
        }

        if (offset >= st.st_size) {
            result.code(0);
            return result;
        }

     
        readReturnVal = ent->Read(static_cast<char*>(_buf), offset, _len, true);
        if(0 > readReturnVal){
          S3FS_PRN_WARN("failed to read file(%s). result=%jd", path.c_str(), (intmax_t)readReturnVal);
          return ERROR(S3_GET_ERROR, (boost::format("%s: failed to read file(%s)") % __FUNCTION__ % path.c_str()));
        }
        //FdManager::get()->Close(ent);
       
        // ent->Read returns the size of the buffer but posix requires
        // we return the actual amount read 
        int bytes_read = 0;
        if (offset + readReturnVal > st.st_size) {
            bytes_read = st.st_size - offset; 
        } else { 
            bytes_read = readReturnVal;
        }

        FileOffsetManager::get()->adjustOffset(irods_fd, bytes_read);

        off_t offset2;
        FileOffsetManager::getOffset(fd, offset2); 
        result.code(bytes_read);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3FileWritePlugin( irods::plugin_context& _ctx,
                                    void*               _buf,
                                    int                 _len ) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not look up file descriptor") % __FUNCTION__));
        }

        ssize_t retVal;

        S3FS_PRN_DBG("[path=%s][size=%zu][fd=%llu]", path.c_str(), _len, (unsigned long long)(fd));

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(path.c_str(), static_cast<int>(fd)))){
            S3FS_PRN_ERR("could not find opened fd(%s)", path.c_str());
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not find opened fd(%s)") % __FUNCTION__ % fd));
        }
        if(ent->GetFd() != fd) {
            S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), (unsigned long long)(fd));
        }

        // read the offset from the cache
        off_t offset = 0;
        if (!(FileOffsetManager::get()->getOffset(irods_fd, offset))) {
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not read offset for write (%llu)") % __FUNCTION__ % offset));
        }
        S3FS_PRN_DBG("[offset=%llu]", offset);

        if(0 > (retVal = ent->Write(static_cast<const char*>(_buf), offset, _len))){
            S3FS_PRN_WARN("failed to write file(%s). result=%jd", path.c_str(), (intmax_t)retVal);
        }
        // TODO just uncommented this
        //FdManager::get()->Close(ent);

        // irods has no flush operation so have to manually flush at the end of the write
        //flush_buffer(path, ent->GetFd());

        FileOffsetManager::get()->adjustOffset(irods_fd, _len);

        result.code(retVal);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3FileClosePlugin(  irods::plugin_context& _ctx ) {

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        // remove entry from FileOffsetManager
        int irods_fd = fco->file_descriptor(); 

        FileOffsetManager::get()->delete_entry(irods_fd);


        FdEntity*   ent;
 
        // we are finished with only close if only one is open 
        if(NULL != (ent = FdManager::get()->ExistOpen(path.c_str())) && !FileOffsetManager::get()->fd_exists(ent->GetFd())){
            FdManager::get()->Close(ent);

            // iRODS does not have a flush operation so manually flush here
            flush_buffer(path, ent->GetFd());
        }
        S3FS_MALLOCTRIM(0);
        result.code(0);
        return result;
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3FileUnlinkPlugin( 
        irods::plugin_context& _ctx) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        int result;

        S3fsCurl s3fscurl;
        result = s3fscurl.DeleteRequest(path.c_str());
        FdManager::DeleteCacheFile(path.c_str());
        StatCache::getStatCacheData()->DelStat(path.c_str());
        S3FS_MALLOCTRIM(0);

        if (result < 0) {
          return ERROR(S3_FILE_UNLINK_ERR, (boost::format("%s: - Could not unlink file %s") % __FUNCTION__ % path.c_str()));
        }
        return SUCCESS();

    } // s3FileUnlinkPlugin 

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3FileStatPlugin(
        irods::plugin_context& _ctx,
        struct stat* _statbuf )
    {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        int returnVal;
        returnVal = get_object_attribute(path.c_str(), _statbuf);
        if (0 != returnVal) {
            return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Failed to perform a stat of %s") % __FUNCTION__ % path.c_str()));
        }
      
        // If has already opened fd, the st_size should be instead.
        // (See: Issue 241)
        if(_statbuf){
          FdEntity*   ent;
      
          if(NULL != (ent = FdManager::get()->ExistOpen(path.c_str()))){
            struct stat tmpstbuf;
            if(ent->GetStats(tmpstbuf)){
              _statbuf->st_size = tmpstbuf.st_size;
            }
            //FdManager::get()->Close(ent);
          }
          _statbuf->st_blksize = 4096;
          _statbuf->st_blocks  = get_blocks(_statbuf->st_size);
        }
        S3FS_PRN_DBG("[path=%s] uid=%u, gid=%u, mode=%04o", path.c_str(), (unsigned int)(_statbuf->st_uid), (unsigned int)(_statbuf->st_gid), _statbuf->st_mode);
        S3FS_MALLOCTRIM(0);
      
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                     struct stat*        _statbuf ) {
        return SUCCESS();

    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3FileLseekPlugin(  irods::plugin_context& _ctx,
                                     long long            _offset,
                                     int                 _whence ) {


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        // TODO create S3_FILE_SEEK_ERR 
        
        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        // TODO not sure why I need to do this
        // clear stat for reading fresh stat.
        // (if object stat is changed, we refresh it. then s3fs gets always
        // stat when s3fs open the object).
        StatCache::getStatCacheData()->DelStat(path.c_str());

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, (boost::format("%s: - Could not look up file descriptor") % __FUNCTION__));
        }

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(path.c_str(), static_cast<int>(fd)))){
          S3FS_PRN_ERR("could not find opened fd(%s)", path.c_str());
          return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Could not find opened fd(%s)") % __FUNCTION__ % fd));
        }
        if(ent->GetFd() != fd) {
            S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), fd);
        }

        // update the position based on the _offset and _whence
        // note:  we have a valid fd so no need to check errors from GetOffset and AdjustOffset below
        switch (_whence) {
            case SEEK_SET:
                FileOffsetManager::get()->setOffset(irods_fd, _offset);
                //ent->SetOffset(_offset);
                break;
            case SEEK_CUR:
                FileOffsetManager::get()->adjustOffset(irods_fd, _offset);
                //ent->AdjustOffset(_offset);
                break;
            case SEEK_END:

                // need to do a stat to get the the file size to determine the end point
                { 
                    struct stat st;
                    headers_t meta;
                    int returnVal = get_object_attribute(path.c_str(), &st, &meta, true, NULL, true);    // no truncate cache
                                    //get_object_attribute(path.c_str(), &st, &meta);
                    if (0 != returnVal) {
                        return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Failed to perform a stat of %s") % __FUNCTION__ % path.c_str()));
                    }

                    FileOffsetManager::get()->setOffset(irods_fd, st.st_size + _offset);
                    //ent->SetOffset(st.st_size + _offset);
                    break;
                }
            default:
                S3FS_PRN_ERR("invalid whence argument (%d) on lseek for object (%s)", _whence, path.c_str());
                return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Setting seek failed (%lld)") % __FUNCTION__ % _offset));
        }

        // read the new offset and set in ret.code
        off_t new_offset;
        FileOffsetManager::get()->getOffset(irods_fd, new_offset);
        ret.code(new_offset);

        return ret;

    } // s3FileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileMkdirPlugin(  irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3FileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileRmdirPlugin(  irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3FileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3FileOpendirPlugin( irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3FileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3FileClosedirPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    } // s3FileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3FileReaddirPlugin( irods::plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr ) {
        return SUCCESS();
    } // s3FileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3FileRenamePlugin( irods::plugin_context& _ctx,
                                     const char*         _new_file_name )
    {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return ERROR(S3_INIT_ERROR, (boost::format("init cacheless mode returned error %s") % ret.result().c_str()));
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string from = fco->physical_path();

        // TODO S3_RENAME_ERR

        struct stat buf;
        int result;
      
        S3FS_PRN_DBG("[from=%s][to=%s]", from.c_str(), _new_file_name);

        ret = s3FileStatPlugin(_ctx, &buf);
        if(!ret.ok()) {
            return ERROR(S3_FILE_STAT_ERR, (boost::format("%s: - Failed to stat file (%s) during move to (%s)") % __FUNCTION__ % from.c_str(), _new_file_name));
        }

        // files larger than 5GB must be modified via the multipart interface
        if(!nomultipart && ((long long)buf.st_size >= (long long)FIVE_GB)) {
            result = rename_large_object(from.c_str(), _new_file_name);
        } else {
            if(!nocopyapi && !norenameapi){
                result = rename_object(from.c_str(), _new_file_name);
            } else {
                result = rename_object_nocopy(from.c_str(), _new_file_name);
            }
        }
        S3FS_MALLOCTRIM(0);
     
        if (result != 0) { 
            return ERROR(S3_FILE_COPY_ERR, (boost::format("%s: - Failed to rename file from (%s) to (%s) result = %d") % __FUNCTION__ % from.c_str() % _new_file_name % result));
        }


        return SUCCESS();

    } // s3FileRenamePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3FileTruncatePlugin


    // interface to determine free space on a device given a path
    irods::error s3FileGetFsFreeSpacePlugin(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3FileGetFsFreeSpacePlugin

    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3StageToCachePlugin(
        irods::plugin_context& _ctx,
        const char*                               _cache_file_name )
    {
        std::cerr << irods::stacktrace().dump();
        return ERROR(SYS_NOT_SUPPORTED, "s3StageToCachePlugin");
    }

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3SyncToArchPlugin(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
        std::cerr << irods::stacktrace().dump();
        return ERROR(SYS_NOT_SUPPORTED, "s3StageToCachePlugin");
    }

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    irods::error s3RedirectPlugin(
        irods::plugin_context& _ctx,
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
                    if( irods::OPEN_OPERATION == (*_opr) ||
                            irods::WRITE_OPERATION == (*_opr) ||
                            irods::UNLINK_OPERATION == (*_opr) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'get' operation
                        result = s3RedirectOpen(
                                     _ctx.comm(),
                                     _ctx.prop_map(),
                                     file_obj,
                                     resc_name,
                                     (*_curr_host),
                                     (*_out_vote));
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
        irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3FileRebalance

    irods::error s3FileNotifyPlugin( irods::plugin_context& _ctx, 
        const std::string* str ) {
        return SUCCESS();
    } // s3FileNotifyPlugin



}
