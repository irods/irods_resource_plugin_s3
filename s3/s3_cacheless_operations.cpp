
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
#include "s3fs/common.h"


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
#include <irods_collection_object.hpp>
#include <irods_stacktrace.hpp>
#include <irods_random.hpp>
#include <irods/irods_resource_backport.hpp>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/filesystem/path.hpp>

// =-=-=-=-=-=-=-
// other includes
#include <string>
#include <iomanip>
#include <fcntl.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>


extern size_t g_retry_count;
extern size_t g_retry_wait;

extern S3ResponseProperties savedProperties;

namespace irods_s3_cacheless {

    int get64RandomBytes( char *buf ) {
        const int num_random_bytes = 32;
        const int num_hex_bytes = 2 * num_random_bytes;
        unsigned char random_bytes[num_random_bytes];
        irods::getRandomBytes( random_bytes, sizeof(random_bytes) );

        std::stringstream ss;
        for ( size_t i = 0; i < sizeof(random_bytes); ++i ) {
            ss << std::hex << std::setw(2) << std::setfill('0') << (unsigned int)( random_bytes[i] );
        }

        snprintf( buf, num_hex_bytes + 1, "%s", ss.str().c_str() );
        return 0;
    }   

    irods::error set_s3_configuration_from_context(irods::plugin_property_map& _prop_map) {

        static bool already_destroyed = false;

        // this is taken from s3fs.cpp - main() with adjustments
        
        irods::error ret = s3Init( _prop_map );
        if (!ret.ok()) {
            return PASS(ret);
        }
        
        // get keys
        std::string key_id, access_key;
        ret = _prop_map.get< std::string >(s3_key_id, key_id);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }
            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to read S3_ACCESS_KEY_ID.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
    
        ret = _prop_map.get< std::string >(s3_access_key, access_key);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }
            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to read S3_SECRET_ACCESS_KEY.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
    
        // save keys
        if(!S3fsCurl::SetAccessKey(key_id.c_str(), access_key.c_str())){
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }

            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to set internal data for access key/secret key.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
        S3fsCurl::InitUserAgent();
    
        ret = _prop_map.get< std::string >(s3_proto, s3_protocol_str);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }

            std::string error_str =  boost::str(boost::format("[resource_name=%s] S3_PROTO is not defined for resource.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }

        // if cachedir is defined, use that else use /tmp/<resc_name>
        std::string s3_cache_dir_str;
        ret = _prop_map.get< std::string >(s3_cache_dir, s3_cache_dir_str);
        if (!ret.ok()) {
            const auto& shared_memory_name_salt = irods::get_server_property<const std::string>(irods::CFG_RE_CACHE_SALT_KW);
            std::string resc_name  = "";
            ret = _prop_map.get< std::string >( irods::RESOURCE_NAME, resc_name);
            s3_cache_dir_str = "/tmp/" + resc_name + shared_memory_name_salt;
            _prop_map.set< std::string >(s3_cache_dir, s3_cache_dir_str);
        }
        FdManager::SetCacheDir(s3_cache_dir_str);
    
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
    
        nomultipart = !s3GetEnableMultiPartUpload(_prop_map);

        // set multipart size
        //    Note:  SetMultipartSize takes value in MB so need to convert back from bytes to MB.
        S3fsCurl::SetMultipartSize(s3GetMPUChunksize(_prop_map) / (1024ULL * 1024ULL));
    
        // set number of simultaneous threads
        S3fsCurl::SetMaxParallelCount(s3GetMPUThreads(_prop_map));

        // set the MD5 flag
        S3fsCurl::SetContentMd5(s3GetEnableMD5(_prop_map));
    
        //service_path = "";
        strncpy(host, s3GetHostname(_prop_map).c_str(), MAX_NAME_LEN-1);

        std::string endpoint_str;
        _prop_map.get< std::string >(s3_region_name, endpoint_str); // if this fails use default
        strncpy(endpoint, endpoint_str.c_str(), MAX_NAME_LEN-1);

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
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int result;

        result = create_file_object(key);
        StatCache::getStatCacheData()->DelStat(key.c_str());
        if(result != 0){
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Code is %d") 
                        % get_resource_name(_ctx.prop_map()).c_str() % result));
        }


        FdEntity*   ent;
        headers_t   meta;
        get_object_attribute(key.c_str(), NULL, &meta, true, NULL, true);    // no truncate cache
        if(NULL == (ent = FdManager::get()->Open(key.c_str(), &meta, 0, -1, false, true))){
            StatCache::getStatCacheData()->DelStat(key.c_str());
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] code is EIO") 
                        % get_resource_name(_ctx.prop_map()).c_str()));
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

        irods::error ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        // =-=-=-=-=-=-=-
        // check incoming parameters
        ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        bool needs_flush = false;

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        // clear stat for reading fresh stat.
        // (if object stat is changed, we refresh it. then s3fs gets always
        // stat when s3fs open the object).
        StatCache::getStatCacheData()->DelStat(key.c_str());

        int flags = fco->flags();

        // get file size 
        struct stat st;

        headers_t meta;
        int returnVal = get_object_attribute(key.c_str(), &st, &meta, true, NULL, true);    // no truncate cache
        if (0 != returnVal) {
            StatCache::getStatCacheData()->DelStat(key.c_str());
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to perform a stat of %s") 
                        % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }

        if((unsigned int)flags & O_TRUNC){
            if(0 != st.st_size){
                st.st_size = 0; 
                needs_flush = true;
            }    
        }

        FdEntity*   ent;
        if(NULL == (ent = FdManager::get()->Open(key.c_str(), &meta, st.st_size, -1, false, true))){
          StatCache::getStatCacheData()->DelStat(key.c_str());

          // TODO create S3_OPEN_ERROR
          return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Error opening %s.") 
                      % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }

        if (needs_flush){
            if(0 != (returnVal = ent->RowFlush(key.c_str(), true))){
                S3FS_PRN_ERR("could not upload file(%s): result=%d", key.c_str(), returnVal);

                FdManager::get()->Close(ent);
                StatCache::getStatCacheData()->DelStat(key.c_str());

                // TODO create S3_OPEN_ERROR
                return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Error opening %s.") 
                            % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
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
                                   const int           _len ) {


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not look up file descriptor [irods_fd=%d]") 
                        % get_resource_name(_ctx.prop_map()).c_str() % irods_fd));
        }

        ssize_t readReturnVal;

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(key.c_str(), fd))) {
          S3FS_PRN_ERR("could not find opened fd(%d) for %s", fd, key.c_str());
          return ERROR(S3_GET_ERROR, boost::str(boost::format("[resource_name=%s] Could not find opened fd(%d) for %s") 
                      % get_resource_name(_ctx.prop_map()).c_str() % fd % key.c_str()));
        }
        if(ent->GetFd() != fd){
          S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), (unsigned long long)(fd));
        }

        // read the offset from the cache
        off_t offset = 0;
        if (!(FileOffsetManager::get()->getOffset(irods_fd, offset))) {
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not read offset for read (%llu)") 
                        % get_resource_name(_ctx.prop_map()).c_str() % offset));
        }
        S3FS_PRN_DBG("[path=%s][size=%zu][offset=%jd][fd=%llu]", key.c_str(), _len, (intmax_t)offset, (unsigned long long)(fd));
      
        // check real file size
        size_t realsize;
        if(!ent->GetSize(realsize) || realsize == 0){
          S3FS_PRN_DBG("file size is 0, so break to read.");
          result.code(0);
          return result;
        }

        // read the file size into st.st_size to mimic posix read semantics
        struct stat st;
        headers_t meta;
        int returnVal = get_object_attribute(key.c_str(), &st, &meta);
        if (0 != returnVal) {
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to perform a stat of %s") 
                        % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }

        if (offset >= st.st_size) {
            result.code(0);
            return result;
        }

        // - First thread will just get the mutex below and return immediately
        //   and get the data from S3 when Read() is called.
        // - Second thread will do a full load of data into cache.
        // - Third and subsequent threads will wait for this load (due to mutex lock)
        //   and then read their part from cache.
        {
	        std::unique_lock<std::mutex> lck(ent->cv_mtx);

	        ++(ent->simultaneous_read_count);

            if (ent->simultaneous_read_count == 2) {

                // This thread is doing a full load. 
                ent->Load(0, realsize);
            }
        }

    
        // now this should just read from cache unless we are the first reader
        readReturnVal = ent->Read(static_cast<char*>(_buf), offset, _len, false);
        if(0 > readReturnVal){
          S3FS_PRN_WARN("failed to read file(%s). result=%jd", key.c_str(), (intmax_t)readReturnVal);
          return ERROR(S3_GET_ERROR, boost::str(boost::format("[resource_name=%s] failed to read file(%s)") 
                      % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }

        {
	        std::unique_lock<std::mutex> lck(ent->cv_mtx);
	        --(ent->simultaneous_read_count);
        }

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
                                    const void*            _buf,
                                    const int              _len ) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not look up file descriptor") 
                        % get_resource_name(_ctx.prop_map()).c_str()));
        }

        ssize_t retVal;

        S3FS_PRN_DBG("[path=%s][size=%zu][fd=%llu]", key.c_str(), _len, (unsigned long long)(fd));

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(key.c_str(), static_cast<int>(fd)))){
            S3FS_PRN_ERR("could not find opened fd(%s)", key.c_str());
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not find opened fd(%d)") 
                        % get_resource_name(_ctx.prop_map()).c_str() % fd));
        }
        if(ent->GetFd() != fd) {
            S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), (unsigned long long)(fd));
        }

        // read the offset from the cache
        off_t offset = 0;
        if (!(FileOffsetManager::get()->getOffset(irods_fd, offset))) {
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not read offset for write (%llu)") 
                        % get_resource_name(_ctx.prop_map()).c_str() % offset));
        }
        S3FS_PRN_DBG("[offset=%llu]", offset);

        if(0 > (retVal = ent->Write(static_cast<const char*>(_buf), offset, _len))){
            S3FS_PRN_WARN("failed to write file(%s). result=%jd", key.c_str(), (intmax_t)retVal);
        }

        FileOffsetManager::get()->adjustOffset(irods_fd, _len);

        result.code(retVal);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3FileClosePlugin(  irods::plugin_context& _ctx ) {

        irods::error result = SUCCESS();

        result = set_s3_configuration_from_context(_ctx.prop_map());
        if (!result.ok()) {
            return PASS(result);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;

        result = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!result.ok()) {
            return PASS(result);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        // remove entry from FileOffsetManager
        int irods_fd = fco->file_descriptor(); 

        FileOffsetManager::get()->delete_entry(irods_fd);


        FdEntity*   ent;
 
        // we are finished with only close if only one is open 
        if(NULL != (ent = FdManager::get()->ExistOpen(key.c_str())) && !FileOffsetManager::get()->fd_exists(ent->GetFd())) {
            flush_buffer(key, ent->GetFd());
            FdManager::get()->Close(ent);
            StatCache::getStatCacheData()->DelStat(key.c_str());
            FdManager::DeleteCacheFile(key.c_str());
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
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int result;

        S3fsCurl s3fscurl;
        result = s3fscurl.DeleteRequest(key.c_str());
        FdManager::DeleteCacheFile(key.c_str());
        StatCache::getStatCacheData()->DelStat(key.c_str());
        S3FS_MALLOCTRIM(0);

        if (result < 0) {
          return ERROR(S3_FILE_UNLINK_ERR, boost::str(boost::format("[resource_name=%s] Could not unlink file %s") 
                      % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
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
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int returnVal;
        returnVal = get_object_attribute(key.c_str(), _statbuf);
        if (0 != returnVal) {
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to perform a stat of %s") 
                        % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }
      
        // If has already opened fd, the st_size should be instead.
        // (See: Issue 241)
        if(_statbuf){
          FdEntity*   ent;
      
          if(NULL != (ent = FdManager::get()->ExistOpen(key.c_str()))){
            struct stat tmpstbuf;
            if(ent->GetStats(tmpstbuf)){
              _statbuf->st_size = tmpstbuf.st_size;
            }
          }
          _statbuf->st_blksize = 4096;
          _statbuf->st_blocks  = get_blocks(_statbuf->st_size);
          S3FS_PRN_DBG("[path=%s] uid=%u, gid=%u, mode=%04o", key.c_str(), (unsigned int)(_statbuf->st_uid), (unsigned int)(_statbuf->st_gid), _statbuf->st_mode);
        }
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
                                     const long long        _offset,
                                     const int              _whence ) {


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        // TODO create S3_FILE_SEEK_ERR 
        
        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        // clear stat for reading fresh stat.
        // (if object stat is changed, we refresh it. then s3fs gets always
        // stat when s3fs open the object).
        StatCache::getStatCacheData()->DelStat(key.c_str());

        int irods_fd = fco->file_descriptor(); 
        int fd;
        if (!(FileOffsetManager::get()->getFd(irods_fd, fd))) {
            return ERROR(S3_PUT_ERROR, boost::str(boost::format("[resource_name=%s] Could not look up file descriptor") 
                        % get_resource_name(_ctx.prop_map()).c_str()));
        }

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(key.c_str(), static_cast<int>(fd)))){
          S3FS_PRN_ERR("could not find opened fd(%s)", key.c_str());
          return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Could not find opened fd(%d)") 
                      % get_resource_name(_ctx.prop_map()).c_str() % fd));
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
                    int returnVal = get_object_attribute(key.c_str(), &st, &meta, true, NULL, true);    // no truncate cache
                    if (0 != returnVal) {
                        return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to perform a stat of %s") 
                                    % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
                    }

                    FileOffsetManager::get()->setOffset(irods_fd, st.st_size + _offset);
                    //ent->SetOffset(st.st_size + _offset);
                    break;
                }
            default:
                S3FS_PRN_ERR("invalid whence argument (%d) on lseek for object (%s)", _whence, key.c_str());
                return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Setting seek failed (%lld)") 
                            % get_resource_name(_ctx.prop_map()).c_str() % _offset));
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


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::collection_object_ptr fco = boost::dynamic_pointer_cast< irods::collection_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        S3ObjList head;
        int result;

        S3FS_PRN_INFO("[path=%s]", path.c_str());

        if (!(DirectoryListStreamManager::get()->key_exists(key))) {

            // Do not have entries cached.  Go ahead and retrieve them

            // get a list of all the objects
            if ((result = list_bucket(key.c_str(), head, "/")) != 0) {
              return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] list_bucket returns error(%d).") 
                          % get_resource_name(_ctx.prop_map()).c_str() % result));
            }

            if (head.IsEmpty()) {
                return SUCCESS();
            }

            // Send stats caching.
            std::string strpath = path;
            if (strcmp(path.c_str(), "/") != 0) {
                strpath += "/";
            }

            s3obj_list_t objects;
            head.GetNameList(objects);

            for (auto& object : objects) {
                DirectoryListStreamManager::get()->add_entry(key, object);
            }
        }


        std::string next_entry;
        if (DirectoryListStreamManager::get()->get_next_entry(key, next_entry)) {

           std::string object_key = key + "/" + next_entry;
           struct stat st;
           headers_t meta;
           if (0 != (result = get_object_attribute(object_key.c_str(), &st, &meta, true, NULL, true))) {
               return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] get_object_attribute on %s returns error(%d).") 
                           % get_resource_name(_ctx.prop_map()).c_str() % object_key.c_str() % result));
           }
           *_dirent_ptr = ( rodsDirent_t* ) malloc( sizeof( rodsDirent_t ) );
           boost::filesystem::path p(object_key.c_str());
           strcpy((*_dirent_ptr)->d_name, p.filename().string().c_str()); 

        }

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
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string from = fco->physical_path();

        std::string bucket;
        std::string from_key;
        ret = parseS3Path(from, bucket, from_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        from_key = "/" + from_key;

        std::string new_file_key;
        ret = parseS3Path(_new_file_name, bucket, new_file_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        new_file_key = "/" + new_file_key;

        // TODO S3_RENAME_ERR

        struct stat buf;
        int result;
      
        S3FS_PRN_DBG("[from=%s][to=%s]", from_key.c_str(), new_file_key.c_str());

        ret = s3FileStatPlugin(_ctx, &buf);
        if(!ret.ok()) {
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to stat file (%s) during move to (%s)") 
                        % get_resource_name(_ctx.prop_map()).c_str() % from.c_str() % _new_file_name));
        }

        // files larger than 5GB must be modified via the multipart interface
        if(!nomultipart && ((long long)buf.st_size >= (long long)FIVE_GB)) {
            result = rename_large_object(from_key.c_str(), new_file_key.c_str());
        } else {
            if(!nocopyapi && !norenameapi){
                result = rename_object(from_key.c_str(), new_file_key.c_str());
            } else {
                result = rename_object_nocopy(from_key.c_str(), new_file_key.c_str());
            }
        }
        S3FS_MALLOCTRIM(0);
     
        if (result != 0) { 
            return ERROR(S3_FILE_COPY_ERR, boost::str(boost::format("[resource_name=%s] Failed to rename file from (%s) to (%s) result = %d") 
                        % get_resource_name(_ctx.prop_map()).c_str() % from.c_str() % _new_file_name % result));
        }

        // issue 1855 (irods issue 4326) - resources must now set physical path
        fco->physical_path(_new_file_name);
 
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
        return ERROR(SYS_NOT_SUPPORTED, boost::str(boost::format("[resource_name=%s] %s") % get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3SyncToArchPlugin(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
        return ERROR(SYS_NOT_SUPPORTED, boost::str(boost::format("[resource_name=%s] %s") % get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // redirect_open - code to determine redirection for open operation
    irods::error s3RedirectOpen(
        irods::plugin_property_map&   _prop_map,
        irods::file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {

        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // initially set a good default
        _out_vote = 0.0;

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, boost::str(boost::format("[resource_name=%s] Failed to get \"status\" property.") % _resc_name.c_str() ) ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN != resc_status ) {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, boost::str(boost::format("[resource_name=%s] Failed to get \"location\" property.") % _resc_name.c_str() ) ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // set a flag to test if were at the curr host, if so we vote higher
                    bool curr_host = ( _curr_host == host_name );

                    // =-=-=-=-=-=-=-
                    // make some flags to clarify decision making
                    bool need_repl = ( _file_obj->repl_requested() > -1 );

                    // =-=-=-=-=-=-=-
                    // set up variables for iteration
                    irods::error final_ret = SUCCESS();
                    std::vector< irods::physical_object > objs = _file_obj->replicas();
                    std::vector< irods::physical_object >::iterator itr = objs.begin();

                    // =-=-=-=-=-=-=-
                    // check to see if the replica is in this resource, if one is requested
                    for ( ; itr != objs.end(); ++itr ) {
                        // =-=-=-=-=-=-=-
                        // run the hier string through the parser and get the last
                        // entry.
                        std::string last_resc;
                        irods::hierarchy_parser parser;
                        parser.set_string( itr->resc_hier() );
                        parser.last_resc( last_resc );

                        // =-=-=-=-=-=-=-
                        // more flags to simplify decision making
                        bool repl_us  = ( _file_obj->repl_requested() == itr->repl_num() );
                        bool resc_us  = ( _resc_name == last_resc );
                        bool is_dirty = ( itr->is_dirty() != 1 );

                        // =-=-=-=-=-=-=-
                        // success - correct resource and don't need a specific
                        //           replication, or the repl nums match
                        if ( resc_us ) {
                            // =-=-=-=-=-=-=-
                            // if a specific replica is requested then we
                            // ignore all other criteria
                            if ( need_repl ) {
                                if ( repl_us ) {
                                    _out_vote = 1.0;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // repl requested and we are not it, vote
                                    // very low
                                    _out_vote = 0.25;
                                }
                            }
                            else {
                                // =-=-=-=-=-=-=-
                                // if no repl is requested consider dirty flag
                                if ( is_dirty ) {
                                    // =-=-=-=-=-=-=-
                                    // repl is dirty, vote very low
                                    _out_vote = 0.25;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // if our repl is not dirty then a local copy
                                    // wins, otherwise vote middle of the road
                                    if ( curr_host ) {
                                        _out_vote = 1.0;
                                    }
                                    else {
                                        _out_vote = 0.5;
                                    }
                                }
                            }

                            rodsLog(
                                LOG_DEBUG,
                                "open :: resc name [%s] curr host [%s] resc host [%s] vote [%f]",
                                _resc_name.c_str(),
                                _curr_host.c_str(),
                                host_name.c_str(),
                                _out_vote );

                            break;

                        } // if resc_us

                    } // for itr
                }
            }
            else {
                result.code( SYS_RESC_IS_DOWN );
                std::stringstream msg;
                msg << "[resource_name=" << get_resource_name(_prop_map) << "] resource is down";
                return PASSMSG(msg.str(), result);
            }
        }

        return result;

    } // S3RedirectOpen 

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
        if ( ( result = ASSERT_PASS( ret, "[resource_name=%s] Invalid resource context.", get_resource_name(_ctx.prop_map()).c_str() ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // check incoming parameters
            if( ( result = ASSERT_ERROR( _opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM,
                                      "[resource_name=%s] One or more NULL pointer arguments.", get_resource_name(_ctx.prop_map()).c_str() ) ).ok() ) {

                std::string resc_name;

                // =-=-=-=-=-=-=-
                // cast down the chain to our understood object type
                irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

                // =-=-=-=-=-=-=-
                // get the name of this resource
                ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
                if((result = ASSERT_PASS(ret, "Failed to get resource name property.")).ok() ) {

                    // if we are in detached mode, set the location to current host
                    bool attached_mode, cacheless_mode;
                    get_modes_from_properties(_ctx.prop_map(), attached_mode, cacheless_mode);

                    if (!attached_mode && _curr_host) {

                        // set the hostname to the local host
                        _ctx.prop_map().set<std::string>(irods::RESOURCE_LOCATION, *_curr_host);

                        rodsServerHost_t* host = nullptr;
                        rodsLong_t resc_id = 0;

                        ret = _ctx.prop_map().get<rodsLong_t>( irods::RESOURCE_ID, resc_id );
                        if ( !ret.ok() ) { 
                            std::string msg = boost::str(boost::format("[resource_name=%s] get_property in s3RedirectPlugin failed to get irods::RESOURCE _ID") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }

                        ret = irods::get_resource_property< rodsServerHost_t* >( resc_id, irods::RESOURCE_HOST, host );
                        if ( !ret.ok() ) { 
                            std::string msg = boost::str(boost::format("[resource_name=%s] get_resource_property (irods::RESOURCE_HOST) in s3RedirectPlugin for detached mode failed") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }
                      
                        // pave over host->hostName->name in rodsServerHost_t 
                        free(host->hostName->name);
                        host->hostName->name = static_cast<char*>(malloc(strlen(_curr_host->c_str()) + 1));
                        strcpy(host->hostName->name, _curr_host->c_str()); 
                        host->localFlag = LOCAL_HOST;

                        ret = irods::set_resource_property< rodsServerHost_t* >( resc_name, irods::RESOURCE_HOST, host );
                        if ( !ret.ok() ) { 
                            std::string msg = boost::str(boost::format("[resource_name=%s] set_resource_property (irods::RESOURCE_HOST) in s3RedirectPlugin for detached mode failed") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }

                    }


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
                        result = irods_s3_cacheless::s3RedirectOpen(
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
                        result = ASSERT_ERROR(false, SYS_INVALID_INPUT_PARAM,
                                      "[resource_name=%s] Unknown redirect operation: \"%s\".", get_resource_name(_ctx.prop_map()).c_str(), _opr->c_str() );
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
