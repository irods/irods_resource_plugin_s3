/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <curl/curl.h>
#include <pwd.h>
#include <grp.h>
#include <getopt.h>
#include <signal.h>

#include <fstream>
#include <vector>
#include <algorithm>
#include <map>
#include <string>
#include <list>

#include "common.h"
#include "s3fs.h"
#include "curl.h"
#include "cache.h"
#include "string_util.h"
#include "s3fs_util.h"
#include "fdcache.h"
#include "s3fs_auth.h"
#include "addhead.h"

#include "irods_stacktrace.hpp"

using namespace std;

//-------------------------------------------------------------------
// Define
//-------------------------------------------------------------------
enum dirtype {
  DIRTYPE_UNKNOWN = -1,
  DIRTYPE_NEW = 0,
  DIRTYPE_OLD = 1,
  DIRTYPE_FOLDER = 2,
  DIRTYPE_NOOBJ = 3,
};

#if !defined(ENOATTR)
#define ENOATTR				ENODATA
#endif

//-------------------------------------------------------------------
// Structs
//-------------------------------------------------------------------
typedef struct incomplete_multipart_info{
  string key;
  string id;
  string date;
}UNCOMP_MP_INFO;

typedef std::list<UNCOMP_MP_INFO>          uncomp_mp_list_t;
typedef std::list<std::string>             readline_t;
typedef std::map<std::string, std::string> kvmap_t;
typedef std::map<std::string, kvmap_t>     bucketkvmap_t;

//-------------------------------------------------------------------
// Global variables
//-------------------------------------------------------------------
bool foreground                                 = false;
bool nomultipart                                = false;
bool pathrequeststyle                           = true;
bool complement_stat                            = false;
std::string program_name;
std::string service_path                        = "/";
thread_local char host[MAX_NAME_LEN]            = {0}; //"https://s3.amazonaws.com";
thread_local char bucket[MAX_NAME_LEN]          = {0};
thread_local char endpoint[MAX_NAME_LEN]        = {0}; //"us-east-1";
std::string cipher_suites                       = "";
std::string instance_name                       = "";
s3fs_log_level debug_level                      = S3FS_LOG_CRIT;
const char*    s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
std::string aws_profile                         = "default";

// flag to say whether we're doing http or https
std::string s3_protocol_str                     = "";

//-------------------------------------------------------------------
// Static variables
//-------------------------------------------------------------------
static uid_t mp_uid               = 0;    // owner of mount point(only not specified uid opt)
static gid_t mp_gid               = 0;    // group of mount point(only not specified gid opt)
static mode_t mp_mode             = 0;    // mode of mount point
static std::string mountpoint;
static std::string passwd_file    = "";
static bool noxmlns               = false;
bool nocopyapi                    = false;
bool norenameapi                  = false;
static uid_t s3fs_uid             = 0;
static gid_t s3fs_gid             = 0;
static bool is_s3fs_uid           = false;// default does not set.
static bool is_s3fs_gid           = false;// default does not set.
static bool is_specified_endpoint = false;
static bool support_compat_dir    = true;// default supports compatibility directory type
static int max_keys_list_object   = 1000;// default is 1000

static const std::string allbucket_fields_type = "";         // special key for mapping(This name is absolutely not used as a bucket name)
static const std::string keyval_fields_type    = "\t";       // special key for mapping(This name is absolutely not used as a bucket name)

//-------------------------------------------------------------------
// Static functions : prototype
//-------------------------------------------------------------------
static FdEntity* get_local_fent(const char* path, bool is_load = false);
static int directory_empty(const char* path);
static bool is_truncated(xmlDocPtr doc);
static int append_objects_from_xml_ex(const char* path, xmlDocPtr doc, xmlXPathContextPtr ctx, 
              const char* ex_contents, const char* ex_key, const char* ex_etag, int isCPrefix, S3ObjList& head);
static int append_objects_from_xml(const char* path, xmlDocPtr doc, S3ObjList& head);
static bool GetXmlNsUrl(xmlDocPtr doc, string& nsurl);
static xmlChar* get_base_exp(xmlDocPtr doc, const char* exp);
static xmlChar* get_prefix(xmlDocPtr doc);
static xmlChar* get_next_marker(xmlDocPtr doc);
static char* get_object_name(xmlDocPtr doc, xmlNodePtr node, const char* path);
static int put_headers(const char* path, headers_t& meta, bool is_copy);
static int remote_mountpath_exists(const char* path);
static int s3fs_unlink(const char* path);
//int s3fs_check_service(void);

//-------------------------------------------------------------------
// Functions
//-------------------------------------------------------------------

//
// Get object attributes with stat cache.
// This function is base for s3fs_getattr().
//
// [NOTICE]
// Checking order is changed following list because of reducing the number of the requests.
// 1) "dir"
// 2) "dir/"
// 3) "dir_$folder$"
//
int get_object_attribute(const char* path, struct stat* pstbuf, headers_t* pmeta, bool overcheck, bool* pisforce, bool add_no_truncate_cache)
{
  int          result = -1;
  struct stat  tmpstbuf;
  struct stat* pstat = pstbuf ? pstbuf : &tmpstbuf;
  headers_t    tmpHead;
  headers_t*   pheader = pmeta ? pmeta : &tmpHead;
  string       strpath;
  S3fsCurl     s3fscurl;
  bool         forcedir = false;
  string::size_type Pos;

  S3FS_PRN_DBG("[path=%s]", path);

  if(!path || '\0' == path[0]){
    return -ENOENT;
  }

  memset(pstat, 0, sizeof(struct stat));
  if(0 == strcmp(path, "/") || 0 == strcmp(path, ".")){
    pstat->st_nlink = 1; // see fuse faq
    pstat->st_mode  = mp_mode;
    pstat->st_uid   = is_s3fs_uid ? s3fs_uid : mp_uid;
    pstat->st_gid   = is_s3fs_gid ? s3fs_gid : mp_gid;
    return 0;
  }

  // Check cache.
  pisforce    = (NULL != pisforce ? pisforce : &forcedir);
  (*pisforce) = false;
  strpath     = path;
  if(support_compat_dir && overcheck && string::npos != (Pos = strpath.find("_$folder$", 0))){
    strpath = strpath.substr(0, Pos);
    strpath += "/";
  }
  if(StatCache::getStatCacheData()->GetStat(strpath, pstat, pheader, overcheck, pisforce)){
    StatCache::getStatCacheData()->ChangeNoTruncateFlag(strpath, add_no_truncate_cache);
    return 0;
  }
  if(StatCache::getStatCacheData()->IsNoObjectCache(strpath)){
    // there is the path in the cache for no object, it is no object.
    return -ENOENT;
  }

  // At first, check path
  strpath     = path;
  result      = s3fscurl.HeadRequest(strpath.c_str(), (*pheader));
  s3fscurl.DestroyCurlHandle();

  // if not found target path object, do over checking
  if(0 != result){
    if(overcheck){
      // when support_compat_dir is disabled, strpath maybe have "_$folder$".
      if('/' != strpath[strpath.length() - 1] && string::npos == strpath.find("_$folder$", 0)){
        // now path is "object", do check "object/" for over checking
        strpath    += "/";
        result      = s3fscurl.HeadRequest(strpath.c_str(), (*pheader));
        s3fscurl.DestroyCurlHandle();
      }
      if(support_compat_dir && 0 != result){
        // now path is "object/", do check "object_$folder$" for over checking
        strpath     = strpath.substr(0, strpath.length() - 1);
        strpath    += "_$folder$";
        result      = s3fscurl.HeadRequest(strpath.c_str(), (*pheader));
        s3fscurl.DestroyCurlHandle();

        if(0 != result){
          // cut "_$folder$" for over checking "no dir object" after here
          if(string::npos != (Pos = strpath.find("_$folder$", 0))){
            strpath  = strpath.substr(0, Pos);
          }
        }
      }
    }
    if(support_compat_dir && 0 != result && string::npos == strpath.find("_$folder$", 0)){
      // now path is "object" or "object/", do check "no dir object" which is not object but has only children.
      if('/' == strpath[strpath.length() - 1]){
        strpath = strpath.substr(0, strpath.length() - 1);
      }
      if(-ENOTEMPTY == directory_empty(strpath.c_str())){
        // found "no dir object".
        strpath  += "/";
        *pisforce = true;
        result    = 0;
      }
    }
  }else{
    if(support_compat_dir && '/' != strpath[strpath.length() - 1] && string::npos == strpath.find("_$folder$", 0) && is_need_check_obj_detail(*pheader)){
      // check a case of that "object" does not have attribute and "object" is possible to be directory.
      if(-ENOTEMPTY == directory_empty(strpath.c_str())){
        // found "no dir object".
        strpath  += "/";
        *pisforce = true;
        result    = 0;
      }
    }
  }

  if(0 != result){
    // finally, "path" object did not find. Add no object cache.
    strpath = path;  // reset original
    StatCache::getStatCacheData()->AddNoObjectCache(strpath);
    return result;
  }

  // if path has "_$folder$", need to cut it.
  if(string::npos != (Pos = strpath.find("_$folder$", 0))){
    strpath = strpath.substr(0, Pos);
    strpath += "/";
  }

  // Set into cache
  //
  // [NOTE]
  // When add_no_truncate_cache is true, the stats is always cached.
  // This cached stats is only removed by DelStat().
  // This is necessary for the case to access the attribute of opened file.
  // (ex. getxattr() is called while writing to the opened file.)
  //
  if(add_no_truncate_cache || 0 != StatCache::getStatCacheData()->GetCacheSize()){
    // add into stat cache
    if(!StatCache::getStatCacheData()->AddStat(strpath, (*pheader), forcedir, add_no_truncate_cache)){
      S3FS_PRN_ERR("failed adding stat cache [path=%s]", strpath.c_str());
      return -ENOENT;
    }
    if(!StatCache::getStatCacheData()->GetStat(strpath, pstat, pheader, overcheck, pisforce)){
      // There is not in cache.(why?) -> retry to convert.
      if(!convert_header_to_stat(strpath.c_str(), (*pheader), pstat, forcedir)){
        S3FS_PRN_ERR("failed convert headers to stat[path=%s]", strpath.c_str());
        return -ENOENT;
      }
    }
  }else{
    // cache size is Zero -> only convert.
    if(!convert_header_to_stat(strpath.c_str(), (*pheader), pstat, forcedir)){
      S3FS_PRN_ERR("failed convert headers to stat[path=%s]", strpath.c_str());
      return -ENOENT;
    }
  }
  return 0;
}

//
// ssevalue is MD5 for SSE-C type, or KMS id for SSE-KMS
//
bool get_object_sse_type(const char* path, sse_type_t& ssetype, string& ssevalue)
{
  if(!path){
    return false;
  }

  headers_t meta;
  if(0 != get_object_attribute(path, NULL, &meta)){
    S3FS_PRN_ERR("Failed to get object(%s) headers", path);
    return false;
  }

  ssetype = SSE_DISABLE;
  ssevalue.erase();
  for(headers_t::iterator iter = meta.begin(); iter != meta.end(); ++iter){
    string key = (*iter).first;
    if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption") && 0 == strcasecmp((*iter).second.c_str(), "AES256")){
      ssetype  = SSE_S3;
    }else if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption-aws-kms-key-id")){
      ssetype  = SSE_KMS;
      ssevalue = (*iter).second;
    }else if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption-customer-key-md5")){
      ssetype  = SSE_C;
      ssevalue = (*iter).second;
    }
  }
  return true;
}

static FdEntity* get_local_fent(const char* path, bool is_load)
{
  struct stat stobj;
  FdEntity*   ent;
  headers_t   meta;

  S3FS_PRN_DBG("[path=%s]", path);

  if(0 != get_object_attribute(path, &stobj, &meta)){
    return NULL;
  }

  // open
  time_t mtime         = (!S_ISREG(stobj.st_mode) || S_ISLNK(stobj.st_mode)) ? -1 : stobj.st_mtime;
  bool   force_tmpfile = S_ISREG(stobj.st_mode) ? false : true;

  if(NULL == (ent = FdManager::get()->Open(path, &meta, static_cast<ssize_t>(stobj.st_size), mtime, force_tmpfile, true))){
    S3FS_PRN_ERR("Could not open file. errno(%d)", errno);
    return NULL;
  }
  // load
  if(is_load && !ent->OpenAndLoadAll(&meta)){
    S3FS_PRN_ERR("Could not load file. errno(%d)", errno);
    FdManager::get()->Close(ent);
    return NULL;
  }
  return ent;
}

/**
 * create or update s3 meta
 * ow_sse_flg is for over writing sse header by use_sse option.
 * @return fuse return code
 */
static int put_headers(const char* path, headers_t& meta, bool is_copy)
{
  int         result;
  S3fsCurl    s3fscurl(true);
  struct stat buf;

  S3FS_PRN_DBG("[path=%s]", path);

  // files larger than 5GB must be modified via the multipart interface
  // *** If there is not target object(a case of move command),
  //     get_object_attribute() returns error with initializing buf.
  (void)get_object_attribute(path, &buf);

  if(buf.st_size >= FIVE_GB){
    // multipart
    if(0 != (result = s3fscurl.MultipartHeadRequest(path, buf.st_size, meta, is_copy))){
      return result;
    }
  }else{
    if(0 != (result = s3fscurl.PutHeadRequest(path, meta, is_copy))){
      return result;
    }
  }

  FdEntity* ent = NULL;
  if(NULL == (ent = FdManager::get()->ExistOpen(path, -1, !(FdManager::get()->IsCacheDir())))){
    // no opened fd
    if(FdManager::get()->IsCacheDir()){
      // create cache file if be needed
      ent = FdManager::get()->Open(path, &meta, static_cast<ssize_t>(buf.st_size), -1, false, true);
    }
  }
  if(ent){
    time_t mtime = get_mtime(meta);
    ent->SetMtime(mtime);
    FdManager::get()->Close(ent);
  }

  return 0;
}

// common function for creation of a plain object
int create_file_object(const char* path)
{
    headers_t meta;
    meta["Content-Type"]     = S3fsCurl::LookupMimeType(std::string(path));
    meta["x-amz-meta-uid"]   = "999";
    meta["x-amz-meta-gid"]   = "999";
    meta["x-amz-meta-mode"]  = "33204";
    meta["x-amz-meta-mtime"] = str(time(NULL));
  
    S3fsCurl s3fscurl(true);
    return s3fscurl.PutRequest(path, meta, -1);    // fd=-1 means for creating zero byte object.
}


static int directory_empty(const char* path)
{
  int result;
  S3ObjList head;

  if((result = list_bucket(path, head, "/", true)) != 0){
    S3FS_PRN_ERR("list_bucket returns error.");
    return result;
  }
  if(!head.IsEmpty()){
    return -ENOTEMPTY;
  }
  return 0;
}

int rename_object(const char* from, const char* to)
{
  int result;
  string s3_realpath;
  headers_t meta;

  S3FS_PRN_DBG("[from=%s][to=%s]", from , to);

/*
  if(0 != (result = check_parent_object_access(to, W_OK | X_OK))){
    // not permit writing "to" object parent dir.
    return result;
  }
  if(0 != (result = check_parent_object_access(from, W_OK | X_OK))){
    // not permit removing "from" object parent dir.
    return result;
  }*/

  if(0 != (result = get_object_attribute(from, nullptr, &meta))){
    return result;
  }
  s3_realpath = get_realpath(from);

  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + s3_realpath);
  meta["Content-Type"]             = S3fsCurl::LookupMimeType(string(to));
  meta["x-amz-metadata-directive"] = "REPLACE";

  if(0 != (result = put_headers(to, meta, true))){
    return result;
  }

  FdManager::get()->Rename(from, to);

  s3fs_unlink(from);
  result = 0;
  StatCache::getStatCacheData()->DelStat(to);

  return result;
}

int rename_object_nocopy(const char* from, const char* to)
{
  int result;

  S3FS_PRN_DBG("[from=%s][to=%s]", from , to);

/*  if(0 != (result = check_parent_object_access(to, W_OK | X_OK))){
    // not permit writing "to" object parent dir.
    return result;
  }
  if(0 != (result = check_parent_object_access(from, W_OK | X_OK))){
    // not permit removing "from" object parent dir.
    return result;
  }*/

  // open & load
  FdEntity* ent;
  if(NULL == (ent = get_local_fent(from, true))){
    S3FS_PRN_ERR("could not open and read file(%s)", from);
    return -EIO;
  }

  // Set header
  if(!ent->SetContentType(to)){
    S3FS_PRN_ERR("could not set content-type for %s", to);
    return -EIO;
  }

  // upload
  if(0 != (result = ent->RowFlush(to, true))){
    S3FS_PRN_ERR("could not upload file(%s): result=%d", to, result);
    FdManager::get()->Close(ent);
    return result;
  }
  FdManager::get()->Close(ent);

  // Remove file
  result = 0;
  //result = s3fs_unlink(from);

  // Stats
  StatCache::getStatCacheData()->DelStat(to);
  StatCache::getStatCacheData()->DelStat(from);

  return result;
}

int rename_large_object(const char* from, const char* to)
{
  int         result;
  struct stat buf;
  headers_t   meta;

  S3FS_PRN_DBG("[from=%s][to=%s]", from , to);

  /*if(0 != (result = check_parent_object_access(to, W_OK | X_OK))){
    // not permit writing "to" object parent dir.
    return result;
  }
  if(0 != (result = check_parent_object_access(from, W_OK | X_OK))){
    // not permit removing "from" object parent dir.
    return result;
  }
  if(0 != (result = get_object_attribute(from, &buf, &meta, false))){
    return result;
  }*/

  S3fsCurl s3fscurl(true);
  if(0 != (result = s3fscurl.MultipartRenameRequest(from, to, meta, buf.st_size))){
    return result;
  }
  s3fscurl.DestroyCurlHandle();
  StatCache::getStatCacheData()->DelStat(to);

  return 0; //s3fs_unlink(from);
}

int list_bucket(const char* path, S3ObjList& head, const char* delimiter, bool check_content_only)
{
  string    s3_realpath;
  string    query_delimiter;;
  string    query_prefix;;
  string    query_maxkey;;
  string    next_marker = "";
  bool      truncated = true;
  S3fsCurl  s3fscurl;
  xmlDocPtr doc;

  S3FS_PRN_DBG("[path=%s]", path);

  if(delimiter && 0 < strlen(delimiter)){
    query_delimiter += "delimiter=";
    query_delimiter += delimiter;
    query_delimiter += "&";
  }

  query_prefix += "&prefix=";
  s3_realpath = get_realpath(path);
  if(0 == s3_realpath.length() || '/' != s3_realpath[s3_realpath.length() - 1]){
    // last word must be "/"
    query_prefix += urlEncode(s3_realpath.substr(1) + "/");
  }else{
    query_prefix += urlEncode(s3_realpath.substr(1));
  }
  if (check_content_only){
    // Just need to know if there are child objects in dir
    // For dir with children, expect "dir/" and "dir/child"
    query_maxkey += "max-keys=2";
  }else{
    query_maxkey += "max-keys=" + str(max_keys_list_object);
  }

  while(truncated){
    string each_query = query_delimiter;
    if(next_marker != ""){
      each_query += "marker=" + urlEncode(next_marker) + "&";
      next_marker = "";
    }
    each_query += query_maxkey;
    each_query += query_prefix;

    // request
    int result; 
    if(0 != (result = s3fscurl.ListBucketRequest(path, each_query.c_str()))){
      S3FS_PRN_ERR("ListBucketRequest returns with error.");
      return result;
    }
    BodyData* body = s3fscurl.GetBodyData();

    // xmlDocPtr
    if(NULL == (doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0))){
      S3FS_PRN_ERR("xmlReadMemory returns with error.");
      return -1;
    }
    if(0 != append_objects_from_xml(path, doc, head)){
      S3FS_PRN_ERR("append_objects_from_xml returns with error.");
      xmlFreeDoc(doc);
      return -1;
    }
    if(true == (truncated = is_truncated(doc))){
      xmlChar*	tmpch = get_next_marker(doc);
      if(tmpch){
        next_marker = (char*)tmpch;
        xmlFree(tmpch);
      }else{
        // If did not specify "delimiter", s3 did not return "NextMarker".
        // On this case, can use last name for next marker.
        //
        string lastname;
        if(!head.GetLastName(lastname)){
          S3FS_PRN_WARN("Could not find next marker, thus break loop.");
          truncated = false;
        }else{
          next_marker = s3_realpath.substr(1);
          if(0 == s3_realpath.length() || '/' != s3_realpath[s3_realpath.length() - 1]){
            next_marker += "/";
          }
          next_marker += lastname;
        }
      }
    }
    S3FS_XMLFREEDOC(doc);

    // reset(initialize) curl object
    s3fscurl.DestroyCurlHandle();

    if (check_content_only)
      break;
  }
  S3FS_MALLOCTRIM(0);

  return 0;
}

static const char* c_strErrorObjectName = "FILE or SUBDIR in DIR";

static int append_objects_from_xml_ex(const char* path, xmlDocPtr doc, xmlXPathContextPtr ctx, 
       const char* ex_contents, const char* ex_key, const char* ex_etag, int isCPrefix, S3ObjList& head)
{
  xmlXPathObjectPtr contents_xp;
  xmlNodeSetPtr content_nodes;

  if(NULL == (contents_xp = xmlXPathEvalExpression((xmlChar*)ex_contents, ctx))){
    S3FS_PRN_ERR("xmlXPathEvalExpression returns null.");
    return -1;
  }
  if(xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)){
    S3FS_PRN_DBG("contents_xp->nodesetval is empty.");
    S3FS_XMLXPATHFREEOBJECT(contents_xp);
    return 0;
  }
  content_nodes = contents_xp->nodesetval;

  bool   is_dir;
  string stretag;
  int    i;
  for(i = 0; i < content_nodes->nodeNr; i++){
    ctx->node = content_nodes->nodeTab[i];

    // object name
    xmlXPathObjectPtr key;
    if(NULL == (key = xmlXPathEvalExpression((xmlChar*)ex_key, ctx))){
      S3FS_PRN_WARN("key is null. but continue.");
      continue;
    }
    if(xmlXPathNodeSetIsEmpty(key->nodesetval)){
      S3FS_PRN_WARN("node is empty. but continue.");
      xmlXPathFreeObject(key);
      continue;
    }
    xmlNodeSetPtr key_nodes = key->nodesetval;
    char* name = get_object_name(doc, key_nodes->nodeTab[0]->xmlChildrenNode, path);

    if(!name){
      S3FS_PRN_WARN("name is something wrong. but continue.");

    }else if((const char*)name != c_strErrorObjectName){
      is_dir  = isCPrefix ? true : false;
      stretag = "";

      if(!isCPrefix && ex_etag){
        // Get ETag
        xmlXPathObjectPtr ETag;
        if(NULL != (ETag = xmlXPathEvalExpression((xmlChar*)ex_etag, ctx))){
          if(xmlXPathNodeSetIsEmpty(ETag->nodesetval)){
            S3FS_PRN_DBG("ETag->nodesetval is empty.");
          }else{
            xmlNodeSetPtr etag_nodes = ETag->nodesetval;
            xmlChar* petag = xmlNodeListGetString(doc, etag_nodes->nodeTab[0]->xmlChildrenNode, 1);
            if(petag){
              stretag = (char*)petag;
              xmlFree(petag);
            }
          }
          xmlXPathFreeObject(ETag);
        }
      }
      if(!head.insert(name, (0 < stretag.length() ? stretag.c_str() : NULL), is_dir)){
        S3FS_PRN_ERR("insert_object returns with error.");
        xmlXPathFreeObject(key);
        xmlXPathFreeObject(contents_xp);
        free(name);
        S3FS_MALLOCTRIM(0);
        return -1;
      }
      free(name);
    }else{
      S3FS_PRN_DBG("name is file or subdir in dir. but continue.");
    }
    xmlXPathFreeObject(key);
  }
  S3FS_XMLXPATHFREEOBJECT(contents_xp);

  return 0;
}

static bool GetXmlNsUrl(xmlDocPtr doc, string& nsurl)
{
  static time_t tmLast = 0;  // cache for 60 sec.
  static string strNs("");
  bool result = false;

  if(!doc){
    return result;
  }
  if((tmLast + 60) < time(NULL)){
    // refresh
    tmLast = time(NULL);
    strNs  = "";
    xmlNodePtr pRootNode = xmlDocGetRootElement(doc);
    if(pRootNode){
      xmlNsPtr* nslist = xmlGetNsList(doc, pRootNode);
      if(nslist){
        if(nslist[0] && nslist[0]->href){
          strNs  = (const char*)(nslist[0]->href);
        }
        S3FS_XMLFREE(nslist);
      }
    }
  }
  if(0 < strNs.size()){
    nsurl  = strNs;
    result = true;
  }
  return result;
}

static int append_objects_from_xml(const char* path, xmlDocPtr doc, S3ObjList& head)
{
  string xmlnsurl;
  string ex_contents = "//";
  string ex_key      = "";
  string ex_cprefix  = "//";
  string ex_prefix   = "";
  string ex_etag     = "";

  if(!doc){
    return -1;
  }

  // If there is not <Prefix>, use path instead of it.
  xmlChar* pprefix = get_prefix(doc);
  string   prefix  = (pprefix ? (char*)pprefix : path ? path : "");
  if(pprefix){
    xmlFree(pprefix);
  }

  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    ex_contents+= "s3:";
    ex_key     += "s3:";
    ex_cprefix += "s3:";
    ex_prefix  += "s3:";
    ex_etag    += "s3:";
  }
  ex_contents+= "Contents";
  ex_key     += "Key";
  ex_cprefix += "CommonPrefixes";
  ex_prefix  += "Prefix";
  ex_etag    += "ETag";

  if(-1 == append_objects_from_xml_ex(prefix.c_str(), doc, ctx, ex_contents.c_str(), ex_key.c_str(), ex_etag.c_str(), 0, head) ||
     -1 == append_objects_from_xml_ex(prefix.c_str(), doc, ctx, ex_cprefix.c_str(), ex_prefix.c_str(), NULL, 1, head) )
  {
    S3FS_PRN_ERR("append_objects_from_xml_ex returns with error.");
    S3FS_XMLXPATHFREECONTEXT(ctx);
    return -1;
  }
  S3FS_XMLXPATHFREECONTEXT(ctx);

  return 0;
}

static xmlChar* get_base_exp(xmlDocPtr doc, const char* exp)
{
//std::cerr << irods::stacktrace().dump();
  xmlXPathObjectPtr  marker_xp;
  string xmlnsurl;
  string exp_string;

  if(!doc){
    return NULL;
  }
  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    exp_string = "/s3:ListBucketResult/s3:";
  } else {
    exp_string = "/ListBucketResult/";
  }
  
  exp_string += exp;

  if(NULL == (marker_xp = xmlXPathEvalExpression((xmlChar *)exp_string.c_str(), ctx))){
    xmlXPathFreeContext(ctx);
    return NULL;
  }
  if(xmlXPathNodeSetIsEmpty(marker_xp->nodesetval)){
//std::cerr << irods::stacktrace().dump();

    S3FS_PRN_DBG("marker_xp->nodesetval is empty.");
    xmlXPathFreeObject(marker_xp);
    xmlXPathFreeContext(ctx);
    return NULL;
  }
  xmlNodeSetPtr nodes  = marker_xp->nodesetval;
  xmlChar*      result = xmlNodeListGetString(doc, nodes->nodeTab[0]->xmlChildrenNode, 1);

  xmlXPathFreeObject(marker_xp);
  xmlXPathFreeContext(ctx);

  return result;
}

static xmlChar* get_prefix(xmlDocPtr doc)
{
  return get_base_exp(doc, "Prefix");
}

static xmlChar* get_next_marker(xmlDocPtr doc)
{
  return get_base_exp(doc, "NextMarker");
}

static bool is_truncated(xmlDocPtr doc)
{
  bool result = false;

  xmlChar* strTruncate = get_base_exp(doc, "IsTruncated");
  if(!strTruncate){
    return result;
  }
  if(0 == strcasecmp((const char*)strTruncate, "true")){
    result = true;
  }
  xmlFree(strTruncate);
  return result;
}

// return: the pointer to object name on allocated memory.
//         the pointer to "c_strErrorObjectName".(not allocated)
//         NULL(a case of something error occurred)
static char* get_object_name(xmlDocPtr doc, xmlNodePtr node, const char* path)
{
  // Get full path
  xmlChar* fullpath = xmlNodeListGetString(doc, node, 1);
  if(!fullpath){
    S3FS_PRN_ERR("could not get object full path name..");
    return NULL;
  }
  // basepath(path) is as same as fullpath.
  if(0 == strcmp((char*)fullpath, path)){
    xmlFree(fullpath);
    return (char*)c_strErrorObjectName;
  }

  // Make dir path and filename
  string   strdirpath = mydirname(string((char*)fullpath));
  string   strmybpath = mybasename(string((char*)fullpath));
  const char* dirpath = strdirpath.c_str();
  const char* mybname = strmybpath.c_str();
  const char* basepath= (path && '/' == path[0]) ? &path[1] : path;
  xmlFree(fullpath);

  if(!mybname || '\0' == mybname[0]){
    return NULL;
  }

  // check subdir & file in subdir
  if(dirpath && 0 < strlen(dirpath)){
    // case of "/"
    if(0 == strcmp(mybname, "/") && 0 == strcmp(dirpath, "/")){
      return (char*)c_strErrorObjectName;
    }
    // case of "."
    if(0 == strcmp(mybname, ".") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of ".."
    if(0 == strcmp(mybname, "..") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of "name"
    if(0 == strcmp(dirpath, ".")){
      // OK
      return strdup(mybname);
    }else{
      if(basepath && 0 == strcmp(dirpath, basepath)){
        // OK
        return strdup(mybname);
      }else if(basepath && 0 < strlen(basepath) && '/' == basepath[strlen(basepath) - 1] && 0 == strncmp(dirpath, basepath, strlen(basepath) - 1)){
        string withdirname = "";
        if(strlen(dirpath) > strlen(basepath)){
          withdirname = &dirpath[strlen(basepath)];
        }
        if(0 < withdirname.length() && '/' != withdirname[withdirname.length() - 1]){
          withdirname += "/";
        }
        withdirname += mybname;
        return strdup(withdirname.c_str());
      }
    }
  }
  // case of something wrong
  return (char*)c_strErrorObjectName;
}

static int remote_mountpath_exists(const char* path)
{
  struct stat stbuf;

  S3FS_PRN_DBG("[path=%s]", path);

  // getattr will prefix the path with the remote mountpoint
  if(0 != get_object_attribute("/", &stbuf, NULL)){
    return -1;
  }
  if(!S_ISDIR(stbuf.st_mode)){
    return -1;
  }
  return 0;
}

//
// If calling with wrong region, s3fs gets following error body as 400 error code.
// "<Error><Code>AuthorizationHeaderMalformed</Code><Message>The authorization header is 
//  malformed; the region 'us-east-1' is wrong; expecting 'ap-northeast-1'</Message>
//  <Region>ap-northeast-1</Region><RequestId>...</RequestId><HostId>...</HostId>
//  </Error>"
//
// So this is cheep codes but s3fs should get correct region automatically.
//
static bool check_region_error(const char* pbody, string& expectregion)
{
  if(!pbody){
    return false;
  }
  const char* region;
  const char* regionend;
  if(NULL == (region = strcasestr(pbody, "<Message>The authorization header is malformed; the region "))){
    return false;
  }
  if(NULL == (region = strcasestr(region, "expecting \'"))){
    return false;
  }
  region += strlen("expecting \'");
  if(NULL == (regionend = strchr(region, '\''))){
    return false;
  }
  string strtmp(region, (regionend - region));
  if(0 == strtmp.length()){
    return false;
  }
  expectregion = strtmp;

  return true;
}

int s3fs_check_service(void)
{
  S3FS_PRN_DBG("check services.");

  // At first time for access S3, we check IAM role if it sets.
  if(!S3fsCurl::CheckIAMCredentialUpdate()){
    S3FS_PRN_CRIT("Failed to check IAM role name(%s).", S3fsCurl::GetIAMRole());
    return EXIT_FAILURE;
  }

  S3fsCurl s3fscurl;
  int      res;
  if(0 > (res = s3fscurl.CheckBucket())){
    // get response code
    long responseCode = s3fscurl.GetLastResponseCode();

    // check wrong endpoint, and automatically switch endpoint
    if(responseCode == 400 && !is_specified_endpoint){
      // check region error
      BodyData* body = s3fscurl.GetBodyData();
      string    expectregion;
      if(check_region_error(body->str(), expectregion)){
        // not specified endpoint, so try to connect to expected region.
        S3FS_PRN_CRIT("Could not connect wrong region %s, so retry to connect region %s.", endpoint, expectregion.c_str());
        strncpy(endpoint, expectregion.c_str(), MAX_NAME_LEN-1);
        if(S3fsCurl::IsSignatureV4()){
            if(0 == strncmp(host, "http://s3.amazonaws.com", MAX_NAME_LEN)){
                strncpy(host, (std::string("http://s3-") + endpoint + ".amazonaws.com").c_str(), MAX_NAME_LEN - 1);
            }else if(0 == strncmp(host, "https://s3.amazonaws.com", MAX_NAME_LEN)){
                strncpy(host, (std::string("http://s3-") + endpoint + ".amazonaws.com").c_str(), MAX_NAME_LEN - 1);
            }
        }

        // retry to check with new endpoint
        s3fscurl.DestroyCurlHandle();
        res          = s3fscurl.CheckBucket();
        responseCode = s3fscurl.GetLastResponseCode();
      }
    }

    // try signature v2
    if(0 > res && (responseCode == 400 || responseCode == 403) && S3fsCurl::IsSignatureV4()){
      // switch sigv2
      S3FS_PRN_DBG("Could not connect, so retry to connect by signature version 2.");
      S3fsCurl::SetSignatureV4(false);

      // retry to check with sigv2
      s3fscurl.DestroyCurlHandle();
      res          = s3fscurl.CheckBucket();
      responseCode = s3fscurl.GetLastResponseCode();
    }

    // check errors(after retrying)
    if(0 > res && responseCode != 200 && responseCode != 301){
      if(responseCode == 400){
        S3FS_PRN_CRIT("Bad Request(host=%s) - result of checking service.", host);

      }else if(responseCode == 403){
        S3FS_PRN_CRIT("invalid credentials(host=%s) - result of checking service.", host);

      }else if(responseCode == 404){
        S3FS_PRN_CRIT("bucket not found(host=%s) - result of checking service.", host);

      }else if(responseCode == CURLE_OPERATION_TIMEDOUT){
        // unable to connect
        S3FS_PRN_CRIT("unable to connect bucket and timeout(host=%s) - result of checking service.", host);
      }else{
        // another error
        S3FS_PRN_CRIT("unable to connect(host=%s) - result of checking service.", host);
      }
      return EXIT_FAILURE;
    }
  }
  s3fscurl.DestroyCurlHandle();

  // make sure remote mountpath exists and is a directory
  if(mount_prefix.size() > 0){
    if(remote_mountpath_exists(mount_prefix.c_str()) != 0){
      S3FS_PRN_CRIT("remote mountpath %s not found.", mount_prefix.c_str());
      return EXIT_FAILURE;
    }
  }
  S3FS_MALLOCTRIM(0);

  return EXIT_SUCCESS;
}

static int s3fs_unlink(const char* path)
{
  int result;

  S3FS_PRN_DBG("[path=%s]", path);

  S3fsCurl s3fscurl;
  result = s3fscurl.DeleteRequest(path);
  FdManager::DeleteCacheFile(path);
  StatCache::getStatCacheData()->DelStat(path);
  S3FS_MALLOCTRIM(0);

  return result;
}

