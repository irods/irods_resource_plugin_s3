# iRODS S3 Resource Plugin

This iRODS storage resource plugin allows iRODS to use any S3-compatible storage device or service to hold iRODS Data Objects, on-premise or in the cloud.

This plugin works when configured as an archive resource under the iRODS compound resource.  Such a configuration provides a POSIX interface to data held on an object storage device or service.

Install the plugin either via your package manager (`yum`, `apt-get`) and [the binary distributions](https://irods.org/download/ "iRODS downloads") or use the instructions below to to build from source.

## Build Prerequisites

To build the S3 Resource Plugin, you will need to have:

 - the iRODS Development Tools (irods-dev(el) and irods-runtime) installed for your platform
     https://irods.org/download

 - libxml2-dev(el)

 - libcurl4-gnutls-dev / curl-devel

 - libs3 installed for your platform
     https://github.com/irods/libs3

## Build Instructions

```
$ git clone -b 4-2-stable https://github.com/irods/irods_resource_plugin_s3
$ mkdir build
$ cd build
$ cmake ../irods_resource_plugin_s3
$ make package
```

This will result in a package (deb/rpm) for your platform suitable for installation.

## Example Configuration and Usage

After installation is complete, the new plugin can be configured live on an iRODS Server:

```
irods@hostname $ iadmin mkresc compResc compound
irods@hostname $ iadmin mkresc cacheResc unixfilesystem <hostname>:</full/path/to/Vault>
irods@hostname $ iadmin mkresc archiveResc s3 <hostname>:/<s3BucketName>/irods/Vault "S3_DEFAULT_HOSTNAME=s3.amazonaws.com;S3_AUTH_FILE=</full/path/to/AWS.keypair>;S3_RETRY_COUNT=<num reconn tries>;S3_WAIT_TIME_SEC=<wait between retries>;S3_PROTO=<HTTP|HTTPS>"
irods@hostname $ iadmin addchildtoresc compResc cacheResc cache
irods@hostname $ iadmin addchildtoresc compResc archiveResc archive
irods@hostname $ iput -R compResc foo.txt
irods@hostname $ ireg -R archiveResc /<s3BucketName>/full/path/in/bucket /full/logical/path/to/dataObject
```

The AWS/S3 keypair file `S3_AUTH_FILE` should have exactly two values (Access Key ID and Secret Access Key):

```
AKDJFH4KJHFCIOBJ5SLK
rlgjolivb7293r928vu98n498ur92jfgsdkjfh8e
```

## Configuration Options

The `S3_DEFAULT_HOSTNAME` may be comma-separated and represent more than one `host:port`:

```
S3_DEFAULT_HOSTNAME=192.168.122.128:443,192.168.122.129:443,192.168.122.130:443
```

If the `S3_DEFAULT_HOSTNAME` points to an AWS host, it should be formed in the following manner:

```
S3_DEFAULT_HOSTNAME=s3.<bucket-region>.amazonaws.com
```

Where the `<bucket-region>` is the one to which your bucket is set, in a form similar to `us-east-1`.

To control multipart uploads:
 - `S3_MPU_CHUNK` is the size of each part to be uploaded in parallel (in MB, default is 5MB).  Objects smaller than this will be uploaded with standard PUTs.
 - `S3_MPU_THREADS` is the number of parts to upload in parallel (only under Linux, default is 10).  On non-Linux OSes, this parameter is ignored and multipart uploads are performed sequentially.

Use the `ARCHIVE_NAMING_POLICY` parameter to control whether the names of the files within the object storage service (S3, or similar) are kept in sync with the logical names in the iRODS Catalog.
The default value of `consistent` will keep the names consistent.  Setting `ARCHIVE_NAMING_POLICY=decoupled` will not keep the names of the objects in sync.

To ensure end-to-end data integrity, MD5 checksums can be calculated and used for S3 uploads.  Note that this requires 2x the disk IO (because the file must first be read to calculate the MD5 before
the S3 upload can start) and a corresponding increase in CPU usage.
```
S3_ENABLE_MD5=[0|1]  (default is 0, off)
```

S3 server-side encryption can be enabled using the parameter `S3_SERVER_ENCRYPT=[0|1]` (default=0=off).  This is not the same as HTTPS, and implies that the data will be stored on disk encrypted.
To encrypt during the network transport to S3, use `S3_PROTO=HTTPS` (the default)

### Modifying your resource configuration

`location` is the external IP address your iRODS server is located at. Note that this is the `host` field when editing in iadmin.

`vault` is the name of your S3 bucket, e.g. `/s3-irods-test-bucket`. Note that this is the `path` field when editing in iadmin.

To change the hostname, keypair location, or other values in that block, use the `context` field in iadmin.

Make sure `S3_REGIONNAME` matches the region for your bucket and is in the form `us-east-1`.

### Using the S3 plugin in cacheless mode

The S3 plugin may be used in cacheless mode.  In this case the resource can be standalone and does not require an associated cache and compound resource.  This is still being actively developed and not all features that exist for cache mode have been implemented at this time.  The following have not been implemented or have not been tested at this time.

* Multiple hosts in a comma-separated list in `S3_DEFAULT_HOSTNAME`.
* `ARCHIVE_NAMING_POLICY` flag

An additional flag called `HOST_MODE` is used to enable cacheless mode.  The default value for this is `archive_attached` which provides the legacy functionality.  The valid settings are as follows:

* `archive_attached` - Legacy functionality.  Resource must be a child of a compound resource (parent/child context of archive) and must have a cache resource associated with it.
* `cacheless_attached` - Resource does not require a compound resource or a cache.  The resource remains tagged to the server defined in the `resc_net` property.  Any requests to this resource will be redirected to that server.
* `cacheless_detached` - Same as above but the resource is not uniquely pinned to a specific resource server.  Any resource server may fulfill a request.  This requires that all resource servers have network access to the S3 region.  (Note:  The cacheless S3 resource's host must be resolvable to an iRODS server.)

The cacheless version uses a local cache directory temporarily during uploads and downloads.  This can be set using the `S3_CACHE_DIR` parameter in the context string.  If it is not set, a directory under `/tmp` will be created and used.

The following is an example of how to configure a `cacheless_attached` S3 resource:

~~~~
iadmin mkresc s3resc s3 `hostname`:/irods-bucket/irods/Vault "S3_DEFAULT_HOSTNAME=s3.amazonaws.com;S3_AUTH_FILE=/var/lib/irods/s3.keypair;S3_REGIONNAME=us-east-1;S3_RETRY_COUNT=1;S3_WAIT_TIME_SEC=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_attached"
~~~~

#### Example of a baseline resource configuration
```
resource name: s3resc
id: 10017
zone: tempZone
type: s3
class: cache
location: 165.227.29.75
vault: /s3-irods-test
free space:
free space time: : Never
status:
info:
comment:
create time: 01575329511: 2019-12-02.23:31:51
modify time: 01576687386: 2019-12-18.16:43:06
context: S3_DEFAULT_HOSTNAME=s3.us-east-1.amazonaws.com;S3_AUTH_FILE=/var/lib/irods/s3.keypair;S3_REGIONNAME=us-east-1;S3_RETRY_COUNT=1;S3_WAIT_TIME_SEC=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_attached
parent:
parent context:
```

You can view your resource's properties via `ilsresc s3resc` (or whatever name you may have entered for it in place of `s3resc`).

## Using this plugin with Google Cloud Storage (GCS)

This plugin has been manually tested to work with google cloud storage. This works because Google has implemented the s3 protocol.  There are several differences:

* Google does not seem to support multipart uploads.  So it is necessary to disable this feature by adding the `S3_ENABLE_MPU=0` flag to the context string.
* The default hostname in the context string should be set to `storage.googleapis.com`
* The signature version should be set to `s3v4`
* The values in the key file have to be generated according to the instructions: https://cloud.google.com/storage/docs/migrating#keys
