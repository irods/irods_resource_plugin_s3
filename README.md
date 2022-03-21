# iRODS S3 Resource Plugin

This iRODS storage resource plugin allows iRODS to use any S3-compatible storage device or service to hold iRODS Data Objects, on-premise or in the cloud.

This plugin can work as a standalone "cacheless" resource or as an archive resource under the iRODS compound resource.  Either configuration provides a POSIX interface to data held on an object storage device or service.

Install the plugin either via your package manager (`yum`/`apt`) and [the binary distributions](https://irods.org/download/ "iRODS Download") or use the instructions below to build from source.

## Build Prerequisites

To build the S3 Resource Plugin, you will need to have:

- the iRODS Development Tools (irods-dev(el) and irods-runtime) from https://irods.org/download

- libxml2-dev(el)

- libcurl4-gnutls-dev / curl-devel

- libs3 from https://github.com/irods/libs3

## Build Instructions

```
$ git clone -b 4-2-stable https://github.com/irods/irods_resource_plugin_s3
$ mkdir build
$ cd build
$ cmake ../irods_resource_plugin_s3
$ make package
```

This will result in a package (deb/rpm) for your platform suitable for installation.

## Example Cacheless Configuration and Usage

After installation is complete, the new plugin can be configured in cacheless mode, live on an iRODS Server:

```
irods@hostname $ iadmin mkresc s3resc s3 $(hostname):/<s3BucketName>/prefix/in/bucket "S3_DEFAULT_HOSTNAME=s3.us-east-1.amazonaws.com;S3_AUTH_FILE=/var/lib/irods/s3.keypair;S3_REGIONNAME=us-east-1;S3_RETRY_COUNT=1;S3_WAIT_TIME_SECONDS=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_attached"
```

A local file can be immediately put into the S3 resource:
```
irods@hostname $ iput -R s3resc foo.txt
```

An object already in S3 can be registered into the iRODS Catalog:
```
irods@hostname $ ireg -R s3resc /<s3BucketName>/full/path/in/bucket /full/logical/path/to/dataObject
```

The S3 Keypair file `S3_AUTH_FILE` should have exactly two values (Access Key ID and Secret Access Key), one per line:

```
AKDJFH4KJHFCIOBJ5SLK
rlgjolivb7293r928vu98n498ur92jfgsdkjfh8e
```

## Configuration Options

The `S3_DEFAULT_HOSTNAME` may be comma-separated and represent more than one `host:port`:

```
S3_DEFAULT_HOSTNAME=192.168.122.128:443,192.168.122.129:443,192.168.122.130:443
```

If the `S3_DEFAULT_HOSTNAME` points to an AWS host, [best practice](https://docs.aws.amazon.com/general/latest/gr/s3.html) includes the bucket region (e.g. `us-east-1`):

```
S3_DEFAULT_HOSTNAME=s3.<bucket-region>.amazonaws.com
```

To define S3 provider constraints and control multipart behavior:
-   `S3_MPU_CHUNK` - This defines the minimum part size allowed (in MB).  The default is 5MB which is the minimum part size defined in AWS.
-   `S3_ENABLE_MPU=0` disables multipart uploads.
-   `S3_MAX_UPLOAD_SIZE` - This defines the maximum upload size for non-multipart uploads (in MB), maximum part size, as well as the maximum size when using the CopyObject API.  The default is 5120MB (5GB).  This setting is ignored if MPU uploads are disabled.
-   `S3_MPU_THREADS` is the number of parts to upload in parallel.
-   `S3_URI_REQUEST_STYLE` - The path request style used.  This is either "path" or "virtualhost".  The default is "path".  See [path vs virtual hosted requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).
-   `S3_RESTORATION_DAYS` - The number of days an object is to be restored when restoring from Glacier.  See [RestoreObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html).
-   `S3_RESTORATION_TIER` - The data access tier option when restoring from Glacier.  Valid values are "Expedited", "Standard", and "Bulk".  The default is "Standard".  See [RestoreObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html).
-   `S3_ENABLE_COPYOBJECT` - Some providers (such as Fujifilm) do not implement the CopyObject S3 API.  If S3_ENABLE_COPYOBJECT=0, the copy will be performed via a read from source and write to destination rather than calling CopyObject.  (Also see the note about GCS support.)

> Notes about virtual hosting:  When using virtual hosted request style, configure the resource path and S3_DEFAULT_HOSTNAME as you would for path request style.  Leave the bucket name in the path and do not put the bucket name in the S3_DEFAULT_HOSTNAME.  This is important to retain backward compatibility with objects already created using path request style. 

Use the `ARCHIVE_NAMING_POLICY` parameter to control whether the names of the files within the object storage service (S3, or similar) are kept in sync with the logical names in the iRODS Catalog.
The default value of `consistent` will keep the names consistent.  Setting `ARCHIVE_NAMING_POLICY=decoupled` will not keep the names of the objects in sync.

S3 server-side encryption can be enabled using the parameter `S3_SERVER_ENCRYPT=[0|1]` (default=0=off).  This is not the same as HTTPS, and implies that the data will be stored on disk encrypted.
To encrypt during the network transport to S3, use `S3_PROTO=HTTPS` (the default)

The `S3_RETRY_COUNT` defines the number of retries for a request after a retryable failure.  The default is 3 retries.
The `S3_WAIT_TIME_SECONDS` defines the initial number of seconds between retries.  The default is 2s.  This wait time will double on each successive failure until the `S3_MAX_WAIT_TIME_SECONDS` is reached.  If this is set to 0, there will be no wait between retries.  (Note:  For backward compatibility with previous releases, `S3_WAIT_TIME_SEC` is also valid but `S3_WAIT_TIME_SECONDS` will take priority.)
The `S3_MAX_WAIT_TIME_SECONDS` is the maximum wait value during successive failures.  The default is 30s.  If this is set to 0, there will be no wait between retries.  (Note:  For backward compatibility with previous releases, `S3_MAX_WAIT_TIME_SEC` is also valid but `S3_MAX_WAIT_TIME_SECONDS` will take priority.)
The `S3_NON_DATA_TRANSFER_TIMEOUT_SECONDS` defines the timeout value used for S3_complete_multipart_upload and S3_delete_object.  The default is 300s.  (Note:  This has been added because in some cases with very large files these take a long time to complete and the data transfer thresholds do not apply to these.)

### Modifying your resource configuration

`location` is the hostname of your iRODS server.  Modify this field as `host` when using `iadmin`.

`vault` contains the S3 bucket name and prefix (`/s3-irods-bucket-name/prefix/in/bucket`. Modify this field as `path` when using `iadmin`.

Modify the `context` field to update any of the resource-specific settings (`S3_DEFAULT_HOSTNAME`, `S3_AUTH_FILE`, etc.)

Confirm `S3_REGIONNAME` matches the region for your bucket and is in the form `us-east-1`.

### Using the S3 plugin in cacheless mode

The S3 plugin may be used in cacheless mode.  In this case the resource can be standalone and does not require an associated cache and compound resource.  This is still being actively developed and not all features that exist for cache mode have been implemented at this time.  The following have not been implemented or have not been tested at this time.

* Multiple hosts in a comma-separated list in `S3_DEFAULT_HOSTNAME`.

An additional flag called `HOST_MODE` is used to enable cacheless mode.  The default value for this is `archive_attached` which provides the legacy functionality.  The valid settings are as follows:

* `archive_attached` - Legacy functionality.  Resource must be a child of a compound resource (parent/child context of archive) and must have a cache resource associated with it.
* `cacheless_attached` - Resource does not require a compound resource or a cache.  The resource remains tagged to the server defined in the `resc_net` property.  Any requests to this resource will be redirected to that server.
* `cacheless_detached` - Same as above but the resource is not uniquely pinned to a specific resource server.  Any resource server may fulfill a request.  This requires that all resource servers have network access to the S3 region.  (Note:  The cacheless S3 resource's host must be resolvable to an iRODS server.)

Cacheless mode has a few extra configuration parameters in addition to HOST_MODE.

-   `CIRCULAR_BUFFER_SIZE` - The plugin uses a circular buffer to store data while it is being streamed to S3.  The size of the circular buffer is CIRCULAR_BUFFER_SIZE * S3_MPU_CHUNK.  The default value is 4 so if the S3_MPU_CHUNK is the default of 5MB the circular buffer size will be 20MB.  CIRCULAR_BUFFER_SIZE must be at least 2.  If a size is set lower than 2 then it will default to 2.
-   `CIRCULAR_BUFFER_TIMEOUT_SECONDS` - The number of seconds the plugin will wait when waiting to read or write data from the circular buffer.  The default is 180s.
-   `S3_CACHE_DIR` - This is the directory where temporary cache files are located in cases where a cache file is required.  (See below.)  The default is `/tmp`.

The following is an example of how to configure a `cacheless_attached` S3 resource:

```
iadmin mkresc s3resc s3 $(hostname):/s3-irods-bucket-name/prefix/in/bucket "S3_DEFAULT_HOSTNAME=s3.us-east-1.amazonaws.com;S3_AUTH_FILE=/var/lib/irods/s3.keypair;S3_REGIONNAME=us-east-1;S3_RETRY_COUNT=1;S3_WAIT_TIME_SECONDS=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_attached"
```

Some configuration settings have special meaning when the resource is in cacheless mode.
-   When S3_ENABLE_MPU = 0, a cache file will be used when the S3 plugin receives parallel uploads from iRODS.
-   When iRODS is using parallel transfer but each transfer part is less than S3_MPU_CHUNK, a cache file will be used
-   The S3_MPU_THREADS setting is only used when flushing a cache file to S3.  In streaming mode iRODS controls the number of transfer threads that are used.

### Cache Rules When Using Cacheless Mode

Care was taken to limit the use of a cache file when cacheless mode is enabled.  However, there are scenarios where a cache file is required.  The following explains when a cache file is required or when cacheless streaming is performed.

1.  All objects opened in read-only mode (including `iget`) will be cacheless as S3 allows random access reads on S3 objects.
2.  All `iput` and `irepl` will stream without a cache except in the following two cases:
-   iRODS is performing a parallel transfer but multipart uploads is disabled.
-   iRODS is performing a parallel transfer but each part size < S3_MPU_CHUNK size.

In the cases where a cache file must be used, the base directory for the cache files can be set using the `S3_CACHE_DIR` parameter in the context string.  If it is not set, a directory under `/tmp` will be created and used.  The cache files are transient and are removed once the data object is closed.

#### Expectations on clients using the s3_transport/dstream directly when the put_repl_flag is set to true

Clients using s3_transport/dstream must set the put_repl_flag to true to use cacheless streaming.  In this case, the s3_transport has some expectations on the behavior of the client.  If these are not followed the results are undefined and the transfers will likely fail.

1.  If the number_of_client_transfer_threads is set to 1, a single thread will send all of the bytes starting from the first byte to the last byte in sequential order.
2.  If the number_of_client_transfer_threads is greater than 1:
-   Each thread must stream the bytes from the source at the offset of thread_number * (object_size / number_of_client_transfer_threads).
-   The last thread will send the extra bytes.
-   Each thread will call s3_transport_ptr->set_part_size(n) where n is the size of its part.
-   The bytes for each thread will be sent sequentially and all bytes will be sent.

This conforms to the way iput breaks up the files when doing parallel writes.  The reason for these is so that the s3_transport object can always determine the part number by the object size and offset.

### Using the S3 plugin in archive mode (under compound)

The S3 plugin may be used in archive mode. In this case the resource requires an associated cache and compound resource and configured as follows:

```
irods@hostname $ iadmin mkresc compResc compound
irods@hostname $ iadmin mkresc cacheResc unixfilesystem <hostname>:</full/path/to/Vault>
irods@hostname $ iadmin mkresc archiveResc s3 <hostname>:/<s3BucketName>/irods/Vault "S3_DEFAULT_HOSTNAME=s3.amazonaws.com;S3_AUTH_FILE=</full/path/to/AWS.keypair>;S3_RETRY_COUNT=<num reconn tries>;S3_WAIT_TIME_SECONDS=<wait between retries>;S3_PROTO=<HTTP|HTTPS>"
irods@hostname $ iadmin addchildtoresc compResc cacheResc cache
irods@hostname $ iadmin addchildtoresc compResc archiveResc archive
irods@hostname $ iput -R compResc foo.txt
irods@hostname $ ireg -R archiveResc /<s3BucketName>/full/path/in/bucket /full/logical/path/to/dataObject
```

Note the use of the `cache` and `archive` contextStrings on the `addchildtoresc` commands.  These inform the parent compound resource which child is serving in which role.  The S3 resource is set to `archive` and a POSIX-capable resource must be set to `cache`.

### Example of a baseline resource configuration
```
$ ilsresc s3resc
resource name: s3resc
id: 10017
zone: tempZone
type: s3
class: cache
location: irods3.example.org
vault: /s3-irods-bucket-name/prefix/in/bucket
free space:
free space time: : Never
status:
info:
comment:
create time: 01575329511: 2019-12-02.23:31:51
modify time: 01576687386: 2019-12-18.16:43:06
context: S3_DEFAULT_HOSTNAME=s3.us-east-1.amazonaws.com;S3_AUTH_FILE=/var/lib/irods/s3.keypair;S3_REGIONNAME=us-east-1;S3_RETRY_COUNT=1;S3_WAIT_TIME_SECONDS=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;HOST_MODE=cacheless_attached
parent:
parent context:
```

## Using this plugin with Google Cloud Storage (GCS)

This plugin has been manually tested to work with Google Cloud Storage, with some caveats.

1. GCS treats bucket names with dots as domain names.  These must be verified.  See [Domain-named Bucket Verification] (https://cloud.google.com/storage/docs/domain-name-verification).
2. If an object is uploaded using multipart uploads, subsequent calls to CopyObject fail.  CopyObject is used with `imv` when consistent naming is used.  As a workaround, GCS resources should be configured with any one of the following options:
-   Disable MPU upload: `S3_ENABLE_MPU=0`
-   Set archive naming policy to decoupled: `ARCHIVE_NAMING_POLICY=decoupled`
-   Disable CopyObject:  `S3_ENABLE_COPYOBJECT=0`

Make sure to:

-   Set `S3_DEFAULT_HOSTNAME=storage.googleapis.com`
-   Set `S3_PROTO=HTTPS`
-   Set 'S3_REGIONNAME` correctly - note that AWS has `us-east-1` and GCS has `us-east4` (without the second hyphen)
