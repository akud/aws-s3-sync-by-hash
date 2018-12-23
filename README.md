# AWS S3 Sync

The standard aws cli command to sync a directory to s3, `aws s3 sync`,
determines changed files by their timestamps. If a local file has a
later timestamp than the one on s3, it will be uploaded. That can be
problematic when using a build tool for a static website that overwrites
files on every build. If you want to be able to only upload files that
have changed, you need to use the file's content hash.

This package provides a function to sync a directory, using the md5 hash
to determine files that need uploading.

Usage:

```javascript
const awsS3Sync = require('@akud/aws-s3-sync-by-hash');

awsS3Sync({
 accessKeyId: AWS_ACCESS_KEY_ID,
 secretAccessKey: AWS_SECRET_ACCESS_KEY,
 bucket: bucket to sync to [Required]
 root: root directory to sync from [Required]
 force: force upload even if hashes match [Default: false]
 delete: delete files from bucket not in root directory [Default false]
 maxAge: max age to set for cache-control, in seconds. [Default: 86400 (one day)]
 acl: ACL to set on uploaded objects. [Default 'private']
});
```

The function returns a promise.
