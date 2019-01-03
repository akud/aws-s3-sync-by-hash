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
 region: AWS_DEFAULT_REGION,
 sessionToken: AWS_SESSION_TOKEN,
 bucket: bucket to sync to [Required]
 root: root directory to sync from [Required]
 force: force upload even if hashes match [Default: false]
 delete: delete files from bucket not in root directory [Default false]
 maxAge: [function|number] max age to set for cache-control, in seconds.
    if a function is supplied, it will be called with the entry object,
    and the return value will be used to set a max age.
    [default: 86400 (one day)]
 acl: [function|string] ACL to set on uploaded objects. If a function
    is supplied, it will be called with an object representing the entry
    and the return value will be used to set an acl. [Default 'private']
});
```

The function returns a promise.

If you have `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` or `AWS_SESSION_TOKEN` in `process.env`, it is recommended you don't set those options at all and the environment variables will be used automagically. It is the recommended way to do it because if you don't set all the variables, e.g. only set `accessKeyId: AWS_ACCESS_KEY_ID` and `secretAccessKey: AWS_SECRET_ACCESS_KEY`, but don't set `sessionToken: AWS_SESSION_TOKEN`, you'll get a `forbidden` error.
