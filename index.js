const AWS = require('aws-sdk');
const es = require('event-stream');
const fs = require('fs');
const md5File = require('md5-file')
const mimetypes = require('mime-types');
const Readable = require('stream').Readable;
const readdirp = require('readdirp');


class Syncer {
  constructor(options) {
    options = Object.assign({
      acl: 'private',
      delete: false,
      force: false,
      maxAge: 86400,
    }, options);
    this.acl = options.acl;
    this.delete = options.delete;
    this.force = options.force;
    this.maxAge = options.maxAge;
    if (options.bucket) {
      this.bucket = options.bucket;
    } else {
      throw new Error('bucket is required');
    }
    if (options.root) {
      this.root = options.root;
    } else {
      throw new Error('root is required');
    }
    this.s3 = new AWS.S3({
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey,
      region: options.region,
    });
  }

  sync() {
    const uploadPromise = this.createUploadPromise();
    if (this.delete) {
      return uploadPromise.then(() => this.createDeletePromise());
    } else {
      return uploadPromise;
    }
  }

  createUploadPromise() {
    return new Promise((resolve, reject) => {
      readdirp({ root: this.root })
        .pipe(es.map(this.filterByHash.bind(this)))
        .pipe(es.map(this.uploadFile.bind(this)))
        .pipe(es.map((entry, callback) => {
          passThrough('Uploaded: ' + entry.Key + '\n', callback);
        }))
        .on('error', reject)
        .on('end', resolve)
        .pipe(process.stdout);
    });
  }

  createDeletePromise() {
    return new Promise((resolve, reject) => {
      this.streamAllKeys()
        .pipe(es.map(this.filterOutExistingFiles.bind(this)))
        .pipe(es.map(this.deleteKey.bind(this)))
        .pipe(es.map((entry, callback) => {
          passThrough('Deleted: ' + entry + '\n', callback);
        }))
        .on('error', reject)
        .on('end', resolve)
        .pipe(process.stdout);
    });
  }

  filterByHash(fsEntry, callback) {
    this.s3.headObject({
      Bucket: this.bucket,
      Key: fsEntry.path,
    }, (awsError, data) => {
      if (awsError && awsError.statusCode !== 404) {
        fail(awsError, callback);
      } else {
        const storedHash = data && data.Metadata.hash;
        md5File(fsEntry.fullPath, (hashError, hash) => {
          if (hashError) {
            fail(hashError, callback);
          } else if (this.force || storedHash !== hash) {
            const withHash = Object.assign({ hash: hash }, fsEntry);
            passThrough(withHash, callback);
          } else {
            exclude(fsEntry, callback);
          }
        });
      }
    })
  }

  uploadFile(fsEntryWithHash, callback) {
    this.s3.upload({
      ACL: this.acl,
      Bucket: this.bucket,
      Key: fsEntryWithHash.path,
      Body: fs.createReadStream(fsEntryWithHash.fullPath),
      CacheControl: 'max-age=' + this.maxAge,
      ContentType: mimetypes.lookup(fsEntryWithHash.path) || 'application/octet-stream',
      Metadata: {
        hash: fsEntryWithHash.hash,
      },
    }, (awsError, data) => {
      if (awsError) {
        fail(awsError, callback);
      } else {
        passThrough(data, callback);
      }
    });
  }

  streamAllKeys() {
    let stream;
    if (arguments[0]) {
      stream = arguments[0];
    } else {
      stream = new Readable();
      stream._read = function() {};
    }
    this.s3.listObjectsV2({
      Bucket: this.bucket,
      ContinuationToken: arguments[1],
    }, (awsError, data) => {
      if (awsError) {
        stream.emit('error', awsError);
      } else {
        data.Contents.forEach(function(entry) {
          stream.push(entry.Key.toString());
        });
        if (data.NextContinuationToken) {
          this.streamAllKeys(stream, data.NextContinuationToken);
        } else {
          stream.push(null);
        }
      }
    });
    return stream;
  }

  filterOutExistingFiles(key, callback) {
    key = key.toString();
    fs.access(this.root + '/' + key, (err) => {
      const fileExists = !err;
      if (fileExists) {
        exclude(key, callback);
      } else {
        passThrough(key, callback);
      }
    });
  }

  deleteKey(key, callback) {
    this.s3.deleteObject({
      Bucket: this.bucket,
      Key: key,
    }, function(awsError, data) {
      if (awsError) {
        fail(awsError, callback);
      } else {
        passThrough(key, callback);
      }
    });
  }
}

const passThrough = function(entry, callback) {
  callback(null, entry);
}

const fail = function(error, callback) {
  callback(error);
}

const exclude = function(entry, callback) {
  callback();
}

/**
 * options: {
 *   accessKeyId: AWS_ACCESS_KEY_ID,
 *   secretAccessKey: AWS_SECRET_ACCESS_KEY,
 *   bucket: bucket to sync to,
 *   root: root directory to sync from,
 *   force: force upload even if hashes match,
 *   delete: delete files from bucket that don't exist in root
 *   maxAge: max age to set for cache-control, in seconds. Defaults to 86400 (one day)
 *   acl: ACL to set on uploaded objects. Defaults to 'private'
 * }
 */
module.exports = function(options) {
  return new Syncer(options).sync();
}
