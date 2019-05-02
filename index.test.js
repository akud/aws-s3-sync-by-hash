jest.disableAutomock();

const mockS3 = {
  deleteObject: jest.fn(),
  headObject: jest.fn(),
  listObjectsV2: jest.fn(),
  upload: jest.fn(),
};
const mockReadDirp = jest.fn();
const mockCreateReadStream = jest.fn();
const mockFileAccess = jest.fn();
const mockMd5File = jest.fn();

jest.mock('aws-sdk', () => {
  return {
    S3: function() { Object.assign(this, mockS3); },
  };
});
jest.mock('fs', () => ({
  access: mockFileAccess,
  createReadStream: mockCreateReadStream
}));
jest.mock('readdirp', () => mockReadDirp);
jest.mock('md5-file', () => mockMd5File);

const awsS3Sync = require('./index');
const es = require('event-stream');


describe('awsS3Sync', () => {
  let options;
  beforeEach(() => {
    options = {
      bucket: 'TEST-BUCKET',
      root: '/foo/bar',
    };

    mockS3.upload.mockReset();
    mockS3.headObject.mockReset();
    mockS3.deleteObject.mockReset();
    mockS3.listObjectsV2.mockReset();

    mockReadDirp.mockReset();

    mockFileAccess.mockReset();
    mockCreateReadStream.mockReset();

    mockMd5File.mockReset();
  });

  it('uploads files that do not exist in the target bucket', async () => {
    const mockBody = 'aslkdfjasdf';
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
    ]));
    mockS3.headObject.mockImplementation(callCallbackWithError(
      {statusCode: 404}
    ));
    mockMd5File.mockImplementation(callCallbackWithData(
      'deadbeef'
    ));
    mockCreateReadStream.mockReturnValue(mockBody);
    mockS3.upload.mockImplementation(callCallbackWithData(
      { Key: 'file1.json' }
    ));

    const result = await awsS3Sync(Object.assign(options, { maxAge: 2400 }));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file1.json'));
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: options.acl || 'private',
        Bucket: options.bucket,
        Key: 'file1.json',
        Body: mockBody,
        CacheControl: 'max-age=2400',
        ContentType: 'application/json',
        Metadata: {
          hash: 'deadbeef',
        },
      },
      expect.any(Function)
    );
    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: ['file1.json'],
      deletedFiles: []
    });
  });

  it('uploads files whose hash has changed', async () => {
    const mockBody = 'aslkdfjasdf';
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
    ]));
    mockS3.headObject.mockImplementation(callCallbackWithData(
      {
        Key: 'file1.json',
        Metadata: {
          hash: 'baddad',
        },
      }
    ));
    mockMd5File.mockImplementation(callCallbackWithData(
      'deadbeef'
    ));
    mockCreateReadStream.mockReturnValue(mockBody);
    mockS3.upload.mockImplementation(callCallbackWithData(
      { Key: 'file1.json' }
    ));

    const result = await awsS3Sync(options);

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file1.json'));
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: options.acl || 'private',
        Bucket: options.bucket,
        Key: 'file1.json',
        Body: mockBody,
        CacheControl: 'max-age=86400',
        ContentType: 'application/json',
        Metadata: {
          hash: 'deadbeef',
        },
      },
      expect.any(Function)
    );
    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: ['file1.json'],
      deletedFiles: []
    });
  });

  it('skips files that have the same hash', async () => {
    const mockBody = 'aslkdfjasdf';
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
    ]));
    mockS3.headObject.mockImplementation(callCallbackWithData(
      {
        Key: 'file1.json',
        Metadata: {
          hash: 'deadbeef',
        },
      }
    ));
    mockMd5File.mockImplementation(callCallbackWithData(
      'deadbeef'
    ));
    mockCreateReadStream.mockReturnValue(mockBody);
    mockS3.upload.mockImplementation(callCallbackWithData(
      { Key: 'file1.json' }
    ));

    const result = await awsS3Sync(options);

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockCreateReadStream).not.toHaveBeenCalled();
    expect(mockS3.upload).not.toHaveBeenCalled();
    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: [],
      deletedFiles: []
    });
  });

  it('uploads files with the same hash if force is specified', async () => {
    const mockBody = 'aslkdfjasdf';
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
    ]));
    mockS3.headObject.mockImplementation(callCallbackWithData(
      {
        Key: 'file1.json',
        Metadata: {
          hash: 'deadbeef',
        },
      }
    ));
    mockMd5File.mockImplementation(callCallbackWithData(
      'deadbeef'
    ));
    mockCreateReadStream.mockReturnValue(mockBody);
    mockS3.upload.mockImplementation(callCallbackWithData(
      { Key: 'file1.json' }
    ));

    const result = await awsS3Sync(Object.assign({ force: true }, options));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file1.json'));
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: options.acl || 'private',
        Bucket: options.bucket,
        Key: 'file1.json',
        Body: mockBody,
        CacheControl: 'max-age=86400',
        ContentType: 'application/json',
        Metadata: {
          hash: 'deadbeef',
        },
      },
      expect.any(Function)
    );
    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: ['file1.json'],
      deletedFiles: []
    });
  });

  it('calls the maxAge function to compute max age', async () => {
    const lastModified = new Date();
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
      fileObject('file2.json'),
    ]));
    mockS3.headObject.mockImplementationOnce(callCallbackWithError({ statusCode: 404 }));
    mockS3.headObject.mockImplementationOnce(callCallbackWithData({
      Key: 'file2.json',
      LastModified: lastModified,
      Metadata: {
        hash: 'oldhash',
      },
    }));
    mockMd5File.mockImplementationOnce(callCallbackWithData('deadbeef'));
    mockMd5File.mockImplementationOnce(callCallbackWithData('baddadfad'));
    mockCreateReadStream.mockImplementationOnce(() => 'body1');
    mockCreateReadStream.mockImplementationOnce(() => 'body2');
    mockS3.upload.mockImplementationOnce(callCallbackWithData({ Key: 'file1.json' }));
    mockS3.upload.mockImplementationOnce(callCallbackWithData({ Key: 'file2.json' }));

    const maxAgeProvider = jest.fn()
      .mockImplementationOnce(() => 1234)
      .mockImplementationOnce(() => 5678);

    const result = await awsS3Sync(Object.assign(options, { maxAge: maxAgeProvider }));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file2.json',
      },
      expect.any(Function)
    );

    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file2.json'), expect.any(Function));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file1.json'));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file2.json'));
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: options.acl || 'private',
        Bucket: options.bucket,
        Key: 'file1.json',
        Body: 'body1',
        CacheControl: 'max-age=1234',
        ContentType: 'application/json',
        Metadata: {
          hash: 'deadbeef',
        },
      },
      expect.any(Function)
    );
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: options.acl || 'private',
        Bucket: options.bucket,
        Key: 'file2.json',
        Body: 'body2',
        CacheControl: 'max-age=5678',
        ContentType: 'application/json',
        Metadata: {
          hash: 'baddadfad',
        },
      },
      expect.any(Function)
    );
    expect(maxAgeProvider).toHaveBeenCalledWith({
      fullPath: fullPath('file1.json'),
      hash: 'deadbeef',
      path: 'file1.json',
      s3Metadata: {
        hash: null,
        lastModified: null
      }
    });
    expect(maxAgeProvider).toHaveBeenCalledWith({
      fullPath: fullPath('file2.json'),
      hash: 'baddadfad',
      path: 'file2.json',
      s3Metadata: {
        hash: 'oldhash',
        lastModified: lastModified
      }
    });

    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: ['file1.json', 'file2.json'],
      deletedFiles: []
    });
  });

  it('calls the acl function to compute acl', async () => {
    const lastModified = new Date();
    mockReadDirp.mockReturnValue(es.readArray([
      fileObject('file1.json'),
      fileObject('file2.json'),
    ]));
    mockS3.headObject.mockImplementationOnce(callCallbackWithError({ statusCode: 404 }));
    mockS3.headObject.mockImplementationOnce(callCallbackWithData({
      Key: 'file2.json',
      LastModified: lastModified,
      Metadata: {
        hash: 'oldhash',
      },
    }));
    mockMd5File.mockImplementationOnce(callCallbackWithData('deadbeef'));
    mockMd5File.mockImplementationOnce(callCallbackWithData('baddadfad'));
    mockCreateReadStream.mockImplementationOnce(() => 'body1');
    mockCreateReadStream.mockImplementationOnce(() => 'body2');
    mockS3.upload.mockImplementationOnce(callCallbackWithData({ Key: 'file1.json' }));
    mockS3.upload.mockImplementationOnce(callCallbackWithData({ Key: 'file2.json' }));

    const aclProvider = jest.fn()
      .mockImplementationOnce(() => 'private')
      .mockImplementationOnce(() => 'public');

    const result = await awsS3Sync(Object.assign(options, { acl: aclProvider }));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file1.json',
      },
      expect.any(Function)
    );
    expect(mockS3.headObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'file2.json',
      },
      expect.any(Function)
    );

    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file1.json'), expect.any(Function));
    expect(mockMd5File).toHaveBeenCalledWith(fullPath('file2.json'), expect.any(Function));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file1.json'));
    expect(mockCreateReadStream).toHaveBeenCalledWith(fullPath('file2.json'));
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: 'private',
        Bucket: options.bucket,
        Key: 'file1.json',
        Body: 'body1',
        CacheControl: 'max-age=86400',
        ContentType: 'application/json',
        Metadata: {
          hash: 'deadbeef',
        },
      },
      expect.any(Function)
    );
    expect(mockS3.upload).toHaveBeenCalledWith(
      {
        ACL: 'public',
        Bucket: options.bucket,
        Key: 'file2.json',
        Body: 'body2',
        CacheControl: 'max-age=86400',
        ContentType: 'application/json',
        Metadata: {
          hash: 'baddadfad',
        },
      },
      expect.any(Function)
    );
    expect(aclProvider).toHaveBeenCalledWith({
      fullPath: fullPath('file1.json'),
      hash: 'deadbeef',
      path: 'file1.json',
      s3Metadata: {
        hash: null,
        lastModified: null
      }
    });
    expect(aclProvider).toHaveBeenCalledWith({
      fullPath: fullPath('file2.json'),
      hash: 'baddadfad',
      path: 'file2.json',
      s3Metadata: {
        hash: 'oldhash',
        lastModified: lastModified
      }
    });

    expectNoDelete();

    expect(result).toEqual({
      uploadedFiles: ['file1.json', 'file2.json'],
      deletedFiles: []
    });
  });


  it('deletes files that are not on disk when delete is specified', async () => {
    mockReadDirp.mockReturnValue(es.readArray([]));
    mockFileAccess.mockImplementation(callCallbackWithError());
    mockS3.listObjectsV2.mockImplementation(callCallbackWithData({
      Contents: [
        { Key: 'foo.json' },
      ]
    }));
    mockS3.deleteObject.mockImplementation(callCallbackWithData());

    const result = await awsS3Sync(Object.assign({ delete: true }, options));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).not.toHaveBeenCalled();
    expect(mockMd5File).not.toHaveBeenCalled();
    expect(mockCreateReadStream).not.toHaveBeenCalled();
    expect(mockS3.upload).not.toHaveBeenCalled();

    expect(mockS3.listObjectsV2).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        ContinuationToken: undefined,
      },
      expect.any(Function)
    );

    expect(mockS3.deleteObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'foo.json',
      },
      expect.any(Function)
    );

    expect(result).toEqual({
      uploadedFiles: [],
      deletedFiles: ['foo.json']
    });
  });

  it('does not delete files that are on disk when delete is specified', async () => {
    mockReadDirp.mockReturnValue(es.readArray([]));
    mockFileAccess.mockImplementation(callCallbackWithData());
    mockS3.listObjectsV2.mockImplementation(callCallbackWithData({
      Contents: [
        { Key: 'foo.json' },
      ]
    }));
    mockS3.deleteObject.mockImplementation(callCallbackWithData());

    const result = await awsS3Sync(Object.assign({ delete: true }, options));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).not.toHaveBeenCalled();
    expect(mockMd5File).not.toHaveBeenCalled();
    expect(mockCreateReadStream).not.toHaveBeenCalled();
    expect(mockS3.upload).not.toHaveBeenCalled();

    expect(mockS3.listObjectsV2).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        ContinuationToken: undefined,
      },
      expect.any(Function)
    );

    expect(mockS3.deleteObject).not.toHaveBeenCalled();

    expect(result).toEqual({
      uploadedFiles: [],
      deletedFiles: []
    });
  });

  it('pages through s3 listObjectsV2 results to get all keys', async () => {
    mockReadDirp.mockReturnValue(es.readArray([]));
    mockFileAccess.mockImplementation(callCallbackWithError());
    mockS3.listObjectsV2.mockImplementationOnce(
      callCallbackWithData({
        Contents: [ { Key: 'foo.json' }, { Key: 'bar.json' }],
        NextContinuationToken: 'foobar'
      })
    );
    mockS3.listObjectsV2.mockImplementationOnce(
      callCallbackWithData({
        Contents: [ { Key: 'baz.json' }, ]
      })
    );
    mockS3.deleteObject.mockImplementation(callCallbackWithData());


    const result = await awsS3Sync(Object.assign({ delete: true }, options));

    expect(mockReadDirp).toHaveBeenCalledWith({ root: options.root });
    expect(mockS3.headObject).not.toHaveBeenCalled();
    expect(mockMd5File).not.toHaveBeenCalled();
    expect(mockCreateReadStream).not.toHaveBeenCalled();
    expect(mockS3.upload).not.toHaveBeenCalled();

    expect(mockS3.listObjectsV2).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        ContinuationToken: undefined,
      },
      expect.any(Function)
    );
    expect(mockS3.listObjectsV2).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        ContinuationToken: 'foobar',
      },
      expect.any(Function)
    );

    expect(mockS3.deleteObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'foo.json',
      },
      expect.any(Function)
    );
    expect(mockS3.deleteObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'bar.json',
      },
      expect.any(Function)
    );
    expect(mockS3.deleteObject).toHaveBeenCalledWith(
      {
        Bucket: options.bucket,
        Key: 'baz.json',
      },
      expect.any(Function)
    );

    expect(result).toEqual({
      uploadedFiles: [],
      deletedFiles: ['foo.json', 'bar.json', 'baz.json']
    });
  });

 const fileObject = path => ({
    path: path,
    fullPath: fullPath(path),
  });
  const fullPath = path => options.root + '/' + path;
  const callCallbackWithError = error => (params, callback) => callback(error || {}, null);
  const callCallbackWithData = data => (params, callback) => callback(null, data);
  const expectNoDelete = () => {
    expect(mockS3.listObjectsV2).not.toHaveBeenCalled();
    expect(mockS3.deleteObject).not.toHaveBeenCalled();
    expect(mockFileAccess).not.toHaveBeenCalled();
  };
});
