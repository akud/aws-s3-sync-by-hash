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

    await awsS3Sync(options);

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

    await awsS3Sync(options);

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

    await awsS3Sync(options);

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

    await awsS3Sync(Object.assign({ force: true }, options));

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

    await awsS3Sync(Object.assign({ delete: true }, options));

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

    await awsS3Sync(Object.assign({ delete: true }, options));

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


    await awsS3Sync(Object.assign({ delete: true }, options));

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
