
// eslint-disable-line strict

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const stream = require('stream');
const async = require('async');

const Sproxy = require('../../index');

const lockedObjectKey = 'locked-object000000011111111111111111111';
const bucketName = 'aperture';
const namespace = 'default';
const owner = 'glados';
const parameters = { bucketName, namespace, owner };
const reqUid = 'REQ1';
const upload = crypto.randomBytes(9000);
let savedKey;
let server;
const md = {};
let mdHex;
let expectedRequestHeaders;
let notExpectedRequestHeaders;

function clientAssert(bootstrap, sproxydPath) {
    assert.deepStrictEqual(bootstrap[0][0], '127.0.0.1');
    if (bootstrap[0][1] === '9000') {
        assert.strictEqual(sproxydPath, '/proxy/arc/');
    } else {
        assert.deepStrictEqual(bootstrap[0][1], '9001');
        assert.strictEqual(sproxydPath, '/custom/path');
    }
}

function generateMD() {
    return Buffer.from(crypto.randomBytes(32)).toString('hex');
}

function generateKey() {
    const tmp = crypto.createHash('md5').update(crypto.randomBytes(1024)
        .toString()).digest().slice(0, 10);
    const tmp2 = crypto.createHash('md5').update(crypto.randomBytes(1024)
        .toString()).digest().slice(0, 10);
    return Buffer.concat([tmp, tmp2]).toString('hex').toUpperCase();
}

function _batchDelKeys(n) {
    let iter = n;
    const list = { keys: [] };
    while (iter--) {
        list.keys.push(generateKey());
    }
    return list;
}

function makeResponse(res, code, message, data, md) {
    /* eslint-disable no-param-reassign */
    res.statusCode = code;
    res.statusMessage = message;
    /* eslint-enable no-param-reassign */
    if (data) {
        res.write(data);
    }
    if (md) {
        res.setHeader('x-scal-usermd', md);
    }
    res.end();
}

function handler(req, res) {
    const key = req.url.slice(-40);
    if (expectedRequestHeaders) {
        Object.keys(expectedRequestHeaders).forEach(header => {
            assert.strictEqual(req.headers[header],
                expectedRequestHeaders[header]);
        });
    }
    if (notExpectedRequestHeaders) {
        notExpectedRequestHeaders.forEach(header => {
            assert.strictEqual(req.headers[header], undefined);
        });
    }
    if (req.url === '/proxy/arc/.conf' && req.method === 'GET') {
        makeResponse(res, 200, 'OK');
    } else if (!req.url.startsWith('/proxy/arc')) {
        makeResponse(res, 404, 'NoSuchPath');
    } else if (req.method === 'PUT') {
        if (server[key]) {
            makeResponse(res, 404, 'AlreadyExists');
        } else {
            server[key] = Buffer.alloc(0);
            if (req.headers['x-scal-usermd']) {
                md[key] = req.headers['x-scal-usermd'];
            }
            req.on('data', data => {
                server[key] = Buffer.concat([server[key], data]);
            })
                .on('end', () => makeResponse(res, 200, 'OK'));
        }
    } else if (req.method === 'GET') {
        if (!server[key]) {
            makeResponse(res, 404, 'NoSuchPath');
        } else {
            makeResponse(res, 200, 'OK', server[key]);
        }
    } else if (req.method === 'DELETE') {
        if (key === lockedObjectKey) {
            makeResponse(res, 423, 'Locked');
        } else if (!server[key]) {
            makeResponse(res, 404, 'NoSuchPath');
        } else {
            delete server[key];
            if (md[key]) {
                delete md[key];
            }
            makeResponse(res, 200, 'OK');
        }
    } else if (req.method === 'HEAD') {
        if (server[key]) {
            makeResponse(res, 200, 'OK', null, md[key]);
        } else {
            makeResponse(res, 404, 'NoSuchPath');
        }
    } else if (req.method === 'POST') {
        makeResponse(res, 200);
    }
}

const clientCustomPath = new Sproxy({ bootstrap: ['127.0.0.1:9001'], path: '/custom/path' });
clientAssert(clientCustomPath.bootstrap, clientCustomPath.path);

const clientNonImmutable = new Sproxy({ bootstrap: ['127.0.0.1:9000'] });
clientAssert(clientNonImmutable.bootstrap, clientNonImmutable.path);

const clientNonImmutableWithFailover = new Sproxy({
    bootstrap: ['127.0.0.1:9001', '127.0.0.1:9000']
});

const clientImmutable = new Sproxy({
    bootstrap: ['127.0.0.1:9000'],
    immutable: true,
});
clientAssert(clientImmutable.bootstrap, clientImmutable.path);
const clientImmutableWithFailover = new Sproxy({
    bootstrap: ['127.0.0.1:9001', '127.0.0.1:9000'],
    immutable: true,
});

[
    [
        'Sproxyd client immutable',
        clientImmutable,
        { 'x-scal-replica-policy': 'immutable' },
        null,
        false,
    ],
    [
        'Sproxyd client non-immutable',
        clientNonImmutable,
        null,
       ['x-scal-replica-policy'],
        false,
    ],
    [
        'Sproxyd client immutable with failover',
        clientImmutableWithFailover,
        { 'x-scal-replica-policy': 'immutable' },
        null,
        true,
    ],
    [
        'Sproxyd client immutable with failover',
        clientNonImmutableWithFailover,
        null,
        ['x-scal-replica-policy'],
        true
    ],

].forEach(([msg, client, expectHeader, expectNonHeader, failover]) => {
    describe(msg, function () {
        this.timeout(5000);

        before('Create the server', done => {
            server = http.createServer(handler).listen(9000);
            server.on('listening', () => {
                done();
            });
            server.on('error', err => {
                process.stdout.write(`${err.stack}\n`);
                process.exit(1);
            });
        });

        after('Shutdown the server', done => {
            client.destroy();
            server.close(done);
        });

        beforeEach(() => {
            // force bootstrap order to failover test
            client.bootstrap = failover
                ? [ ['127.0.0.1', '9001'], ['127.0.0.1', '9000'] ]
                : [ ['127.0.0.1', '9000'] ];
            client.current = client.bootstrap[0];
            expectedRequestHeaders = expectHeader;
            notExpectedRequestHeaders = expectNonHeader;
        });

        afterEach(() => {
            expectedRequestHeaders = undefined;
            notExpectedRequestHeaders = undefined;
        });

        it('should put some data via sproxyd',done => {
            const upStream = new stream.PassThrough();
            upStream.write(upload);
            upStream.end();
            client.put(upStream, upload.length, parameters, reqUid,
                (err, key) => {
                    savedKey = key;
                    done(err);
                });
        });

        it('should get some data via sproxyd', done => {
            client.get(savedKey, undefined, reqUid, (err, stream) => {
                let ret = Buffer.alloc(0);
                if (err) {
                    done(err);
                } else {
                    stream.on('data', val => {
                        ret = Buffer.concat([ret, val]);
                    });
                    stream.on('end', () => {
                        assert.deepStrictEqual(ret, upload);
                        done();
                    });
                }
            });
        });

        it('should delete some data via sproxyd', done => {
            client.delete(savedKey, reqUid, done);
        });

        it('should fail getting non existing data', done => {
            client.get(savedKey, undefined, reqUid, err => {
                const error = new Error();
                error.isExpected = true;
                error.code = 404;
                error.retryable = true;
                assert.deepStrictEqual(err, error,
                    'Doesn\'t fail properly');
                done();
            });
        });

        it('should return success when deleting a locked object', done => {
            client.delete(lockedObjectKey, reqUid, done);
        });

        it('should put an empty object via sproxyd', done => {
            savedKey = generateKey();
            mdHex = generateMD();
            client.putEmptyObject(savedKey, mdHex, reqUid, err => {
                done(err);
            });
        });

        it('Should get the md of the object', done => {
            client.getHEAD(savedKey, reqUid, (err, data) => {
                assert.strictEqual(err, null);
                assert.strictEqual(data, mdHex);
                done();
            });
        });

        it('Get HEAD should return an error', done => {
            client.getHEAD(generateKey(), reqUid, err => {
                assert.notStrictEqual(err, null);
                assert.notStrictEqual(err, undefined);
                assert.strictEqual(err.code, 404);
                done();
            });
        });

        it('should return success for batch delete', done => {
            const list = _batchDelKeys(2000);
            client.batchDelete(list, reqUid, err => {
                assert.strictEqual(err, null);
                done();
            });
        });

        it('should abort an unfinished request', done => {
            const upStream = new stream.PassThrough();
            upStream.write(upload.slice(0, upload.length - 10));
            setTimeout(() => upStream.destroy(), 500);
            client.put(upStream, upload.length, parameters, reqUid,
                err => {
                    if (err) {
                        done();
                    } else {
                        assert.fail('expected an immediate error from sproxyd');
                    }
                });
        });
    });
});

describe('Sproxyd client', () => {
    const client = new Sproxy({ bootstrap: ['127.0.0.1:9000'] });
    clientAssert(client.bootstrap, client.path);

    before('Create the server', done => {
        server = http.createServer(handler).listen(9000);
        server.on('listening', () => {
            done();
        });
        server.on('error', err => {
            process.stdout.write(`${err.stack}\n`);
            process.exit(1);
        });
    });

    after('Shutdown the server', done => {
        client.destroy();
        server.close(done);
    });

    describe('Healthcheck', () => {
        it('Healthcheck should return 200 OK', done => {
            client.healthcheck(null, (err, response) => {
                assert.strictEqual(err, null);
                assert.strictEqual(response.statusCode, 200);
                done();
            });
        });
    });

    describe('Get request uid', () => {
        const uids = 'id1:id2:id3';
        const log = client.createLogger(uids);
        const ids = log.getUids();

        it('should return first request id without colon', () => {
            const firstRequestUid = client._getFirstReqUid(log);
            assert.notStrictEqual(firstRequestUid, undefined);
            assert.strictEqual(firstRequestUid.indexOf(':'), -1);
            assert.strictEqual(ids[0], firstRequestUid);
        });

        it('should return last request id without colon', () => {
            const lastRequestUid = client._getLastReqUid(log);
            assert.notStrictEqual(lastRequestUid, undefined);
            assert.strictEqual(lastRequestUid.indexOf(':'), -1);
            assert.strictEqual(ids.pop(), lastRequestUid);
        });
    });
});

describe('Sproxyd PUT error handling', function () {
    this.timeout(10000);

    const client = new Sproxy({
        bootstrap: ['127.0.0.1:9001', '127.0.0.1:9000']
    });
    let serverWithError;
    let server;

    before('Create the server', done => {
        client.bootstrap = [
            ['127.0.0.1', '9001'],
            ['127.0.0.1', '9000'],
        ];
        client.current = client.bootstrap[0];
        async.series([
            next => {
                serverWithError = http.createServer((req, res) => {
                    const key = req.url.slice(-40);
                    req.on('data', data => {
                        req.destroy();
                    });
                }).listen(9001);
                serverWithError.on('listening', () => {
                    next();
                });
                serverWithError.on('error', err => {
                    process.stdout.write(`${err.stack}\n`);
                    process.exit(1);
                });
            },
            next => {
                server = http.createServer(handler).listen(9000);
                server.on('listening', () => {
                    next();
                });
                server.on('error', err => {
                    process.stdout.write(`${err.stack}\n`);
                    process.exit(1);
                });
            },
        ], done);

    });

    after('Shutdown the server', done => {
        client.destroy();
        async.series([
            next => serverWithError.close(next),
            next => server.close(next),
        ], done);
    });

    it('should not failover for inflight PUT request',done => {
        const upStream = new stream.PassThrough();
        upStream.write(upload);
        client.put(upStream, upload.length, parameters, reqUid,
            err => {
                assert(err);
                assert.strictEqual(err.retryable, false);
                done();
            });
    });
});
