'use strict'; // eslint-disable-line strict

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const stream = require('stream');

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
    }
}

const clientCustomPath =
    new Sproxy({ bootstrap: ['127.0.0.1:9001'], path: '/custom/path' });
clientAssert(clientCustomPath.bootstrap, clientCustomPath.path);

const clientNonImmutable = new Sproxy({ bootstrap: ['127.0.0.1:9000'] });
clientAssert(clientNonImmutable.bootstrap, clientNonImmutable.path);

const clientImmutable = new Sproxy({ bootstrap: ['127.0.0.1:9000'],
                                     immutable: true });
clientAssert(clientImmutable.bootstrap, clientImmutable.path);

describe('Sproxyd client', () => {
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
        clientNonImmutable.destroy();
        clientImmutable.destroy();
        server.close(done);
    });

    [false, true].forEach(immutable => {
        let client;
        describe(immutable ? 'immutable' : 'non-immutable', () => {
            before(() => {
                if (immutable) {
                    client = clientImmutable;
                    expectedRequestHeaders = {
                        'x-scal-replica-policy': 'immutable',
                    };
                } else {
                    client = clientNonImmutable;
                    notExpectedRequestHeaders = ['x-scal-replica-policy'];
                }
            });
            after(() => {
                expectedRequestHeaders = undefined;
                notExpectedRequestHeaders = undefined;
            });
            it('should put some data via sproxyd', done => {
                const upStream = new stream.Readable;
                upStream.push(upload);
                upStream.push(null);
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
                    const error = new Error(404);
                    error.isExpected = true;
                    error.code = 404;
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
        });
    });

    describe('Healthcheck', () => {
        it('Healthcheck should return 200 OK', done => {
            clientNonImmutable.healthcheck(null, (err, response) => {
                assert.strictEqual(err, null);
                assert.strictEqual(response.statusCode, 200);
                done();
            });
        });
    });
});
