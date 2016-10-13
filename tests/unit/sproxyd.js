'use strict'; // eslint-disable-line strict

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const stream = require('stream');

const Sproxy = require('../../index');

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
    if (!req.url.startsWith('/proxy/arc')) {
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
                server[key] = Buffer
                   .concat([server[key], data]);
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
        if (!server[key]) {
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

const client = new Sproxy({ bootstrap: ['127.0.0.1:9000'] });
assert.deepStrictEqual(client.bootstrap[0][0], '127.0.0.1');
assert.deepStrictEqual(client.bootstrap[0][1], '9000');
assert.deepStrictEqual(client.path, '/proxy/arc/');

describe('Create the server', () => {
    it('Listen', done => {
        server = http.createServer(handler).listen(9000);
        server.on('listening', () => {
            done();
        });
        server.on('error', err => {
            process.stdout.write(`${err.stack}\n`);
            process.exit(1);
        });
    });
});

crypto.getHashes().forEach(algo => {
    describe(`Requesting Sproxyd ${algo}`, () => {
        before('initialize a new sproxyd client and fake server', done => {
            parameters.algo = algo;
            done();
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
                assert.deepStrictEqual(err, error, 'Doesn\'t fail properly');
                done();
            });
        });

        it(`should put some data via sproxyd without ${algo}`, done => {
            const upStream = new stream.Readable;
            upStream.push(upload);
            upStream.push(null);
            client.put(upStream, upload.length, parameters, reqUid,
                       (err, key) => {
                           savedKey = key;
                           done(err);
                       });
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
