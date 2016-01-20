'use strict';

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');

const async = require('async');

const Sproxy = require('../../index');

const bucketName = 'aperture';
const chunkSize = new Sproxy().chunkSize;
const namespace = 'default';
const owner = 'glados';
const parameters = { bucketName, namespace, owner };
const reqUid = 'REQ1';
let upload = crypto.randomBytes(4);
let client;
let savedKeys;
let server;


function makeResponse(res, code, message, data) {
    res.statusCode = code;
    res.statusMessage = message;
    if (data) {
        res.write(data);
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
            server[key] = new Buffer(0);
            req.on('data', data => server[key] = Buffer
                   .concat([ server[key], data ]))
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
            makeResponse(res, 200, 'OK');
        }
    }
}

describe('Requesting Sproxyd', function tests() {
    before('initialize a new sproxyd client and fake server', done => {
        client = new Sproxy({ bootstrap: [ '127.0.0.1:9000' ] });
        assert.deepStrictEqual(client.bootstrap[0][0], '127.0.0.1');
        assert.deepStrictEqual(client.bootstrap[0][1], '9000');
        assert.deepStrictEqual(client.path, '/proxy/arc/');
        server = http.createServer(handler).listen(9000);
        done();
    });

    it('should put some data via sproxyd', done => {
        client.put(upload, parameters, reqUid, (err, keys) => {
            savedKeys = keys;
            done(err);
        });
    });

    it('should get some data via sproxyd', done => {
        client.get(savedKeys, reqUid, (err, data) => {
            if (err) { return done(err); }
            assert.deepStrictEqual(data, [ upload, ]);
            done();
        });
    });

    it('should delete some data via sproxyd', done => {
        client.delete(savedKeys, reqUid, done);
    });

    it('should fail getting non existing data', done => {
        client.get(savedKeys, reqUid, (err) => {
            const error = new Error(404);
            error.isExpected = true;
            assert.deepStrictEqual(err, error, 'Doesn\'t fail properly');
            done();
        });
    });

    it('should put some chunks of data via sproxyd', (done) => {
        upload = crypto.randomBytes(3 * chunkSize);
        client.put(upload, parameters, reqUid, (err, keys) => {
            savedKeys = keys;
            done(err);
        });
    });

    it('should get some data via sproxyd', done => {
        client.get(savedKeys, reqUid, (err, data) => {
            if (err) { return done(err); }
            data.forEach(chunk => assert.strictEqual(chunk.length, chunkSize));
            assert.strictEqual(data.length, 3);
            const tmp = Buffer.concat(data);
            assert.deepStrictEqual(upload, tmp);
            done();
        });
    });

    it('should delete some data via sproxyd', done => {
        client.delete(savedKeys, reqUid, done);
    });

    it('should fail getting any non existing data', done => {
        async.each(savedKeys, (key, next) => {
            client.get([ key ], reqUid, (err) => {
                const error = new Error(404);
                error.isExpected = true;
                assert.deepStrictEqual(err, error, 'Doesn\'t fail properly');
                next();
            });
        }, done);
    });
});
