'use strict';

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
const uploadMD5 = crypto.createHash('md5').update(upload).digest('hex');
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

    it('should fail to put some data with wrong md5 via sproxyd', done => {
        const upStream = new stream.Readable;
        upStream.push(upload);
        upStream.push(null);
        upStream.contentMD5 = 'invaliddigest';
        client.put(upStream, parameters, reqUid, err => {
            if (err === 'InvalidDigest') { return done(); }
            done(new Error('did not raise an error'));
        });
    });

    it('should put some data via sproxyd', done => {
        const upStream = new stream.Readable;
        upStream.push(upload);
        upStream.push(null);
        upStream.contentMD5 = uploadMD5;
        client.put(upStream, parameters, reqUid, (err, keys) => {
            savedKeys = keys;
            assert.strictEqual(upStream.calculatedMD5, uploadMD5);
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
});
