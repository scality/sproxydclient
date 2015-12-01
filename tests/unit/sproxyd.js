'use strict';

const assert = require('assert');
const Sproxy = require('../../index');
const opts = require('../../config.json');

let savedKeys;

describe('Requesting Sproxyd', function tests() {
    it('should initialize a new sproxyd client', (done) => {
        const client = new Sproxy({
            bootstrap: [ '127.0.0.1:8000' ]
        });
        assert.deepStrictEqual(client.bootstrap[0][0], '127.0.0.1');
        assert.deepStrictEqual(client.bootstrap[0][1], '8000');
        assert.deepStrictEqual(client.path, '/proxy/arc/');
        done();
    });

    it('should put some data via sproxyd', function putTest(done) {
        const client = new Sproxy();
        client.put(new Buffer('test'), (err, keys) => {
            savedKeys = keys;
            done(err);
        });
    });

    it('should get some data via sproxyd', done => {
        const client = new Sproxy();
        client.get(savedKeys, (err, data) => {
            if (err) { return done(err); }
            assert.deepStrictEqual(data, [ new Buffer('test'), ]);
            done();
        });
    });

    it('should delete some data via sproxyd', done => {
        const client = new Sproxy();
        client.delete(savedKeys, done);
    });

    it('should fail getting non existing data', done => {
        const client = new Sproxy();
        client.get(savedKeys, (err) => {
            assert.deepStrictEqual(err, new Error(404), 'Doesn\'t fail');
            done();
        });
    });
});
