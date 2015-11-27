'use strict';

const assert = require('assert');
const async = require('async');
const Sproxy = require('../../index');

let savedKeys;

describe('Requesting Sproxyd', function tests() {
    it('should initialize a new sproxyd client', (done) => {
        let client = new Sproxy({
            hostname: '127.0.0.1'
        });
        assert.deepStrictEqual(client.opts.hostname, '127.0.0.1');
        assert.deepStrictEqual(client.opts.port, 81);
        assert.deepStrictEqual(client.opts.path, '/proxy/arc/');
        done();
    });

    it('should put some data via sproxyd', done => {
        // The put operation has sometimes a lot of latency
        this.timeout(0);
        let client = new Sproxy();
        client.put(new Buffer('test'), (err, keys) => {
            savedKeys = keys;
            done(err);
        });
    });

    it('should get some data via sproxyd', done => {
        let client = new Sproxy();
        client.get(savedKeys, (err, data) => {
            if (err) { return done(err); }
            assert.deepStrictEqual(data, [ new Buffer('test'), ]);
            done();
        });
    });

    it('should delete some data via sproxyd', done => {
        let client = new Sproxy();
        client.delete(savedKeys, done);
    });

    it('should fail getting non existing data', done => {
        let client = new Sproxy();
        client.get(savedKeys, (err, data) => {
            assert.deepStrictEqual(err, new Error(404), 'Doesn\'t fail');
            done();
        });
    });
});
