'use strict';

const assert = require('assert');
const crypto = require('crypto');

const keygen = require('../../lib/keygen');

const bucketName = 'vogosphere';
const cos = new Buffer([ 0x70 ]).toString('hex').toUpperCase();
const namespace = 'poem';
const owner = 'jeltz';
const params = { bucketName, namespace, owner };
const sid = new Buffer([ 0x59 ]).toString('hex').toUpperCase();

describe('Key generation', () => {
    it('should only create valid keys', () => {
        const hashBucket = crypto.createHash('md5').update(bucketName).digest()
            .slice(0, 6).toString('hex').toUpperCase();
        const hashOwner = crypto.createHash('md5').update(owner).digest()
            .slice(3, 4).toString('hex').toUpperCase();
        for (let i = 0; i < 600; i++) {
            const key = keygen(params);
            assert.strictEqual(key.slice(30, 32), sid);
            assert.strictEqual(key.slice(38, 40), cos);
            assert.strictEqual(key.slice(18, 30), hashBucket);
            assert.strictEqual(key.slice(16, 18), hashOwner);
        }
    });
});
