'use strict'; // eslint-disable-line

const assert = require('assert');

const keygen = require('../../lib/keygen');

const bucketName = 'vogosphere';
const cos = new Buffer([0x70]).toString('hex').toUpperCase();
const namespace = 'poem';
const owner = 'jeltz';
const params = { bucketName, namespace, owner };
const sid = new Buffer([0x59]).toString('hex').toUpperCase();

describe('Key generation', () => {
    it('should only create valid keys', () => {
        const result = new Array(600).fill(0).map(() => {
            const key = keygen(params);
            assert.strictEqual(key.slice(30, 32), sid);
            assert.strictEqual(key.slice(38, 40), cos);
            return key.slice(16, 32);
        }).reduce((prev, current) => prev === (current || false));
        assert(result);
    });
});
