'use strict'; // eslint-disable-line strict

const assert = require('assert');

const keygen = require('../../lib/keygen');

const bucketName = 'vogosphere';
const cos = 0x7;
const cosAsStr = (cos << 4).toString(16).toUpperCase();
const namespace = 'poem';
const owner = 'jeltz';
const params = { bucketName, namespace, owner };
const sid = Buffer.from([0x59]).toString('hex').toUpperCase();

describe('Key generation', () => {
    it('should only create valid keys', () => {
        const result = new Array(600).fill(0).map(() => {
            const key = keygen(cos, params);
            assert.strictEqual(key.slice(30, 32), sid);
            assert.strictEqual(key.slice(38, 40), cosAsStr);
            return key.slice(16, 32);
        }).reduce((prev, current) => { // eslint-disable-line arrow-body-style
            return prev === current ? current : false;
        });
        assert(result);
    });
});
