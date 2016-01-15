'use strict';

const crypto = require('crypto');

const cos = new Buffer([ 0x70 ]);
const sid = new Buffer([ 0x59 ]);

function createMd5(str, len) {
    return crypto.createHash('md5').update(str).digest().slice(0, len);
}

module.exports = function createKey(params) {
    const hashNamespace = createMd5(params.namespace, 2); // 16 bits
    const hashOwner = createMd5(params.owner, 4); // 32 bits
    const hashBucket = createMd5(params.bucketName, 6); // 48 bits
    const rand = crypto.randomBytes(11);
    const key = Buffer.concat([
        rand.slice(0, 3),
        hashNamespace,
        hashOwner,
        hashBucket,
        sid,
        rand.slice(8, 11),
        cos
    ], 20);
    const part = key.slice(3, 8);
    for (let i = 0; i < 5; i++) {
        part[i] ^= key[i];
    }
    return key.toString('hex').toUpperCase();
};
