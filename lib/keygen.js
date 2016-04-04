'use strict'; // eslint-disable-line

const crypto = require('crypto');

const cos = new Buffer([0x70]);
const sid = new Buffer([0x59]);

function createMd5(str, len) {
    return crypto.createHash('md5').update(str).digest().slice(0, len);
}

module.exports = function createKey(params) {
    const hashNamespace = createMd5(params.namespace, 2); // 16 bits
    const hashOwner = createMd5(params.owner, 3); // 24 bits
    const hashBucket = createMd5(params.bucketName, 4); // 32 bits
    const rand = crypto.randomBytes(11);
    const key = Buffer.concat([
        rand.slice(0, 8),
        new Buffer([
            hashNamespace[0],
            hashNamespace[1] ^ hashOwner[0],
            hashOwner[1],
            hashOwner[2] ^ hashBucket[0],
        ]),
        hashBucket.slice(1),
        sid,
        rand.slice(8, 11),
        cos,
    ]);
    return key.toString('hex').toUpperCase();
};
