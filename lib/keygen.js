'use strict'; // eslint-disable-line strict

const crypto = require('crypto');

const sid = Buffer.from([0x59]);
const replicaValue = 0;

function createMd5(str, len) {
    return crypto.createHash('md5')
        .update(str, 'binary').digest().slice(0, len);
}

module.exports = function createKey(cos, params) {
    const hashNamespace = createMd5(params.namespace, 2); // 16 bits
    const hashOwner = createMd5(params.owner, 3); // 24 bits
    const hashBucket = createMd5(params.bucketName, 4); // 32 bits
    const rand = crypto.randomBytes(11);

    // replicaValue is always zero but is added separately to show
    // it is distinct from the Cos, which is a single-digit number
    const cosBuffer = Buffer.from([(cos << 4) + replicaValue]);
    const key = Buffer.concat([
        rand.slice(0, 8),
        Buffer.from([
            hashNamespace[0],
            hashNamespace[1] ^ hashOwner[0],
            hashOwner[1],
            hashOwner[2] ^ hashBucket[0],
        ]),
        hashBucket.slice(1),
        sid,
        rand.slice(8, 11),
        cosBuffer,
    ]);
    return key.toString('hex').toUpperCase();
};
