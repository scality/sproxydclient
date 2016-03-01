'use strict';

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const PassThrough = require('stream').PassThrough;

const bunyanLogstash = require('bunyan-logstash');
const Logger = require('werelogs');

const shuffle = require('./shuffle');
const keygen = require('./keygen');

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, log, callback) {
    return http.request(req, function handleResponse(response) {
        response.once('readable', () => {
            if (response.statusCode !== 200) {
                const error = new Error(response.statusCode);
                error.isExpected = true;
                log.debug('got expected response code:',
                          { statusCode: response.statusCode });
                return callback(error);
            }
            return callback(null, response);
        });
    }).on('error', callback);
}

/*
 * This parses an array of strings representing our bootstrap list of
 * the following form: [ 'hostname:port', ... , 'hostname.port' ]
 * into an array of [hostname, port] arrays.
 * Since the bootstrap format may change in the future, having this
 * contained in a separate function will make things easier to
 * maintain.
 */
function _parseBootstrapList(list) {
    return list.map(value => value.split(':'));
}

class SproxydClient {
    /**
     * This represent our interface with the sproxyd server.
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {string[]} [opts.bootstrap] - list of sproxyd servers,
     *      of the form 'hostname:port'
     * @param {Object} [opts.path] - sproxyd base path
     */
    constructor(opts) {
        const options = opts ? opts : {};
        this.bootstrap = options.bootstrap === undefined ?
            [ [ 'connectora.ringr2.devsca.com', '81'] ]
            : _parseBootstrapList(options.bootstrap);
        this.bootstrap = shuffle(this.bootstrap);
        this.path = options.path === undefined ?
            '/proxy/arc/' : options.path;
        this.setCurrentBootstrap(this.bootstrap[0]);
        this.httpAgent = new http.Agent({ keepAlive: true });

        this.setupLogging(options.log);
    }

    setupLogging(config) {
        let options = undefined;
        if (config !== undefined) {
            options = {
                level: config.logLevel,
                dump: config.dumpLevel,
                streams: [
                    { stream: process.stdout },
                    {
                        type: 'raw',
                        stream: bunyanLogstash.createStream({
                            host: config.logstash.host,
                            port: config.logstash.port,
                        }),
                    }
                ],
            };
        }
        this.logging = new Logger('SproxydClient', options);
    }

    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    _shiftCurrentBootstrapToEnd(log) {
        const previousEntry = this.bootstrap.shift();
        this.bootstrap.push(previousEntry);
        const newEntry = this.bootstrap[0];
        this.setCurrentBootstrap(newEntry);

        log.debug(`bootstrap head moved from ${previousEntry} to ${newEntry}`);
        return this;
    }

    setCurrentBootstrap(host) {
        this.current = host;
        return this;
    }

    getCurrentBootstrap() {
        return this.current;
    }

    /*
     * This returns an array of indexes for chunking the output in pieces.
     */
    _getIndexes(value) {
        const indexes = [];
        for (let i = 0; i < value.length; i += this.chunkSize) {
            indexes.push(i);
        }
        return indexes;
    }

    /*
     * This creates a default request for sproxyd, generating
     * a new key on the fly if needed.
     */
    _createRequestHeader(method, key, params) {
        const currentKey = key ? key : keygen(params);
        const currentBootstrap = this.getCurrentBootstrap();
        return {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method,
            path: `${this.path}${currentKey}`,
            headers: {
                'X-Scal-Replica-Policy': 'immutable',
                'content-type': 'application/x-www-form-urlencoded',
            },
            agent: this.httpAgent,
        };
    }

    _failover(method, stream, key, tries, log, callback, params) {
        const args = params === undefined ? {} : params;
        const value = key ? key : '[data]';
        let counter = tries;

        log.info('request', { method, value, args, counter });

        this._handleRequest(method, stream, key, log, (err, ret) => {
            if (err && !err.isExpected) {
                if (++counter >= this.bootstrap.length) {
                    log.errorEnd('failover tried too many times, giving up',
                                 { retries: counter });
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log)
                    ._failover(method, stream, key, counter, log, callback,
                               params);
            }
            log.end('request received response', { err });
            return callback(err, ret);
        }, params);
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending.
     */
    _handleRequest(method, stream, key, log, callback, params) {
        if (stream) {
            const hashAlgo = params.algo ? params.algo : 'MD5';
            const hash = crypto.createHash(hashAlgo).setEncoding('hex');
            const pass = new PassThrough;
            const req = this._createRequestHeader(method, null, params);
            const request = _createRequest(req, log, (err) => {
                if (err) {
                    log.error('PUT chunk to sproxyd', { msg: err.message });
                    return callback(err);
                }
                // We return the key from the path
                const newKey = req.path.split('/')[3];
                log.debug('stored to sproxyd', { newKey });
                return callback(null, newKey);
            });
            pass.on('end', () => {
                hash.end();
                stream.calculatedHash = hash.read();
            });
            stream.pipe(pass).pipe(hash);
            stream.pipe(request);
            request.on('end', () => request.end);
            log.debug('finished sending PUT chunks to sproxyd');
        } else {
            const req = this._createRequestHeader(method, key);
            const request = _createRequest(req, log, callback);
            request.end();
        }
    }

    /**
     * This sends a PUT request to sproxyd.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {string} stream.contentHash - hash of the data to send
     * @param {Object} params - parameters for key generation
     * @param {String} params.bucketName - name of the object's bucket
     * @param {String} params.owner - owner of the object
     * @param {String} params.namespace - namespace of the S3 request
     * @param {String} params.algo - algorithm for the hash 
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~putCallback} callback - callback
     * @returns {undefined}
     */
    put(stream, params, reqUids, callback) {
        assert(stream.readable, 'stream should be readable');
        const log = this.createLogger(reqUids);
        this._failover('PUT', stream, null, 0, log, (err, key) => {
            if (err)
                return callback(err);
            if (stream.contentHash
                && stream.calculatedHash !== stream.contentHash) {
                log.error('hashes do not match', {
                    sentHash: stream.contentHash,
                    computedHash: stream.calculatedHash,
                });
                this.delete(key, reqUids, () => {});
                return callback('InvalidDigest');
            }
            return callback(null, key);
        }, params);
    }

    /**
     * This sends a GET request to sproxyd.
     * @param {String} key - The key associated to the value
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        assert.strictEqual(key.length, 40);
        const log = this.createLogger(reqUids);
        this._failover('GET', null, key, 0, log, callback);
    }

    /**
     * This sends a DELETE request to sproxyd.
     * @param {String} key - The key associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        assert.strictEqual(key.length, 40);
        const log = this.createLogger(reqUids);
        this._failover('DELETE', null, key, 0, log, callback);
    }
}

/**
 * @callback SproxydClient~putCallback
 * @param {Error} - The encountered error
 * @param {String} key - The key to access the data
 */

/**
 * @callback SproxydClient~getCallback
 * @param {Error} - The encountered error
 * @param {stream.Readable} stream - The stream of values fetched
 */

/**
 * @callback SproxydClient~deleteCallback
 * @param {Error} - The encountered error
 */

module.exports = SproxydClient;
