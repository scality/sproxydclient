'use strict';

const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const PassThrough = require('stream').PassThrough;

const async = require('async');
const bunyanLogstash = require('bunyan-logstash');
const Logger = require('werelogs');

const shuffle = require('./shuffle');
const keygen = require('./keygen');

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, log, callback) {
    return http.request(req, function handleResponse(response) {
        let body = new Buffer(0);
        response.on('data', data => body = Buffer.concat([ body, data, ]))
            .on('end', () => {
                if (response.statusCode !== 200) {
                    const error = new Error(response.statusCode);
                    error.isExpected = true;
                    log.debug(`got expected response code ` +
                              ` ${response.statusCode}`);
                    return callback(error);
                }
                callback(null, body);
            })
            .on('error', callback);
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
        if (opts === undefined) {
            opts = {};
        }
        this.bootstrap = opts.bootstrap === undefined ?
            [ [ 'connectora.ringr2.devsca.com', '81'] ]
            : _parseBootstrapList(opts.bootstrap);
        this.bootstrap = shuffle(this.bootstrap);
        this.path = opts.path === undefined ?
            '/proxy/arc/' : opts.path;
        this.chunkSize = 4 * 1024 * 1024; // 4Mb
        this.setCurrentBootstrap(this.bootstrap[0]);
        this.httpAgent = new http.Agent({ keepAlive: true });

        this.setupLogging(opts.log);
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

    _failover(method, stream, keys, tries, log, callback, params) {
        const argsStr = params === undefined ? '{}' : JSON.stringify(params);
        const valStr = method === keys ? keys.join(',') : '[data]';
        let counter = tries;

        log.debug(`failover request method=${method} ` +
                  `keysOrValue=${valStr} params=${argsStr} try=${counter}`);

        this._handleRequest(method, stream, keys, log, (err, ret) => {
            if (err && !err.isExpected) {
                if (++counter >= this.bootstrap.length) {
                    log.error(`failover tried ${counter} times, giving up`);
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log)
                    ._failover(method, stream, keys, counter, log, callback,
                               params);
            }
            log.debug(`failover request received response err=${err}`);
            return callback(err, ret);
        }, params);
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending. async.map() allows us to generate the
     * Array that is sent back with the callback in case of success.
     */
    _handleRequest(method, stream, keys, log, callback, params) {
        if (stream) {
            const hash = crypto.createHash('md5').setEncoding('hex');
            const pass = new PassThrough;
            const req = this._createRequestHeader(method, null, params);
            const request = _createRequest(req, log, (err) => {
                if (err) {
                    log.error(`PUT chunk to sproxyd: ${err.message}`);
                    return callback(err);
                }
                // We return the key from the path
                const key = req.path.split('/')[3];
                log.debug('stored to sproxyd', { key });
                return callback(null, [ key ]);
            });
            pass.on('end', () => {
                hash.end();
                stream.calculatedMD5 = hash.read();
            });
            stream.pipe(pass).pipe(hash);
            stream.pipe(request);
            request.on('end', () => request.end);
            log.debug(`finished sending PUT chunks to sproxyd`);
        } else {
            async.map(keys, function handle(key, cb) {
                const req = this._createRequestHeader(method, key);
                const request = _createRequest(req, log, cb);
                request.end();
            }.bind(this), callback);
        }
    }

    /**
     * This sends a PUT request to sproxyd.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {Object} params - parameters for key generation
     * @param {String} params.bucketName - name of the object's bucket
     * @param {String} params.owner - owner of the object
     * @param {String} params.namespace - namespace of the S3 request
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~putCallback} callback
     */
    put(stream, params, reqUids, callback) {
        assert(stream.readable, 'stream should be readable');
        const log = this.createLogger(reqUids);
        this._failover('PUT', stream, null, 0, log, (err, key) => {
            if (err)
                return callback(err);
            if (stream.contentMD5
                && stream.calculatedMD5 !== stream.contentMD5) {
                log.error('md5s do not match', {
                    sentMD5: stream.contentMD5,
                    computedMD5: stream.calculatedMD5,
                });
                this.delete(key, reqUids, () => {});
                return callback('InvalidDigest');
            }
            return callback(null, key);
        }, params);
    }

    /**
     * This sends a GET request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~getCallback} callback
     */
    get(keys, reqUids, callback) {
        assert(keys instanceof Array, 'keys should be an array');
        assert(keys.length, 'keys should not be empty');
        keys.forEach(key => assert(typeof key === 'string'
                     && key.length === 40, 'wrong key format'));
        const log = this.createLogger(reqUids);
        this._failover('GET', null, keys, 0, log, callback);
    }

    /**
     * This sends a DELETE request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~deleteCallback} callback
     */
    delete(keys, reqUids, callback) {
        assert(keys instanceof Array, 'key should be an array');
        assert(keys.length, 'keys should not be empty');
        keys.forEach(key => assert(typeof key === 'string'
                     && key.length === 40, 'wrong key format'));
        const log = this.createLogger(reqUids);
        this._failover('DELETE', null, keys, 0, log, callback);
    }
}

/**
 * @callback SproxydClient~putCallback
 * @param {Error} - The encountered error
 * @param {String[]} keys - The array of keys to access the data
 */

/**
 * @callback SproxydClient~getCallback
 * @param {Error} - The encountered error
 * @param {Buffer[]} values - The array of values fetched
 */

/**
 * @callback SproxydClient~deleteCallback
 * @param {Error} - The encountered error
 */

module.exports = SproxydClient;
