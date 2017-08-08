'use strict'; // eslint-disable-line strict

const assert = require('assert');
const http = require('http');
const werelogs = require('werelogs');

const shuffle = require('./shuffle');
const keygen = require('./keygen');

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, log, callback) {
    const request = http.request(req, response => {
        response.once('readable', () => {
            // Get range returns a 206
            // Concurrent deletes on sproxyd/immutable keys returns 423
            if (response.statusCode !== 200 && response.statusCode !== 206 &&
                !(response.statusCode === 423 && req.method === 'DELETE')) {
                const error = new Error();
                error.code = response.statusCode;
                error.isExpected = true;
                log.debug('got expected response code:',
                          { statusCode: response.statusCode });
                return callback(error);
            }
            return callback(null, response);
        });
    }).on('error', callback);

    // disable nagle algorithm
    request.setNoDelay(true);
    return request;
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
     * @param {number} [opts.chordCos] - cos coefficient when the chord
     *      driver is enabled, default to cos 2 for ARC (key XXX20)
     * @param {Boolean} [opts.immutable] - enable immutable header for
     *      all operations to allow optimization on immutable data
     *      (disabled by default). ONLY ENABLE IF OBJECTS ARE NEVER
     *      REWRITTEN!
     * @param {werelogs.API} [opts.logApi] - object providing a constructor
     *                                      function for the Logger object
     */
    constructor(opts) {
        const options = opts || {};
        this.bootstrap = opts.bootstrap === undefined ?
            [['localhost', '81']] : _parseBootstrapList(opts.bootstrap);
        this.bootstrap = shuffle(this.bootstrap);
        if (options.chordCos) {
            this.cos = options.chordCos;
            this.path = options.path || '/proxy/chord/';
        } else {
            this.cos = 0x2;
            this.path = options.path || '/proxy/arc/';
        }
        this.immutable = options.immutable || false;
        this.setCurrentBootstrap(this.bootstrap[0]);
        this.httpAgent = new http.Agent({ keepAlive: true });

        this.setupLogging(options.logApi);
    }

    /**
     * Destroy connections kept alive by the client
     *
     * @return {undefined}
     */
    destroy() {
        this.httpAgent.destroy();
    }

    /*
     * Create a dedicated logger for Sproxyd, from the provided werelogs API
     * instance.
     *
     * @param {werelogs.API} [logApi] - object providing a constructor function
     *                                for the Logger object
     * @return {undefined}
     */
    setupLogging(logApi) {
        this.logging = new (logApi || werelogs).Logger('SproxydClient');
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
    _createRequestHeader(method, headers, key, params, log) {
        const reqHeaders = headers || {};

        const currentBootstrap = this.getCurrentBootstrap();
        if (this.immutable) {
            reqHeaders['X-Scal-Replica-Policy'] = 'immutable';
        }
        reqHeaders['content-type'] = 'application/octet-stream';
        reqHeaders['X-Scal-Request-Uids'] = log.getSerializedUids();
        if (params && params.range) {
            /* eslint-disable dot-notation */
            reqHeaders['Range'] = `bytes=${params.range[0]}-${params.range[1]}`;
            /* eslint-enable dot-notation */
        }
        return {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method,
            path: `${this.path}${key}`,
            headers: reqHeaders,
            agent: this.httpAgent,
        };
    }

    _failover(method, stream, size, key, tries, log, callback, params) {
        const args = params === undefined ? {} : params;
        let counter = tries;
        log.info('sending request to sproxyd', { method, key, args, counter });

        let receivedResponse = false;
        this._handleRequest(method, stream, size, key, log, (err, ret) => {
            if (err && !err.isExpected) {
                if (receivedResponse === true) {
                    log.fatal('multiple responses from sproxyd, trying to ' +
                    'write more data to the stream after sproxyd sent a ' +
                    'response, size of the object could be incorrect', {
                        error: err,
                        method: '_failover',
                        size,
                        objectKey: key,
                    });
                    return undefined;
                }
                if (++counter >= this.bootstrap.length) {
                    log.errorEnd('failover tried too many times, giving up',
                                 { retries: counter });
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log)
                    ._failover(method, stream, size, key, counter, log,
                               callback, params);
            }
            receivedResponse = true;
            log.end('request received response', { error: err });
            return callback(err, ret);
        }, args);
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending.
     */
    _handleRequest(method, stream, size, key, log, callback, params) {
        const headers = params.headers ? params.headers : {};
        const newKey = key || keygen(this.cos, params);
        const req = this._createRequestHeader(method, headers, newKey, params,
                                              log);
        const host = this.getCurrentBootstrap();
        if (stream) {
            headers['content-length'] = size;
            const request = _createRequest(req, log, (err, response) => {
                if (err) {
                    log.error('putting chunk to sproxyd', { host, key: newKey,
                        error: err });
                    return callback(err);
                }
                // We return the key
                log.debug('stored to sproxyd', { host, key: newKey,
                    statusCode: response.statusCode });
                return callback(null, newKey);
            });
            request.on('finish',
                () => log.debug('finished sending PUT chunks to sproxyd', {
                    component: 'sproxydclient',
                    method: '_handleRequest',
                    contentLength: size,
                }));
            stream.pipe(request);
            stream.on('error', err => {
                log.error('error from readable stream', {
                    error: err,
                    method: '_handleRequest',
                    component: 'sproxydclient',
                });
                request.end();
            });
        } else {
            headers['content-length'] = 0;
            const request = _createRequest(req, log, (err, response) => {
                if (err) {
                    log.error('error sending sproxyd request', { host,
                        error: err, key: newKey, method: '_handleRequest' });
                    return callback(err);
                }
                log.debug('success sending sproxyd request', { host,
                    statusCode: response.statusCode, key: newKey,
                    method: '_handleRequest' });
                return callback(null, response);
            });
            request.end();
        }
    }

    /**
     * This sends a PUT request to sproxyd.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {string} stream.contentHash - hash of the data to send
     * @param {integer} size - size
     * @param {Object} params - parameters for key generation
     * @param {string} params.bucketName - name of the object's bucket
     * @param {string} params.owner - owner of the object
     * @param {string} params.namespace - namespace of the S3 request
     * @param {string} reqUids - The serialized request id
     * @param {SproxydClient~putCallback} callback - callback
     * @param {string} keyScheme - sproxyd key for put the metadata
     * @returns {undefined}
     */
    put(stream, size, params, reqUids, callback, keyScheme) {
        const log = this.createLogger(reqUids);
        this._failover('PUT', stream, size, keyScheme, 0, log, (err, key) => {
            if (err) {
                return callback(err);
            }
            return callback(null, key);
        }, params);
    }

    /**
     * This sends a PUT request to sproxyd without data.
     * @param {String} keyScheme - sproxyd key for put the metadata
     * @param {String}  metadata - metadata to put in the object
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~putCallback} callback - callback
     * @returns {undefined}
     */
    putEmptyObject(keyScheme, metadata, reqUids, callback) {
        const log = this.createLogger(reqUids);
        const params = { headers: {} };
        params.headers['x-scal-usermd'] = metadata;
        this._failover('PUT', null, 0, keyScheme, 0, log, (err, key) => {
            if (err) {
                return callback(err);
            }
            return callback(null, key);
        }, params);
    }

    /**
     * This sends a GET request to sproxyd.
     * @param {String} key - The key associated to the value
     * @param { Number [] | Undefined} range - range (if any) with
     *                                         first element the start
     * and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(key, range, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        assert.strictEqual(key.length, 40);
        const log = this.createLogger(reqUids);
        const params = { range };
        this._failover('GET', null, 0, key, 0, log, callback, params);
    }

    /**
     * This sends a HEAD request to sproxyd.
     * @param {String} key - The key to get from datastore
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~getCallback} callback - callback
     * @returns {undefined}
     */
    getHEAD(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        assert.strictEqual(key.length, 40);
        const log = this.createLogger(reqUids);
        this._failover('HEAD', null, 0, key, 0, log, (err, res) => {
            if (err) {
                return callback(err);
            }
            if (res.headers['x-scal-usermd']) {
                return callback(null, res.headers['x-scal-usermd']);
            }
            return callback();
        });
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
        this._failover('DELETE', null, 0, key, 0, log, callback);
    }

    /**
    * This sends a GET request with healthcheck path to sproxyd
    * @param {Object} log - The log from s3
    * @param {SproxydClient-healthcheckCallback} callback - callback
    * @returns {undefined}
    **/
    healthcheck(log, callback) {
        const logger = log || this.createLogger();
        const currentBootstrap = this.getCurrentBootstrap();
        const req = {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method: 'GET',
            path: `${this.path}.conf`,
            headers: {
                'X-Scal-Request-Uids': logger.getSerializedUids(),
            },
            agent: this.httpAgent,
        };
        const request = _createRequest(req, logger, callback);
        request.end();
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
