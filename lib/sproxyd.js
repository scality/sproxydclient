
// eslint-disable-line strict
const async = require('async');
const assert = require('assert');
const http = require('http');
const { http: httpAgent } = require('httpagent');
const { finished } = require('stream');
const werelogs = require('werelogs');

const shuffle = require('./shuffle');
const keygen = require('./keygen');

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, log, callback) {
    let callbackCalled = false;
    const request = http.request(req, response => {
        callbackCalled = true;
        // Consume the response body first when not relevant for the
        // request type, i.e. not a GET
        if (req.method !== 'GET') {
            response.resume();
        }
        // Get range returns a 206
        // Concurrent deletes on sproxyd/immutable keys returns 423
        if (response.statusCode !== 200 && response.statusCode !== 206
            && !(response.statusCode === 423 && req.method === 'DELETE')) {
            const error = new Error();
            error.code = response.statusCode;
            error.isExpected = true;
            log.debug('got expected response code:',
                { statusCode: response.statusCode });
            return callback(error);
        }
        return callback(null, response);
    }).on('error', err => {
        if (!callbackCalled) {
            callbackCalled = true;
            return callback(err);
        }
        if (err.code !== 'ERR_SOCKET_TIMEOUT') {
            log.error('got socket error after response', { err });
        }
    });

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
        this.bootstrap = opts.bootstrap === undefined
            ? [['localhost', '81']] : _parseBootstrapList(opts.bootstrap);
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
        this.httpAgent = new httpAgent.Agent({
            freeSocketTimeout: 60 * 1000,
            timeout: 2 * 60 * 1000,
        });

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
        return reqUids
            ? this.logging.newRequestLoggerFromSerializedUids(reqUids)
            : this.logging.newRequestLogger();
    }

    _shiftCurrentBootstrapToEnd(log, failedBootstrap) {
        const currentBootstrap = this.getCurrentBootstrap();
        if (currentBootstrap[0] !== failedBootstrap[0]
            || currentBootstrap[1] !== failedBootstrap[1]) {
            log.debug(`bootstrap list has already been shifted. skipping`);
            return this;
        }

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

    /**
     * Returns the first id from the array of request ids.
     * @param {Object} log - log from s3
     * @returns {String} - first request id
     */
    _getFirstReqUid(log) {
        let reqUids = [];

        if (log) {
            reqUids = log.getUids();
        }

        return reqUids[0];
    }

    /**
     * Returns the last id from the array of request ids.
     * @param {Object} log - log from s3
     * @returns {String} - last request id
     */
    _getLastReqUid(log) {
        let reqUids = [];

        if (log) {
            reqUids = log.getUids();
        }

        return reqUids.pop();
    }

    /*
     * This creates a default request for sproxyd, generating
     * a new key on the fly if needed.
     */
    _createRequestHeader(method, headers, key, params, log) {
        const reqHeaders = headers || {};

        const currentBootstrap = this.getCurrentBootstrap();
        const reqUids = this._getFirstReqUid(log);
        if (this.immutable) {
            reqHeaders['X-Scal-Replica-Policy'] = 'immutable';
        }
        reqHeaders['content-type'] = (key === '.batch_delete')
            ? 'application/json' : 'application/octet-stream';
        reqHeaders['X-Scal-Request-Uids'] = reqUids;
        reqHeaders['X-Scal-Trace-Ids'] = reqUids;
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

    _failover(method, stream, size, key, tries, log, callback, params,
        payload) {
        const args = params === undefined ? {} : params;
        let counter = tries;
        log.debug('sending request to sproxyd', {
            method, key, args, counter,
        });

        let receivedResponse = false;
        // keep track of the current endpoint prior to request.
        // this is to ensure that the bootstrap shifting is only performed
        // when current endpoint matches the endpoint of the failed request
        const currentBootstrap = this.getCurrentBootstrap();

        this._handleRequest(method, stream, size, key, log, (err, ret) => {
            if (err && !err.isExpected) {
                if (receivedResponse === true) {
                    log.fatal('multiple responses from sproxyd, trying to '
                    + 'write more data to the stream after sproxyd sent a '
                    + 'response, size of the object could be incorrect', {
                        error: err,
                        method: '_failover',
                        size,
                        objectKey: key,
                    });
                    return undefined;
                }

                if (!err.retryable) {
                    log.errorEnd('Non-retryable error occured: '
                        + 'Skipping failover', {
                            error: err,
                            method: '_failover',
                            size,
                            objectKey: key,
                        });
                    return callback(err);
                }

                if (++counter >= this.bootstrap.length) {
                    log.errorEnd('failover tried too many times, giving up',
                        { retries: counter });
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log, currentBootstrap)
                    ._failover(method, stream, size, key, counter, log,
                        callback, params, payload);
            }
            receivedResponse = true;
            log.end().debug('request received response');
            return callback(err, ret);
        }, args, payload);
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending.
     */
    _handleRequest(method, stream, size, key, log, callback, params, payload) {
        const headers = params.headers ? params.headers : {};
        const host = this.getCurrentBootstrap();
        const isBatchDelete = key === '.batch_delete';
        const newKey = key || keygen(this.cos, params);
        const req = this._createRequestHeader(method, headers, newKey, params, log);
        log.addDefaultFields({
            component: 'sproxydclient',
            method: '_handleRequest',
            host,
            key: newKey,
            contentLength: size,
        });

        if (stream) {
            let streamingStarted = false;
            let voluntaryAbort = false;
            headers['content-length'] = size;

            const request = _createRequest(req, log, (err, response) => {
                if (err) {
                    if (streamingStarted || voluntaryAbort) {
                        err.retryable = false;
                    } else {
                        err.retryable = true;
                    }
                    if (!voluntaryAbort) {
                        log.error('putting chunk to sproxyd', {error: err});
                    }
                    return callback(err);
                }
                // We return the key
                log.debug('stored to sproxyd', {
                    statusCode: response.statusCode,
                });
                return callback(null, newKey);
            });
    
            const startPayloadStreaming = () => {
                // Once we start piping the stream, it starts being consumed and
                // we can't replay it (in the current implementation)
                streamingStarted = true;
                stream.pipe(request);
                finished(stream, err => {
                    if (err) {
                        log.trace('readable stream aborted');
                        request.abort();
                        voluntaryAbort = true;
                    } else {
                        log.trace('readable stream finished normally');
                    }
                });
            };

            // We have two goals:
            // - Ensure we properly handle connection reuse: 'connect' event
            //   won't happen for every request.
            // - Be able to retry (see _failover) the request when we can't
            //   connect to sproxyd. For this, we must only start consuming the
            //   stream when a socket is available: either on 'connect' or when
            //   reusing a socket. If we get a failure after starting to send
            //   the data, this implementation cannot retry.
            //
            // There is a possible race when an open socket is reused, but
            // closed by the server for inactivity before we can use it. In this
            // case, the current implementation cannot retry and will return a
            // 50x. This can be avoided by ensuring servers have a longer HTTP
            // keepalive time than clients.
            request.on('socket', (socket) => {
                // We can start streaming when reusing a socket
                if (request.reusedSocket) {
                    log.trace('reusing existing socket');
                    startPayloadStreaming();
                } else {
                    // Otherwise, wait for a successful connection
                    socket.on('connect', () => {
                        log.trace('using a new socket');
                        startPayloadStreaming();
                    });
                }
            });

            request.on('finish', () => {
                log.debug('finished sending PUT chunks to sproxyd');
            });

            stream.on('error', err => {
                log.error('error from readable stream');
            });
        } else {
            headers['content-length'] = isBatchDelete ? size : 0;
            const contentType = headers['content-type'];
            headers['content-type'] = isBatchDelete ? 'application/json'
                : contentType;
            const request = _createRequest(req, log, (err, response) => {
                if (err) {
                    // non-streaming are always retryable;
                    err.retryable = true;
                    log.error('error sending sproxyd request', {error: err});
                    return callback(err);
                }
                log.debug('success sending sproxyd request', {
                    statusCode: response.statusCode,
                });
                return callback(null, response);
            });

            request.end(payload);
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
     * This sends a BATCH DELETE request to sproxyd.
     * @param {Object} list - object containing a list of keys to delete
     * @param {Array} list.keys - array of string keys to delete
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    batchDelete(list, reqUids, callback) {
        assert.strictEqual(typeof list, 'object');
        assert(list.keys.every(k => k.length === 40));
        // split the list into batches of 1000 each
        const batches = [];
        while (list.keys.length > 0) {
            batches.push({ keys: list.keys.splice(0, 1000) });
        }
        async.eachLimit(batches, 5, (b, done) => {
            const log = this.createLogger(reqUids);
            const payload = Buffer.from(JSON.stringify(b));
            this._failover('POST', null, payload.length, '.batch_delete', 0,
                log, done, {}, payload);
        }, callback);
    }

    /**
    * This sends a GET request with healthcheck path to sproxyd
    * @param {Object} log - The log from s3
    * @param {SproxydClient-healthcheckCallback} callback - callback
    * @returns {undefined}
    * */
    healthcheck(log, callback) {
        const logger = log || this.createLogger();
        const currentBootstrap = this.getCurrentBootstrap();
        const req = {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method: 'GET',
            path: `${this.path}.conf`,
            headers: {
                'X-Scal-Request-Uids': this._getFirstReqUid(logger),
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
