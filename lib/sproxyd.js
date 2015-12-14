'use strict';

const assert = require('assert');
const http = require('http');

const async = require('async');
const uks = require('node-uks');

const shuffle = require('./shuffle');

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, callback) {
    return http.request(req, function handleResponse(response) {
        let body = new Buffer(0);
        response.on('data', data => body = Buffer.concat([ body, data, ]))
            .on('end', () => {
                if (response.statusCode !== 200)
                    return callback(new Error(response.statusCode));
                callback(null, body);
            })
            .on('error', err => callback(err));
    });
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
    _createRequestHeader(method, key) {
        if (!key) {
            key = uks.createRandomKey().value.toString(16).toUpperCase();
        }
        const currentBootstrap = this.getCurrentBootstrap();
        return {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method,
            path: `${this.path}${key}`,
            headers: {
                'X-Scal-Replica-Policy': 'immutable',
                'content-type': 'application/x-www-form-urlencoded',
            },
        };
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending. async.map() allows us to generate the
     * Array that is sent back with the callback in case of success.
     */
    _handleRequest(method, keysOrValue, callback) {
        if (method === 'PUT') {
            const indexes = this._getIndexes(keysOrValue);
            async.map(indexes, function putChunks(index, cb) {
                const req = this._createRequestHeader('PUT');
                const chunk = keysOrValue.slice(index, index + this.chunkSize);
                req.headers['Content-length'] = chunk.length;
                const request = _createRequest(req, (err) => {
                    if (err) {
                        return cb(err);
                    }
                    // We return the key from the path
                    return cb(null, req.path.split('/')[3]);
                });
                request.write(chunk);
                request.end();
            }.bind(this), callback);
        } else {
            async.map(keysOrValue, function handle(key, cb) {
                const req = this._createRequestHeader(method, key);
                const request = _createRequest(req, cb);
                request.end();
            }.bind(this), callback);
        }
    }

    /**
     * This sends a PUT request to sproxyd.
     * @param {Buffer} value - The data to send
     * @param {SproxydClient~putCallback} callback
     */
    put(value, callback) {
        assert(value instanceof Buffer, 'value should be a buffer');
        assert(value.length, 'value should not be empty');
        this._handleRequest('PUT', value, callback);
    }

    /**
     * This sends a GET request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {SproxydClient~getCallback} callback
     */
    get(keys, callback) {
        assert(keys instanceof Array, 'keys should be an array');
        assert(keys.length, 'keys should not be empty');
        keys.forEach(key => assert(typeof key === 'string'
                     && key.length === 40, 'wrong key format'));
        this._handleRequest('GET', keys, callback);
    }

    /**
     * This sends a DELETE request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {SproxydClient~deleteCallback} callback
     */
    delete(keys, callback) {
        assert(keys instanceof Array, 'key should be an array');
        assert(keys.length, 'keys should not be empty');
        keys.forEach(key => assert(typeof key === 'string'
                     && key.length === 40, 'wrong key format'));
        this._handleRequest('DELETE', keys, callback);
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
