'use strict';

const http = require('http');

const async = require('async');
const uks = require('node-uks');

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

class SproxydClient {
    /**
     * This represent our interface with the sproxyd server.
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {Object} [opts.hostname] - sproxyd hostname
     * @param {Object} [opts.port] - sproxyd port
     * @param {Object} [opts.path] - sproxyd base path
     */
    constructor(opts) {
        if (opts === undefined) {
            opts = {};
        }
        this.opts = {};
        this.opts.hostname = opts.hostname === undefined ?
            '45.55.240.112' : opts.hostname;
        this.opts.port = opts.port === undefined ? 81 : opts.port;
        this.opts.path = opts.path === undefined ?
            '/proxy/arc/' : opts.path;
        this.chunkSize = 4 * 1024 * 1024; // 4Mb
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
        return {
            hostname: this.opts.hostname,
            port: this.opts.port,
            method,
            path: `${this.opts.path}${key}`,
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
            let indexes = this._getIndexes(keysOrValue);
            async.map(indexes, function putChunks(index, cb) {
                const req = this._createRequestHeader('PUT');
                let chunk = keysOrValue.slice(index, this.chunkSize);
                req.headers['Content-length'] = chunk.length;
                let request = _createRequest(req, (err) => {
                    if (err) {
                        return cb(err);
                    }
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
        this._handleRequest('PUT', value, callback);
    }

    /* This sends a GET request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {SproxydClient~getCallback} callback
     */
    get(keys, callback) {
        this._handleRequest('GET', keys, callback);
    }

    /* This sends a DELETE request to sproxyd.
     * @param {String[]} keys - The keys associated to the values
     * @param {SproxydClient~deleteCallback} callback
     */
    delete(keys, callback) {
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
