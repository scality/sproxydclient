## nodeJS sproxyd client API

[![Circle CI][ci-badge]](http://ci.ironmann.io/gh/scality/sproxydclient)

### Usage

#### Installation

```shell
npm install --save scality/sproxydclient
```

#### API

The client API consists of a class handling basic operations on its side,
using a callback to return whatever value is needed.

##### Initialisation

opts is an optional parameter, that will evolve to an array of connectors.

```es6
var SproxydClient = require('sproxydclient');

var opts = {
    hostname: 'example.com',
    port: '81',
    path: '/proxy/arc/',
};

var client = new SproxydClient(opts);
```

##### PUT

```es6
assert(stream instanceof stream.Readable);
Client.put(stream: http.IncomingMessage, (err: Error, key: string) => {});
```

##### GET

```es6
Client.get(key: string, (err: Error, stream: http.IncomingMessage) => {});
```

##### DELETE

```es6
Client.delete(key: string, (err: Error) => {});
```

### TODO

The API is still in its infancy stages. We need to:
- Detect said failures before sending our requests (Phi accrual detector)

[ci-badge]: http://ci.ironmann.io/gh/scality/sproxydclient.svg?style=shield&circle-token=06bf5c091353d80a1296682f78ea08aeb986ce83
