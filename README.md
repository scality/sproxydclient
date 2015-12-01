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
Client.put(new Buffer('example'), (err: Error, keysArray: string[]) => {});
```

##### GET

```es6
Client.get(keys: string[], (err: Error, valuesArray: Buffer[]) => {});
```

##### DELETE

```es6
Client.delete(keys: string[], (err: Error) => {});
```

### TODO

The API is still in its infancy stages. We need to:
- Handle more than one connector at a time in case of failures
- Detect said failures before sending our requests (Phi accrual detector)
- Improve performance by using streams

[ci-badge]: http://ci.ironmann.io/gh/scality/sproxydclient.svg?style=shield&circle-token=06bf5c091353d80a1296682f78ea08aeb986ce83
