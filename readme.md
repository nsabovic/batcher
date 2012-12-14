# Batcher

Batching your streams since 2012.

## Usage

Your logging service takes a stream of JSON objects. You don't want to stream
objects as they come, but rather batch them and send them every minute, or when
a hundred comes by or some such thing.

    var Batcher = require('batcher');

    var b = new Batcher({
      batchSize: 100,
      batchTimeMs: 60000,
      encoder: function(items) {
        return items.map(JSON.stringify).join('');
      }
    });

    b.pipe(net.createConnection(host: 'loggingservice.com', port: 1337));

More frequently, you don't need to pipe it but just accumulate:

    b.on('data', function(payload) {
      upload('/somewhere', payload);
    });

You can also manually flush the batch:

    b.write('one');
    b.flush(function() {
      console.log('one has been sent');
    });

Batchers support `write()`, `pause()`, `resume()`, `flush()`, `end()` and
`destroy()`.
