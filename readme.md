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

`pause()/resume()` is useful when you need to send with retries, but want to
avoid the thundering herd when the server comes back online.

    var b = new Batcher({
      batchSize: 1,
      maxBatchSize: 100,
      encoder: function(items) {
        b.pause();

        function send() {
          request({
            url: some_url,
            json: items,
          }, function(err) {
            if (err) return setTimeout(send, 1000);
            b.resume();
          });
        }

        send();
      }
    });

`batchSize` of 1 means send them as they come. `maxBatchSize` of 100 means if
there are more available, send as many as 100. `setTimeout` is used to retry
sending every second.
