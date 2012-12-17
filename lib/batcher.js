// Stream that batches the writes.

// Call write repeatedly, and when `batchSize` of them are written, we emit
// `data` event. Alternatively, a `batchTimeMs` can be specified after which the
// `data` is emitted. The array of items will be passed through a
// `payloadEncoder` function before being emitted. The stream supports
// `pause()`/`resume()`.

var stream = require('stream');
var util = require('util');
var Queue = require('twoqueue');

// options = {
//   encoder:       Function to use to encode batches of messages.
//                  The default encoder just concatenates them.
//   batchSize:     When this many come in, send the batch. Default 200.
//   maxBatchSize:  At most how many to batch together, default is batchSize.
//   batchTimeMs:   After how much time to send a batch even if size is small,
//                  default is 1 minute.
//   batchOverflow: After how many undelivered messages to emit error,
//                  default is 100,000.
// }
function Batcher(options) {
  if (!(this instanceof Batcher)) {
    return new Batcher(options);
  }

  stream.Stream.call(this);
  this.writable = true;
  this.readable = true;

  options = options || {};
  this._batchSize = options.batchSize > 0 ? options.batchSize : 200;
  this._maxBatchSize = options.maxBatchSize || this._batchSize;
  this._batchTimeMs = options.batchTimeMs || 60000;
  this._batchOverflow = options.batchOverflow || 100000;
  this._encoder = options.encoder || function(x) { return x.join(''); };

  this._messages = new Queue();
  this._paused = true;

  this._emitPos = 0;
  this._sentPos = 0;
  this._flushRequests = [];
  this._flushRequested = false;
  this._timeout = null;
  this._scheduled = false;
}

util.inherits(Batcher, stream);


Batcher.prototype._processQueue = function _processQueue() {
  this._scheduled = false;
  while (this._paused === false && this._messages.size() !== 0 &&
          (this._messages.size() >= this._batchSize ||
           this._flushRequested === true)) {
    var payload = this._messages.dequeueMultiple(this._maxBatchSize);
    this.emit('data', this._encoder(payload));

    this._emitPos += payload.length;
    this._sentPos = this._emitPos;
  }

  if (this._paused === false &&
      this._timeout === null &&
      this._messages.size() !== 0) {
    this._timeout = setTimeout(this._onTimeout.bind(this),
                               this._batchTimeMinMs);
  }

  while(this._flushRequests.length !== 0 &&
        this._emitPos >= this._flushRequests[0].pos) {
    var callback = this._flushRequests.shift().callback;
    if (typeof callback === 'function') {
      callback();
    }
  }
  this._flushRequested = this._flushRequests.length !== 0;
 };

Batcher.prototype._onTimeout = function _onTimeout() {
  if (this._messages.size() !== 0) {
    this._timeout = null;
    this._flushRequested = true;
    this._processQueue();
  }
};

Batcher.prototype.write = function write(obj) {
  if (this._messages.size() < this._batchOverflow) {
    if (this._messages.size() === 0 && this._batchSize !== 1) {
      this._timeout = setTimeout(this._onTimeout.bind(this), this._batchTimeMs);
    }
    this._messages.enqueue(obj);
  } else {
    if (this._batchOverflow === -1) {
      process.nextTick(this.emit.bind(this, 'error',
        new Error('Trying to write() a close()-ed batcher')));
    } else {
      process.nextTick(this.emit.bind(this, 'error',
        new Error('Too many undelivered messages')));
    }
  }
  if (this._paused === false && this._scheduled === false) {
    this._scheduled = true;
    process.nextTick(this._processQueue.bind(this));
  }
  return true;
};

Batcher.prototype.pause = function pause() {
  this._paused = true;
};

Batcher.prototype.resume = function resume() {
  if (this._paused === true) {
    this._paused = false;
    this._sentPos = this._emitPos;
    if (this._scheduled === false && this._messages.size() !== 0) {
      this._scheduled = true;
      process.nextTick(this._processQueue.bind(this));
    }
  }
};

Batcher.prototype.end = function end() {
  this.writable = false;
  this._batchOverflow = -1;
};

Batcher.prototype.destroy = function destroy() {
  this.end();
  clearTimeout(this._timeout);
  this._timeout = null;
  this.readable = false;
};

Batcher.prototype.flush = function flush(callback) {
  if (this._messages.size() === 0) {
    return process.nextTick(callback);
  } else {
    var flushPos = this._sentPos + this._messages.size();
    this._flushRequests.push({
      pos: flushPos,
      callback: callback
    });
    this._flushRequested = true;
    if (this._scheduled === false) {
      this._scheduled = true;
      process.nextTick(this._processQueue.bind(this));
    }
  }
};

Batcher.prototype.setBatchSize = function setBatchSize(size) {
  this._batchSize = size;
  if (this._messages.size() >= this._batchSize && this._scheduled === false) {
    this._scheduled = true;
    process.nextTick(this._processQueue.bind(this));
  }
};

module.exports = Batcher;
