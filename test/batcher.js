var assert = require('assert');
var sinon = require('sinon');

try {
    var Batcher = require('../lib-cov/batcher');
} catch(e) {
    var Batcher = require('../lib/batcher');
}

sinon.assert.expose(global);

describe('Batcher', function() {
  var encoder = sinon.stub().returnsArg(0);
  var ondata = sinon.spy();
  var onerror = sinon.spy(function(e) { onerror.err = e; });
  var batcher = null;
  var clock = null;

  beforeEach(function() {
    clock = sinon.useFakeTimers(Date.now());
    encoder.reset();
    ondata.reset();
    onerror.reset();
    onerror.err = null;
    batcher = Batcher({
      encoder: encoder,
      batchSize: 3,
      batchTimeMs: 1000,
      batchOverflow: 10
    });
    batcher.on('data', ondata);
    batcher.on('error', onerror);
  });

  afterEach(function() {
    clock.restore();
  });

  describe('constructor', function() {
    it('respects options.encoder', function() {
      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      assertCalledWithExactly(encoder, [1, 2, 3]);
      assertCalledWithExactly(ondata, [1, 2, 3]);
      encoder.reset();
      ondata.reset();
      batcher.write(4);
      batcher.write(5);
      batcher.write(6);
      assertCalledWithExactly(encoder, [4, 5, 6]);
      assertCalledWithExactly(ondata, [4, 5, 6]);
      assertNotCalled(onerror);
    });

    it('respects options.batchSize', function() {
      batcher.resume();

      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.write(1);
      batcher.write(2);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.write(3);
      assertCalledOnce(ondata);
      assertCalledOnce(encoder);

      encoder.reset();
      ondata.reset();
      batcher.write(4);
      batcher.write(5);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.write(6);
      assertCalledOnce(ondata);
      assertCalledOnce(encoder);
      assertNotCalled(onerror);
    });

    it('respects options.batchTimeMs', function() {
      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      clock.tick(990);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      clock.tick(20);
      assertCalledOnce(ondata);
      assertCalledOnce(encoder);
      clock.tick(-1010);
      assertNotCalled(onerror);
    });

    it('respects options.batchOverflow', function() {
      for (var i = 1; i <= 10; ++i) {
        batcher.write(i);
      }
      assertNotCalled(onerror);
      batcher.write(11);
      assertCalledOnce(onerror);
      assert(onerror.err instanceof Error);
    });
  });

  describe('#pause/#resume', function() {
    it('begins paused', function() {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      assertNotCalled(onerror);
    });

    it('sends when resumed', function() {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      batcher.resume();
      assertCalledOnce(ondata);
      assertCalledOnce(encoder);
      assertNotCalled(onerror);
    });

    it('sends no more than batchSize entries', function() {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      batcher.resume();
      batcher.pause();
      encoder.reset();
      ondata.reset();
      batcher.write(5);
      batcher.write(6);
      batcher.write(7);
      batcher.write(8);
      batcher.write(9);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.resume();
      assertCalledTwice(ondata);
      assert(ondata.firstCall.calledWithExactly([4, 5, 6]));
      assert(ondata.secondCall.calledWithExactly([7, 8, 9]));
      assertCalledTwice(encoder);
      assert(encoder.firstCall.calledWithExactly([4, 5, 6]));
      assert(encoder.secondCall.calledWithExactly([7, 8, 9]));
      assertNotCalled(onerror);
    });
  });

  describe('#flush', function() {
    it('causes send', function() {
      var callback = sinon.spy();

      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      assertNotCalled(callback);
      batcher.flush(callback);
      assertCalledOnce(callback);
      assertCallOrder(encoder, ondata, callback);
      assertNotCalled(onerror);
    });

    it('waits for resume to send', function() {
      var callback = sinon.spy();

      batcher.write(1);
      batcher.write(2);
      batcher.flush(callback);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      assertNotCalled(callback);
      batcher.resume();
      assertCalledOnce(callback);
      assertCallOrder(encoder, ondata, callback);
      assertNotCalled(onerror);
    });

    it('calls the callback asynchronously', function(done) {
      var callback = sinon.spy();

      batcher.flush(callback);
      // Callback should be called asynchronously.
      assertNotCalled(callback);
      process.nextTick(function() {
        assertCalledOnce(callback);
        assertNotCalled(onerror);
        done();
      });
    });
  });

  describe('#end/destroy', function() {
    it('end() causes future writes to fail', function() {
      batcher.resume();
      batcher.write(1);
      assertNotCalled(onerror);
      batcher.end();
      batcher.write(2);
      assertCalledOnce(onerror);
      assert(onerror.err instanceof Error);
    });
    it('destroy() stops future events', function() {
      batcher.resume();
      batcher.write(1);
      batcher.destroy();
      clock.tick(1100);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      assertNotCalled(onerror);
      clock.tick(-1100);
    });
  });

  describe('#setBatchSize', function() {
    it('takes effect right away', function() {
      batcher.resume();
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.write(1);
      batcher.write(2);
      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.setBatchSize(2);
      assertCalledOnce(ondata);
      assertCalledOnce(encoder);
      assertNotCalled(onerror);
    });
  });
});
