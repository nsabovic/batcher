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
    it('respects options.encoder', function(callback) {
      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      process.nextTick(function() {
        assertCalledWithExactly(encoder, [1, 2, 3]);
        assertCalledWithExactly(ondata, [1, 2, 3]);
        encoder.reset();
        ondata.reset();
        batcher.write(4);
        batcher.write(5);
        batcher.write(6);
        process.nextTick(function() {
          assertCalledWithExactly(encoder, [4, 5, 6]);
          assertCalledWithExactly(ondata, [4, 5, 6]);
          assertNotCalled(onerror);
          callback();
        });
      });
    });

    it('respects options.batchSize', function(callback) {
      batcher.resume();

      assertNotCalled(ondata);
      assertNotCalled(encoder);
      batcher.write(1);
      batcher.write(2);
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        batcher.write(3);
        process.nextTick(function() {
          assertCalledOnce(ondata);
          assertCalledOnce(encoder);

          encoder.reset();
          ondata.reset();
          batcher.write(4);
          batcher.write(5);
          process.nextTick(function() {
            assertNotCalled(ondata);
            assertNotCalled(encoder);
            batcher.write(6);
            process.nextTick(function() {
              assertCalledOnce(ondata);
              assertCalledOnce(encoder);
              assertNotCalled(onerror);
              callback();
            });
          });
        });
      });
    });

    it('respects options.batchTimeMs', function(callback) {
      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      clock.tick(990);

      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        clock.tick(20);

        process.nextTick(function() {
          assertCalledOnce(ondata);
          assertCalledOnce(encoder);
          assertNotCalled(onerror);
          clock.tick(-1010);
          callback();
        });
      });
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
    it('begins paused', function(callback) {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        assertNotCalled(onerror);
        callback();
      });
    });

    it('sends when resumed', function(callback) {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      batcher.resume();
      process.nextTick(function() {
        assertCalledOnce(ondata);
        assertCalledOnce(encoder);
        assertNotCalled(onerror);
        callback();
      });
    });

    it('sends no more than batchSize entries', function(callback) {
      batcher.write(1);
      batcher.write(2);
      batcher.write(3);
      batcher.write(4);
      batcher.resume();
      batcher.pause();
      batcher.write(5);
      batcher.write(6);
      batcher.write(7);
      batcher.write(8);
      batcher.write(9);
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        batcher.resume();
        process.nextTick(function() {
          assertCalledThrice(ondata);
          assert(ondata.firstCall.calledWithExactly([1, 2, 3]));
          assert(ondata.secondCall.calledWithExactly([4, 5, 6]));
          assert(ondata.thirdCall.calledWithExactly([7, 8, 9]));
          assertCalledThrice(encoder);
          assert(encoder.firstCall.calledWithExactly([1, 2, 3]));
          assert(encoder.secondCall.calledWithExactly([4, 5, 6]));
          assert(encoder.thirdCall.calledWithExactly([7, 8, 9]));
          assertNotCalled(onerror);
          callback();
        });
      });
    });
  });

  describe('#flush', function() {
    it('causes send', function(callback) {
      var flushCallback = sinon.spy();

      batcher.resume();
      batcher.write(1);
      batcher.write(2);
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        assertNotCalled(flushCallback);
        batcher.flush(flushCallback);
        process.nextTick(function() {
          assertCalledOnce(flushCallback);
          assertCallOrder(encoder, ondata, flushCallback);
          assertNotCalled(onerror);
          callback();
        });
      });
    });

    it('waits for resume to send', function(callback) {
      var flushCallback = sinon.spy();

      batcher.write(1);
      batcher.write(2);
      batcher.flush(flushCallback);
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        assertNotCalled(flushCallback);
        batcher.resume();
        process.nextTick(function() {
          assertCalledOnce(flushCallback);
          assertCallOrder(encoder, ondata, flushCallback);
          assertNotCalled(onerror);
          callback();
        });
      });
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
    it('takes effect right away', function(callback) {
      batcher.resume();
      process.nextTick(function() {
        assertNotCalled(ondata);
        assertNotCalled(encoder);
        batcher.write(1);
        batcher.write(2);
        process.nextTick(function() {
          assertNotCalled(ondata);
          assertNotCalled(encoder);
          batcher.setBatchSize(2);
          process.nextTick(function() {
            assertCalledOnce(ondata);
            assertCalledOnce(encoder);
            assertNotCalled(onerror);
            callback();
          });
        });
      });
    });
  });
});
