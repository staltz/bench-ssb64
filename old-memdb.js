const path = require('path');
const bipf = require('bipf');
const push = require('push-stream');
const pullDefer = require('pull-defer');
const OffsetLog = require('async-append-only-log');
const Box1 = require('ssb-box/format');
const Classic = require('ssb-classic/format');

const BLOCK_SIZE = 64 * 1024;
function newLogPath(dir) {
  return path.join(dir, 'db2', 'log.bipf');
}

module.exports = {
  name: 'db',
  init(ssb, config) {
    const dir = config.path;
    const log = OffsetLog(newLogPath(dir), {
      cacheSize: 1,
      blockSize: BLOCK_SIZE,
      validateRecord: (d) => {
        try {
          bipf.decode(d, 0);
          return true;
        } catch (ex) {
          return false;
        }
      },
    });
    const msgs = [];

    let resolveDone;
    let rejectDone;
    const promiseDone = new Promise((res, rej) => {
      resolveDone = res;
      rejectDone = rej;
    });

    function ciphertextStrToBuffer(str) {
      const dot = str.indexOf('.');
      return Buffer.from(str.slice(0, dot), 'base64');
    }

    function decrypt(msg) {
      if (typeof msg.value.content !== 'string') return msg;

      // Decrypt
      const ciphertextBuf = ciphertextStrToBuffer(msg.value.content);
      const opts = {keys: config.keys, author: msg.value.author};
      const plaintextBuf = Box1.decrypt(ciphertextBuf, opts);
      if (!plaintextBuf) return msg;

      // Reconstruct KVT in JS encoding
      const originalContent = msg.value.content;
      const nativeMsg = Classic.toNativeMsg(msg.value, 'js');
      const msgVal = Classic.fromDecryptedNativeMsg(
        plaintextBuf,
        nativeMsg,
        'js',
      );
      msg.value = msgVal;
      msg.meta = {
        private: true,
        originalContent,
        encryptionFormat: 'box',
      };

      return msg;
    }

    function filterBy(fn) {
      let x = 0;
      return function source(end, cb) {
        if (end) return cb(end);
        if (x >= msgs.length) return cb(true);
        for (let i = x; i < msgs.length; i++) {
          const msg = msgs[i];
          if (fn(msg)) {
            x = i + 1;
            return cb(null, msg);
          }
        }
        return cb(true);
      };
    }

    function* filterAsIterator(fn) {
      let i = 0;
      while (i < msgs.length) {
        const msg = msgs[i];
        if (fn(msg)) yield msg;
        i++;
      }
    }

    // const deferredContactStream = pullDefer.source();

    log.stream({offsets: true, values: true, sizes: true}).pipe(
      push.drain(
        function drainEach({offset, value, size}) {
          if (!value) return;
          const msg = decrypt(bipf.decode(value, 0));
          msg.meta ??= {};
          msg.meta.offset = offset;
          msg.meta.size = size;
          msgs.push(msg);
        },
        function drainEnd(err) {
          if (err) rejectDone(err);
          else resolveDone();
          // deferredContactStream.resolve(
          //   filterBy((msg) => msg.value.content.type === 'contact'),
          // );
        },
      ),
    );

    return {
      // contactStream() {
      //   return deferredContactStream;
      // },
      onDone() {
        return promiseDone;
      },
      filterBy,
      filterAsIterator,
    };
  },
};
