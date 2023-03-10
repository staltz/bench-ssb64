const path = require('path');
const bipf = require('bipf');
const push = require('push-stream');
const AAOL = require('async-append-only-log');

const DATASET = 'data64';

const bipfLog = AAOL(path.join(__dirname, DATASET, 'db2', 'log.bipf'), {
  blockSize: 64 * 1024,
  codec: {
    encode(msg) {
      return bipf.allocAndEncode(msg);
    },
    decode(buf) {
      return bipf.decode(buf, 0);
    },
  },
  validateRecord(buf) {
    try {
      bipf.decode(buf, 0);
      return true;
    } catch {
      return false;
    }
  },
});

const jsonLog = AAOL(path.join(__dirname, DATASET, 'memdb-log.bin'), {
  blockSize: 64 * 1024,
  codec: {
    encode(msg) {
      return Buffer.from(JSON.stringify(msg), 'utf8');
    },
    decode(buf) {
      return JSON.parse(buf.toString('utf8'));
    },
  },
  validateRecord(buf) {
    try {
      JSON.parse(buf.toString('utf8'));
      return true;
    } catch {
      return false;
    }
  },
});

console.log('dataset: ' + DATASET);
const msgs = [];
bipfLog.stream({offsets: false, values: true, sizes: false}).pipe(
  push.drain(
    function drainEach(msg) {
      if (!msg) return; // deleted record
      msgs.push(msg);
    },
    function drainEnd(err) {
      console.log('bipfLog read with ' + msgs.length + ' messages');

      jsonLog.append(msgs, (err) => {
        if (err) throw new Error('Failed to append to jsonLog', {cause: err});
        else console.log('jsonLog written with ' + msgs.length + ' messages');
      });
    },
  ),
);
