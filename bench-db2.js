const path = require('path');
const pull = require('pull-stream');
const caps = require('ssb-caps');
const ssbKeys = require('ssb-keys');
const makeConfig = require('ssb-config/inject');
const SecretStack = require('secret-stack');
const promisify = require('util').promisify;
const {logMem} = require('micro-bmark').utils;

const DATASET = 'data64';
console.log('dataset', DATASET);

logMem();
const start = Date.now();

const SSB_DIR = path.join(__dirname, DATASET);
const KEYS_PATH = path.join(SSB_DIR, 'secret');
const keys = ssbKeys.loadOrCreateSync(KEYS_PATH);

const config = makeConfig('ssb', {
  caps,
  keys,
  path: SSB_DIR,
  db2: {
    automigrate: true,
    dangerouslyKillFlumeWhenMigrated: true,
  },
  blobs: {
    sympathy: 2,
  },
  friends: {
    hops: 2,
    hookAuth: false,
  },
  suggest: {
    autostart: true,
  },
  connections: {
    incoming: {
      net: [{scope: 'private', transform: 'shs', port: 26831}],
    },
    outgoing: {
      net: [{transform: 'shs'}],
    },
  },
});

const ssb = SecretStack()
  // Core
  .use(require('ssb-master'))
  .use(require('ssb-db2'))
  .use(require('ssb-db2/compat/db'))
  .use(require('ssb-db2/compat/ebt'))
  .use(require('ssb-db2/compat/log-stream'))
  .use(require('ssb-db2/compat/history-stream'))
  .use(require('ssb-friends'))
  .use(require('ssb-about-self'))
  .use(require('ssb-suggest-lite'))
  .use(require('ssb-threads'))
  .use(require('ssb-db2/full-mentions'))
  .use(require('ssb-search2'))
  .use(require('ssb-storage-used'))
  .call(null, config);

const {where, and, type, author, mentions, toPullStream, toPromise} =
  ssb.db.operators;

pull(
  ssb.db.query(where(type('contact')), toPullStream()),
  pull.onEnd(async () => {
    logMem();
    const end = Date.now();
    console.log('Startup:', end - start, 'ms');

    // jitdb
    {
      await p(setTimeout)(500);
      const start = Date.now();
      const myPosts = await ssb.db.query(
        where(and(type('post'), author(ssb.id, {dedicated: true}))),
        toPromise(),
      );
      const end = Date.now();
      // console.log(myPosts);
      console.log('Query all my posts:', end - start, 'ms');
    }

    // ssb-suggest-list
    {
      await p(setTimeout)(500);
      const text = 'et';
      const start = Date.now();
      const found = await promisify(ssb.suggest.profile)({text});
      const end = Date.now();
      // console.log(found);
      console.log('Search profile names:', end - start, 'ms');
    }

    // Query my followlist
    {
      await p(setTimeout)(500);
      const start = Date.now();
      const follows = await promisify(ssb.friends.hops)({
        start: ssb.id,
        max: 1,
      });
      const end = Date.now();
      // console.log(follows)
      console.log('Collect followlist:', end - start, 'ms');
    }

    // Query my profile details
    {
      await p(setTimeout)(500);
      const start = Date.now();
      const profile = await promisify(ssb.aboutSelf.get)(ssb.id);
      const end = Date.now();
      // console.log(profile)
      console.log('Collect my profile:', end - start, 'ms');
    }

    // Query 100 mentions
    {
      await p(setTimeout)(500);
      const start = Date.now();
      const arr = await pull(
        ssb.db.query(where(mentions(ssb.id)), toPullStream()),
        pull.take(100),
        pull.collectAsPromise(),
      );
      const end = Date.now();
      // console.log(arr);
      console.log('Collect 100 posts that mention me:', end - start, 'ms');
    }

    // Calculate size of all feeds
    {
      await p(setTimeout)(500);
      const start = Date.now();
      const arr = await pull(ssb.storageUsed.stream(), pull.collectAsPromise());
      const end = Date.now();
      // console.log(arr);
      console.log('Calculate size of all feeds:', end - start, 'ms');
    }

    ssb.close(true);
  }),
);
