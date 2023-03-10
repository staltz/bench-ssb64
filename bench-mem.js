const path = require('path');
const pull = require('pull-stream');
const caps = require('ssb-caps');
const ssbKeys = require('ssb-keys');
const makeConfig = require('ssb-config/inject');
const SecretStack = require('secret-stack');
const {logMem} = require('micro-bmark').utils;

const DATASET = 'data64';
console.log('dataset', DATASET);

logMem();
const start = Date.now();

function onEndAsPromise() {
  return (source) => {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });

    pull.onEnd((err) => {
      if (err) reject(err);
      else resolve();
    })(source);

    return promise;
  };
}

function drainAsPromise(cb) {
  return (source) => {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });

    pull.drain(cb, (err) => {
      if (err) reject(err);
      else resolve();
    })(source);

    return promise;
  };
}

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
  .use(require('ssb-master'))
  .use(require('ssb-memdb'))
  .use(require('ssb-classic'))
  .use(require('ssb-box'))
  .call(null, config);

(async function main() {
  await ssb.db.loaded();
  logMem();
  const end = Date.now();
  console.log('Startup:', end - start, 'ms');

  // Scan the entire in-memory log
  {
    const start = Date.now();
    let i = 0;
    ssb.db.forEach((msg) => {
      i = 1 - i;
    });
    const end = Date.now();
    console.log('Naked query:', end - start, 'ms');
  }

  // Query all my posts
  {
    const start = Date.now();
    const myPosts = ssb.db.filterAsArray(
      (msg) => msg.value.author === ssb.id && msg.value.content.type === 'post',
    );
    const end = Date.now();
    // console.log(myPosts);
    console.log('Query all my posts:', end - start, 'ms');
  }

  // ssb-suggest-lite simulation
  {
    const targetName = 'et';
    const found = new Map();
    const start = Date.now();
    ssb.db.forEach((msg) => {
      if (
        msg?.value?.content?.type === 'about' &&
        msg.value.content.about === msg.value.author &&
        typeof msg.value.content.name === 'string' &&
        (msg.value.content.name === targetName ||
          msg.value.content.name?.startsWith(targetName))
      ) {
        found.set(msg.value.author, msg.value.content.name);
      }
    });
    const end = Date.now();
    console.log('Search profile names:', end - start, 'ms');
  }

  // Query my followlist
  {
    const start = Date.now();
    const followlist = new Set();
    ssb.db.forEach((msg) => {
      if (
        msg?.value?.content?.type === 'contact' &&
        msg.value.author === ssb.id &&
        msg.value.content.contact !== ssb.id
      ) {
        const {contact, following, blocking} = msg.value.content;
        if (following) followlist.add(contact);
        else if (!following) followlist.delete(contact);
        else if (blocking) followlist.delete(contact);
      }
    });
    const end = Date.now();
    // console.log(followlist);
    console.log('Collect followlist:', end - start, 'ms');
  }

  // Query my profile details
  {
    const start = Date.now();
    const profile = {};
    ssb.db.forEach((msg) => {
      if (
        msg?.value?.content?.type === 'about' &&
        msg.value.author === ssb.id &&
        msg.value.content.about === ssb.id
      ) {
        const {name, description, image} = msg.value.content;
        if (name) profile.name = name;
        if (description) profile.description = description;
        if (image) profile.image = image;
      }
    });
    const end = Date.now();
    // console.log(profile);
    console.log('Collect my profile:', end - start, 'ms');
  }

  // Query 100 mentions
  {
    const start = Date.now();
    const mentions = [];
    await pull(
      ssb.db.filterAsPullStream((msg) =>
        msg?.value?.content?.mentions?.some(
          (mention) => mention.link === ssb.id,
        ),
      ),
      pull.take(100),
      drainAsPromise((msg) => {
        mentions.push(msg);
      }),
    );
    const end = Date.now();
    // console.log(mentions.map((msg) => JSON.stringify(msg, null, 2)));
    console.log('Collect 100 posts that mention me:', end - start, 'ms');
  }

  // Calculate sizes of all feeds
  {
    const start = Date.now();
    const sizes = new Map();
    ssb.db.forEach((msg) => {
      const size = sizes.get(msg.value.author) || 0;
      sizes.set(msg.value.author, size + msg.meta.size);
    });
    const end = Date.now();
    // console.log(sizes);
    console.log('Calculate sizes of all feeds:', end - start, 'ms');
  }

  logMem();
  ssb.close(true);
})();
