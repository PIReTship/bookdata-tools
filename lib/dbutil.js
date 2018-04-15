const log = require('gulplog');
const pg = require('pg');

async function connect(url) {
  let pg2;
  try {
    pg2 = pg.native;
  } catch (e) {
    log.warn('cannot open native pg: %s', e);
  }
  if (!pg2) {
    pg2 = pg;
  }

  const client = new pg2.Client(url);
  await client.connect();
  return client;
}
module.exports.connect = connect;
