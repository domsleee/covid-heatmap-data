const { MongoClient } = require('mongodb');
const { exit } = require('process');

const URI_KEY = 'MONGODB_URI';
const uri = process.env[URI_KEY];

if (!(URI_KEY in process.env)) {
  console.log(`ERROR: must define ${URI_KEY}!`);
  exit(1);
}

/**
 * 
 * @returns {MongoClient}
 */
function getMongoClient() {
  return new MongoClient(uri);
}

module.exports = {getMongoClient};