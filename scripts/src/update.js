const { Collection } = require('mongodb');
const DATA_URL = 'https://data.nsw.gov.au/data/api/3/action/package_show?id=aefcde60-3b0c-4bc0-9af1-6fe652944ec2';
const axios = require('axios');
const Downloader = require('nodejs-file-downloader');
const path = require('path');
const simpleGit = require('simple-git');
const csv = require('csv-parser')
const fs = require('fs');
const { getMongoClient } = require('./mongodb');
const { exit } = require('process');

const dir = path.dirname(require.main.filename);
var rootDir = path.join(dir, '..', '..');
const docsDir = path.join(rootDir, 'docs');
const casesPath = path.join(docsDir, 'cases.csv');

async function main() {
  const dataJson = (await axios.get(DATA_URL)).data.result;
  const client = getMongoClient();
  await client.connect();

  const dbo = await client.db('covid');
  const dataCollection = await dbo.collection('data');
  const metadataCollection = await dbo.collection('metadata');
  const meta = await metadataCollection.findOne({});

  const resource = dataJson.resources[0];
  const resourceDate = resource['created'];
  const metaDate = meta?.date;

  console.log(`resourceDate: ${resourceDate} vs ${metaDate}`);
  if (resourceDate == meta?.date) {
    console.log('up to date');
    exit(0);
  }

  const dataUrl = resource['url'];
  await downloadCsv(dataUrl);
  const keysToRefresh = await getDocs();
  const entries = await readEntriesFromCsv();
  const entriesToUpdate = entries.filter(entry => !(getKey(entry) in keysToRefresh));

  console.time('bulkUpdate')
  await bulkUpdate(dataCollection, entriesToUpdate);
  console.timeEnd('bulkUpdate');

  await updateMetadata(metadataCollection, new Date(resource['created']));

  client.close();
}

async function getDocs() {
  const git = simpleGit(rootDir);
  const diff = await git.diff(`HEAD`, `--`, `${casesPath}`);
  const spl = diff.split("\n");
  const res = {};
  for (const line of spl) {
    if (!(line[0] == '+' || line[0] == '-')) continue;
    const ss = line.split(',');
    if (ss.length === 1) continue;

    const [date, postcode] = [ss[0].replace('+',''), ss[1]];
    if (isNaN(postcode)) continue;
    const entry = {date,postcode};
    res[getKey(entry)] = entry;
  }
  return res;
}

function getKey(entry) {
  return `${entry['notification_date']}_${entry['postcode']}`;
}

async function downloadCsv(dataUrl) {
  const downloader = new Downloader({
    url: dataUrl,
    directory: docsDir,
    fileName: 'cases.csv',
    cloneFiles: false
  });
  return await downloader.download();
}

async function readEntriesFromCsv() {
  const results = {};
  return new Promise(resolve => {
    fs.createReadStream(casesPath)
    .pipe(csv())
    .on('data', (data) => {
      const key = getKey(data);
      if (results[key]) {
        results[key]['count']++;
      } else {
        data['postcode'] = toIntOrDefault(data['postcode'], -1);
        data['notification_date'] = toDateOrDefault(data['notification_date']);
        results[key] = data;
        results[key]['count'] = 1;
      }
    })
    .on('end', () => {
      resolve(Object.values(results));
    });
  });
}

function toIntOrDefault(n, def) {
  return n == '' ? def : parseInt(n);
}

function toDateOrDefault(v) {
  return v == '' ? null : new Date(v);
}

/**
 * 
 * @param {Collection} dataCollection 
 * @param {*} entriesToUpdate 
 */
 async function bulkUpdateChunks(dataCollection, entriesToUpdate) {
  var i, j, chunk = 1000;
  for (i = 0, j = entriesToUpdate.length; i < j; i += chunk) {

    console.log(`doing ${i}-${i+chunk} / ${entriesToUpdate.length}`)
    console.time('dbsave')
    await bulkUpdate(dataCollection, entriesToUpdate.slice(i, i + chunk));
    console.timeEnd('dbsave');
  }
}

/**
 * 
 * @param {Collection} dataCollection 
 * @param {*} entriesToUpdate 
 */
async function bulkUpdate(dataCollection, entriesToUpdate) {
  console.log(`updating ${entriesToUpdate.length} entries...`);
  const bulk = dataCollection.initializeUnorderedBulkOp();
  for (const entry of entriesToUpdate) {
    bulk.find({
      notification_date: entry['notification_date'],
      postcode: entry['postcode']
    }).upsert().updateOne({$set: entry});
  }
  await bulk.execute();
}

/**
 * 
 * @param {Collection} metadataCollection 
 * @param {*} newDate 
 */
async function updateMetadata(metadataCollection, newDate) {
  console.log('updating metadata...');
  await metadataCollection.updateOne({}, {$set: {'date': newDate}}, {upsert: true});
}

main();