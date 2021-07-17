const { Collection } = require('mongodb');

const { getMongoClient } = require("./mongodb");

async function main() {
  const client = getMongoClient();
  await client.connect();
  const dataCollection = await client.db('covid').collection('data');

  const week1 = await runAggregate(dataCollection, '05-07-2021');
  await runAggregate(dataCollection, '01-01-1970');
  await runAggregate(dataCollection, '01-06-2020');

  //console.log(week1);

  client.close();
}

/**
 * 
 * @param {Collection} dataCollection 
 * @param {*} date 
 * @returns 
 */
async function runAggregate(dataCollection, date) {
  console.time(date);
  const agg = dataCollection.aggregate([
    { $match: { "notification_date": { $gt: new Date(date) } } },
    { $group: { _id: {postcode: "$postcode"}, sum: {$sum: "$count"}} },
  ], {writeConcern: null});

  const res = await agg.toArray();
  console.timeEnd(date);
  return res;
}

main();