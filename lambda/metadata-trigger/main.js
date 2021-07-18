const { getMongoClient } = require("./mongodb");
const { getCaseData, checkMetadata } = require('./util');

async function main() {
  const client = getMongoClient();
  await client.connect();
  const metaPromise = client
    .db('covid')
    .collection('metadata')
    .findOne({});
  const dataPromise = getCaseData();

  const [metaData, dataJson] = await Promise.all([metaPromise, dataPromise]);
  client.close();

  const res = checkMetadata(dataJson, metaData);

  if (!res.needsUpdating) return 0;
  invokeLambdas();

}

function invokeLambdas() {
  invokeLambda('covid-data');
}

function invokeLambda(name) {
  lambda.invoke({
    FunctionName: 'testLambda',
    InvocationType: 'Event',
    Payload: JSON.stringify({}, null, 2)
  }, function(err,data){});
}

if (require.main === module) {
  main();
}
