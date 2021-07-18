const axios = require('axios');
const DATA_URL = 'https://data.nsw.gov.au/data/api/3/action/package_show?id=aefcde60-3b0c-4bc0-9af1-6fe652944ec2';

/**
 * 
 * @param {*} dataJson 
 * @param {*} meta 
 * @returns 
 */
function checkMetadata(dataJson, meta) {
  const resourceDate = getCaseDate(dataJson);
  const metaDate = meta?.date;
  const needsUpdating = resourceDate.getTime() != metaDate.getTime();

  console.log(`resourceDate: ${resourceDate} vs ${metaDate} ${needsUpdating}`);
  return {needsUpdating};
}

function getCaseDate(dataJson) {
  return new Date(dataJson.resources[0]['last_modified']);
}

async function getCaseData() {
  const res = await axios.get(DATA_URL);
  return res.data.result;
}

module.exports = {checkMetadata, getCaseData, getCaseDate};