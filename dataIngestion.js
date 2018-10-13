const csv = require('fast-csv');
var sourceData = [];

csv
 .fromPath('testData-1539286153057.csv', {headers: true})
 .on('data', (data) =>
   sourceData.push(data)
 )
 .on('end', () =>
 console.log(sourceData)
 );
