const csv = require('fast-csv');
const sequelize = require('sequelize');
const moment = require('moment');
var sourceData = [];
var validData = [];

function dataIngestion(){
  csv
   .fromPath('testData-1539286153057.csv', {headers: true})
   .on('data', (data) =>
     sourceData.push(data)
   )
   .on('end', () => {
   console.log(sourceData.length + " records loaded");
   dataCleaning();
   });
}

function dataCleaning(){
  for (var i = 0; i < sourceData.length; i++) {
      if(sourceData[i].data != '' &&
         sourceData[i].data > 0)
        parts = sourceData[i].createdAt.split(' ');
        date = parts[3] + parts[1] + parts[2] + parts[4] + parts[0];
        sourceData[i].createdAt = moment(moment(date, "YYYYMMMDDHH:mm:ss")).format('YYYY-MM-DD HH:mm:ss');
        validData = sourceData[i];
      }
}



dataIngestion();



/* console.log(
moment("Thu Mar 08 2018 17:19:42").format("ddd mmm dd yyyy HH:mm:ss")
);

parts = rawData[i].createdAt.split(' ');
date = parts[3] + parts[1] + parts[2] + parts[4] + parts[0];
sourceData[i].createdAt = moment(moment(date, "YYYYMMMDDHH:mm:ss")).format('LLLL');
console.log(rawData);

var input = 'Thu Oct 11 2018 09:51:24 GMT-0400 (EDT)';
var parts = input.split(' ');
date = parts[3] + parts[1] + parts[2] + parts[4] + parts[0]
date = moment(date, "YYYYMMMDDHH:mm:ss");
result = moment(date).format('LLLL');
console.log(result);
*/
