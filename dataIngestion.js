const csv = require('fast-csv');
const sequelize = require('sequelize');
const moment = require('moment');
const MongoClient = require('mongodb').MongoClient;

var sourceData = [];
var validData = [];


// replace the uri string with your connection string.
const uri = "mongodb+srv://sugandanrg:temp1234@cluster0-hd2ly.mongodb.net/test?retryWrites=true"
MongoClient.connect(uri, function(err, client) {
   if(err) {
        console.log('Error occurred while connecting to MongoDB Atlas...\n',err);
   }
   console.log('Connected...');
   const collection = client.db("coindesk").collection("challenge");
   client.close();
});

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
  createModel();
}

function createModel(){




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
