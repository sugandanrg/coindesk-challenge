const csv = require('fast-csv');
const moment = require('moment');
const mongoose = require('mongoose');
const stageData = require('./models/stageData.js');
const coinData = require('./models/coinData.js');

var sourceData = [];
var validData = [];
const uri = "mongodb+srv://sugandanrg:temp1234@cluster0-hd2ly.mongodb.net/coindesk";


function dataIngestion(){
  //CSV data ingestion
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
  //filter invalid records
  sourceData = sourceData.filter( i => i.data != '' && i.data > 0);
  //timestamp conversion
   for (var i = 0; i < sourceData.length; i++) {
        parts = sourceData[i].createdAt.split(' ');
        date = parts[3] + parts[1] + parts[2] + parts[0];
        time = parts[4];
        sourceData[i].createdAt_date = moment(moment(date, "YYYYMMMDD ddd")).format('YYYY-MM-DD ddd');
        sourceData[i].createdAt_time = moment(moment(time, "HH:mm:ss")).format('HH:mm:ss');
        validData[i] = sourceData[i];
     }
    console.log(validData.length + " records loaded");
  createModel();
}

function createModel(){
  mongoose.connect(uri);
  stageData.collection.insert(validData, function(err, res) {
    if(err){ console.log(err);}
    console.log("loaded successfully -> " + res);
    dataTransformation();
  });
}

function dataTransformation(){
    stageData.aggregate([
      {
        $group: {
          _id: {
            FactorConfigId: '$FactorConfigId',
            createdAt_date: '$createdAt_date'
          },
          id: {$max: "$id"},
          data: {$max: "$data"},
          FactorConfigId: {$max: "$FactorConfigId"},
          createdAt: {$max: "$createdAt"},
          createdAt_time: {$max: "$createdAt_time"}
        }
      },
      { $project: {_id: 0} },
      { $sort: { id: 1, FactorConfigId: 1 } },
    ], function(err, result){
      if (err) {
        console.log(err); return;
      }
      coinData.collection.insert(result, function(err, res) {
        if(err){ console.log(err);}
        console.log("Loaded successfully -> " + result.length);
        mongoose.connection.close();
      });
    });
}


dataIngestion();

/*
//insert data into mongodb
function insertMongo(){
  MongoClient.connect(uri, function(err, client) {
     if(err) {
          console.log('Error occurred while connecting to MongoDB Atlas...\n',err);
     }
     console.log('Connected...');
     var db = client.db("coindesk");
     try
       {
         db.collection("stage").drop();
         db.collection("stage").insertMany(validData);
     } catch(e) { console.log(e); };
     try
       {
          test = db.collection("stage").aggregate(
           [
             {$group: {_id: {$FactorConfigId: "$FactorConfigId", $createdAt_date: "$createdAt_date" }}},
             {$sort: {id: 1}}
           ]
       );
       console.log(test);
     } catch(e) { console.log(e); };
     //client.close();
  });
}

*/





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
