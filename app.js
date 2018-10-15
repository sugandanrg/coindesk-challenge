const csv = require('fast-csv');
const moment = require('moment');
const mongoose = require('mongoose');
const stageData = require('./models/stageData.js');
const coinData = require('./models/coinData.js');

var sourceData = [];
var validData = [];
//mongoDB connection string
const uri = "mongodb+srv://sugandanrg:temp1234@cluster0-hd2ly.mongodb.net/coindesk";
mongoose.Promise = global.Promise;

function dataIngestion(){
  //CSV data ingestion
  csv
   .fromPath('testData-1539286153057.csv', {headers: true})
   .on('data', (data) =>
     sourceData.push(data)
   )
   .on('end', () => {
   console.log("RAW DATA: " + sourceData.length + " records loaded");
   dataCleaning();
   });
}

function dataCleaning(){
  //filter invalid records
  sourceData = sourceData.filter( i => i.data != '' && i.data > 0);
  //separate date and time to faciltate
   for (var i = 0; i < sourceData.length; i++) {
        sourceData[i].id = Number(sourceData[i].id);
        sourceData[i].data = Number(sourceData[i].data);
        sourceData[i].FactorConfigId = Number(sourceData[i].FactorConfigId);
        sourceData[i].createdAt = new Date(sourceData[i].createdAt);
        validData[i] = sourceData[i];
     }
     data123 = moment(sourceData[0].createdAt).format('YYYY-MM-DD');
     console.log(data123);
     console.log(typeof(data123));
  createStageModel();
}

//Thu Oct 11 2018 09:51:24 GMT-0400 (EDT)

function createStageModel(){
  //connect to mongoDB
  mongoose.connect(uri);
  //create a collection of valid data
  stageData.collection.insertMany(validData, function(err, res) {
    if(err){ console.log(err);}
    console.log("VALID DATA: " + validData.length + " records loaded");
    dataTransformation();
  });
}

function dataTransformation(){
  //transformation to attain recent coin marketcap in a day
    stageData.aggregate([
      {
        $group: {
          _id: {
            FactorConfigId: '$FactorConfigId',
            createdAt: moment('$createdAt').format('YYYY-MM-DD')
          },
          id: {$max: "$id"},
          data: {$max: "$data"},
          FactorConfigId: {$max: "$FactorConfigId"},
          createdAt: {$max: moment('$createdAt').format('YYYY-MM-DD')}
        }
      },
      { $project: {_id: 0} },
      { $sort: { id: 1, FactorConfigId: 1 } },
    ], function(err, result){
      if (err) {
        console.log(err); return;
      }
      //create a collection of transformed data
      coinData.collection.insertMany(result)
      .then( function(res) {
        console.log("EXPECTED DATA: " + result.length + " records loaded");
        //closeMongoDBConn();
        stageData.collection.drop();
        mongoose.connection.close();
      })
      .catch(function(err){
        console.log(err);
      });
    });
}


//data flow starts here
dataIngestion();
