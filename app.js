const csv = require('fast-csv');
const moment = require('moment');
const mongoose = require('mongoose');
const stageData = require('./models/stageData.js');
const coinData = require('./models/coinData.js');

var sourceData = [];
var validData = [];
//mongoDB connection string
const uri = "mongodb+srv://sugandanrg:temp1234@cluster0-hd2ly.mongodb.net/coindesk";


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
        parts = sourceData[i].createdAt.split(' ');
        date = parts[3] + parts[1] + parts[2] + parts[0];
        time = parts[4];
        sourceData[i].createdAt_date = moment(moment(date, "YYYYMMMDD ddd")).format('YYYY-MM-DD ddd');
        sourceData[i].createdAt_time = moment(moment(time, "HH:mm:ss")).format('HH:mm:ss');
        validData[i] = sourceData[i];
     }
  createModel();
}

function createModel(){
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
            createdAt_date: '$createdAt_date'
          },
          id: {$max: "$id"},
          data: {$max: "$data"},
          FactorConfigId: {$max: "$FactorConfigId"},
          createdAt: {$max: "$createdAt"},
          createdAt_date: {$max: "$createdAt_date"},
          createdAt_time: {$max: "$createdAt_time"}
        }
      },
      { $project: {_id: 0} },
      { $sort: { id: 1, FactorConfigId: 1 } },
    ], function(err, result){
      if (err) {
        console.log(err); return;
      }
      //create a collection of transformed data
      coinData.collection.insert(result, function(err, res) {
        if(err){ console.log(err);}
        console.log("EXPECTED DATA: " + result.length + " records loaded");
        //close mongoDB connection
        mongoose.connection.close();
      });
    });
}

//data flow starts here
dataIngestion();
