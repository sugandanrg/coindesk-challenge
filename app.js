const csv = require('fast-csv');
const moment = require('moment');
const mongoose = require('mongoose');
const async = require('async');
const stageData = require('./models/stageData.js');
const coinData = require('./models/coinData.js');
var items = 0;

//mongoDB connection string
const uri = "mongodb+srv://sugandanrg:temp1234@cluster0-hd2ly.mongodb.net/coindesk";


function dataIngestion() {
    //CSV data ingestion
    var sourceData = [];
    csv
        .fromPath('testData-1539286153057.csv', {
            headers: true
        })
        .on('data', (data) =>
            sourceData.push(data)
        )
        .on('end', () => {
            console.log("RAW DATA: " + sourceData.length + " records loaded");
            dataCleaning(sourceData);
        });
}


function dataCleaning(sourceData) {
    var validData = [];
    //filter invalid records
    sourceData = sourceData.filter(i => i.data != '' && i.data > 0);
    //separate date and time to faciltate
    for (var i = 0; i < sourceData.length; i++) {
        sourceData[i].id = Number(sourceData[i].id);
        sourceData[i].data = Number(sourceData[i].data);
        sourceData[i].FactorConfigId = Number(sourceData[i].FactorConfigId);
        sourceData[i].createdAt = new Date(sourceData[i].createdAt);
        validData[i] = sourceData[i];
    }
    createStageModel(validData);
}


function createStageModel(validData) {
    //connect to mongoDB
    mongoose.connect(uri);
    //create a collection of valid data
    stageData.collection.insertMany(validData, function(err, res) {
        if (err) {
            console.log(err);
            return;
        }
        console.log("VALID DATA: " + validData.length + " records loaded");
        dataTransformation(finalInsert);
    });
}


function dataTransformation(callback) {
     mongoose.connect(uri);
     stageData.aggregate([{ //pick the min and max dates from collection
        $group: {
            _id: 'createdAt',
            minDate: {
                $min: '$createdAt'
            },
            maxDate: {
                $max: '$createdAt'
            }
        }
    }], function(err, res) {
        if (err) {
            console.log(err);
            return;
        }
        var minDate = moment(String(res[0].minDate)).format('YYYY-MM-DD HH:mm:ss');
        var maxDate = moment(String(res[0].maxDate)).format('YYYY-MM-DD HH:mm:ss');
        var finalData = [];
        //get an array of distinct FactorConfigId values
        stageData.distinct("FactorConfigId", function(err, pFactorConfigId) {
            if (err) {
                console.log(err);
                return;
            }
            var n = pFactorConfigId.length;
            var itemsProcessed = 0;
            //loop into each distinct FactorConfigId
            pFactorConfigId.forEach(function(pFactorConfigId) {
                stageData.find({
                    FactorConfigId: pFactorConfigId
                }).sort({
                  createdAt: 1 //find the records pertaining to the ID and sort them
                }).exec (function(err, res) {
                    if (err) {
                        console.log(err);
                        return;
                    }
                    var loopDate = minDate;
                    var i = 0;
                    try { //transformation logic
                        while (getDateAlone(loopDate) <= getDateAlone(maxDate)) {
                            if (getDateAlone(res[i + 1].createdAt) == getDateAlone(loopDate)) {
                                loopDate = getDateTime(res[i + 1].createdAt);
                                i += 1;
                            } else {
                                var temp = {
                                    id: res[i].id,
                                    data: res[i].data,
                                    FactorConfigId: res[i].FactorConfigId,
                                    createdAt: new Date(loopDate)
                                };
                                finalData.push(temp);
                                loopDate = addDate(loopDate, 1);
                            }
                            if(i == res.length - 1 && //identify the last record and insert if date matches
                                getDateAlone(loopDate) == getDateAlone(maxDate)){
                              var temp = {
                                  id: res[i].id,
                                  data: res[i].data,
                                  FactorConfigId: res[i].FactorConfigId,
                                  createdAt: new Date(loopDate)
                              };
                              finalData.push(temp);
                            }
                        }
                    } catch (err) {
                        console.log(err)
                    };

                    function getDateAlone(getDate) {
                        var dateAlone = moment(String(getDate)).format('YYYY-MM-DD');
                        return dateAlone;
                    }

                    function getDateTime(getDate) {
                        var dateTime = moment(String(getDate)).format('YYYY-MM-DD HH:mm:ss');
                        return dateTime;
                    }

                    function addDate(getDate, i) {
                        var addDate = moment(String(moment(getDate).add(i, 'days'))).format('YYYY-MM-DD HH:mm:ss');
                        return addDate;
                    }
                    //track all the calls to trigger return (functionc call)
                    if (itemsProcessed == n) {
                        items = items + itemsProcessed;
                    };
                    if ((itemsProcessed * n) == items) {
                        finalInsert(finalData);
                    };
                });
                itemsProcessed = itemsProcessed + 1;
            });
        });
    });
};


function finalInsert(finalData) {
  //insert desired data into coinCap
    coinData.collection.insertMany(finalData)
        .then(function(res) {
            console.log("DATA LOADED: " + finalData.length + " records");
            //drop staged collection and close connection
            stageData.collection.drop()(function (err, res) {
              if(err) { console.log(err); return;}
              mongoose.connection.close();
            });
        });
}

dataIngestion();

/*

avoid duplicate records in a day using aggregate

function aadataTransformation(){
  //transformation to attain recent coin marketcap in a day
    stageData.aggregate([
      {
        $group: {
          _id: {
            FactorConfigId: '$FactorConfigId',
            createdAt: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } }
          },
          id: {$max: "$id"},
          data: {$max: "$data"},
          FactorConfigId: {$max: "$FactorConfigId"},
          createdAt: {$max: "$createdAt"}
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
        //stageData.collection.drop(); //drop staged collection
        mongoose.connection.close(); //closeMongoDBConn();
      })
      .catch(function(err){
        console.log(err);
      });
    });
}

*/
