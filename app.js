const csv = require('fast-csv');
const moment = require('moment');
const mongoose = require('mongoose');
const async = require('async');
const stageData = require('./models/stageData.js');
const coinData = require('./models/coinData.js');

var sourceData = [];
var validData = [];
var testData = [];
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
    //dataTransformation();
  });
}

function dataTransformation(){
  mongoose.connect(uri);
  stageData.aggregate([
    {
      $group: {
          _id: 'createdAt',
          minDate: {$min: '$createdAt'},
          maxDate: {$max: '$createdAt'}
      }
    }
  ], function(err, res){
        if(err) { console.log(err); return; }

        var minDate = moment(String(res[0].minDate)).format('YYYY-MM-DD HH:mm:ss');
        var maxDate = moment(String(res[0].maxDate)).format('YYYY-MM-DD HH:mm:ss');

        stageData.distinct("FactorConfigId", function (err, pFactorConfigId ){
            if(err) { console.log(err); return; }

            //res.forEach(function(pFactorConfigId){
                for(var f = 0; f < pFactorConfigId.length; f++ ){
                stageData.find({
                  FactorConfigId: pFactorConfigId[f]
                }, function(err, res){
                if(err) { console.log(err); return; }
                //console.log("**********" + res[f].FactorConfigId + "**********");
                var loopDate = minDate;
                var i=0; var count = 0;
                //if(FactorConfigId == 112){
                while(getDateAlone(loopDate) <= getDateAlone(maxDate) && i < res.length - 1){
                  //console.log("i " + i + " length " + res.length +  " createdDate " + res[i].createdAt +
                  //"  loopD " + getDateAlone(loopDate) + " maxD "+ getDateAlone(maxDate));
                        if(getDateAlone(res[i+1].createdAt) == getDateAlone(loopDate)){
                      		loopDate = getDateTime(res[i+1].createdAt);
                      		i += 1;
                        }
                        else{
                          var temp= {
                            id: res[i].id,
                            data: res[i].data,
                            FactorConfigId: res[i].FactorConfigId,
                            createdAt: new Date(loopDate)
                          };
                          testData.push(temp);
                          loopDate = addDate(loopDate,1);
                        }
                        if(loopDate >= maxDate){
                            console.log(testData.length);
                        /* console.log(testData.length);
                        coinData.collection.insertMany(testData, function(err, res){
                        if(err){ console.log(err); return; }
                        //console.log(res);
                      }); */
                    }
                  }

                    function getDateAlone(getDate){
                       var DateAlone = moment(String(getDate)).format('YYYY-MM-DD');
                       return DateAlone;
                    }
                    function getDateTime(getDate){
                      var Date_Time = moment(String(getDate)).format('YYYY-MM-DD HH:mm:ss');
                      return Date_Time;
                    }

                    function addDate(getDate,i){
                      var Add_Date = moment(String(moment(getDate).add(i, 'days'))).format('YYYY-MM-DD HH:mm:ss');
                      return Add_Date;
                    }
              });
            }
          //});
    });
  });
}


dataTransformation();


/*
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
