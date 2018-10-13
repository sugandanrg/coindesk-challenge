const csv = require('fast-csv');
var sequelize = require('sequelize');
var sourceData = [];


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
  for (var i = 0; i < sourceData.length; i++)
      if(sourceData[i].data != '' && 
         sourceData[i].data > 0)
        console.log(sourceData[i]);
}

dataIngestion();
