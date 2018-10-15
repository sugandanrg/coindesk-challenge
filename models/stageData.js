const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const stage = new Schema({
  id: {
    type: Number
  },
  data: {
    type: Number
  },
  FactorConfigId: {
    type: Number
  },
  createdAt: {
    type: Date
  }
});

const stageData = mongoose.model('stage', stage);
module.exports = stageData;
