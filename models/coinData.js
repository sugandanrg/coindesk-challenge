const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const coincap = new Schema({
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
    type: String
  },
  createdAt_date:{
    type: String
  },
  createdAt_time:{
    type: String
  }
});

const coinData = mongoose.model('coinCap', coincap);
module.exports = coinData;
