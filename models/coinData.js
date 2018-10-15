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
    type: Date
  }
});

const coinData = mongoose.model('coinCap', coincap);
module.exports = coinData;
