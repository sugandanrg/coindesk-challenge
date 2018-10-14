const mongoose = require('mongoose');
const Schema = mongoose.Schema;


const mData = new Schema({
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

const modelData = mongoose.model('modelData', mData);
module.exports = modelData;

/*

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('validData', {
    id: {
      type: DataTypes.INTEGER(5),
      allowNull: false,
      primaryKey: true
    },
    data: {
      type: DataTypes.INTEGER(5),
      allowNull: true
    },
    FactorConfigId: {
      type: DataTypes.INTEGER(5),
      allowNull: true
    },
    createdAt: {
      type: DataTypes.STRING,
      allowNull: true
    }
  }, {
    tableName: 'coinPrice'
  });
};

*/
