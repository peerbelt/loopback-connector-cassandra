// creative copy/paste from https://github.com/strongloop/loopback-connector-mysql/blob/master/test/init.js

module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {}; // require('rc')('loopback', {test: {mysql: {}}}).test.mysql;

global.getConfig = function (options) {

  var dbConf = {
    host: config.host || 'localhost',
    port: config.port || 3306,
    keyspace: 'connector-test',
    username: config.username,
    password: config.password,
    createKeyspace: true
  };

  if (options) {
    for (var el in options) {
      dbConf[el] = options[el];
    }
  }

  return dbConf;
};

global.getDataSource = global.getSchema = function (options) {
  var db = new DataSource(require('../'), getConfig(options));
  return db;
};