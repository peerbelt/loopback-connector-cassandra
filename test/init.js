// creative copy/paste from https://github.com/strongloop/loopback-connector-mysql/blob/master/test/init.js

module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {}; // require('rc')('loopback', {test: {mysql: {}}}).test.mysql;

global.getConfig = function (options) {

  var dbConf = {
    "contactPoints": [ process.env.CASSANDRA_PORT_9042_TCP_ADDR || process.env.CASSANDRA_HOST || 'localhost' ],
    protocolOptions: { port: process.env.CASSANDRA_PORT_9042_TCP_PORT || process.env.CASSANDRA_PORT || 9042 },
    "keyspace": process.env.CASSANDRA_KEYSPACE || "console",
    queryOptions: {
      fetchSize: 1
    }
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