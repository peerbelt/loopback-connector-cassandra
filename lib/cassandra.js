/**
 * Created by abj on 5/8/15.
 */

/*!
 * Module dependencies
 */
var cassandra = {

  createClient: function() {
    return {
      on: function() {}
    };
  }

};
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:cassandra');

/**
 * see http://docs.strongloop.com/display/public/LB/Advanced+topics%3A+data+sources
 */
exports.initialize = function initializeSchema(dataSource, callback) {
  // check dependencies
  if (!cassandra || !dataSource.settings) {
    return;
  }

  // open client connection
  dataSource.client = cassandra.createClient();

  // handle client connection errors
  dataSource.client.on('error', function (err) {
    dataSource.emit('error', err);
    dataSource.connected = false;
    dataSource.connecting = false;
  });

  if (debug.enabled) {
    debug('Settings: %j', dataSource.settings);
  }

  // create connector
  dataSource.connector = new Cassandra(dataSource.client, dataSource.settings);
  dataSource.connector.dataSource = dataSource;

  process.nextTick(function () {
    callback && callback && callback();
  });
};

exports.Cassandra = Cassandra;

/**
 * @constructor
 * Constructor for Cassandra connector
 * @param {Object} client The cassandra client object
 * @param Cassandra settings The settings
 */
function Cassandra(client, settings) {
  this.name = 'cassandra';
  this._models = {};
  this.client = client;
  this.settings = settings;
}

// TODO (Emo): Consider using SqlConnector
require('util').inherits(Cassandra, Connector);

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Cassandra.prototype.create = function (model, data, callback) {
  debug('create(model:%j data:%j)', model, data);
  callback(null, '1');
};

/**
 * Save a model instance
 */
Cassandra.prototype.save = function (model, data, callback) {
  debug('save(model:%j data:%j)', model, data);
  callback(null, data);
};

/**
 * Check if a model instance exists by id
 */
Cassandra.prototype.exists = function (model, id, callback) {
  debug('exists(model:%j id:%j)', model, id);
  callback(null, id);
};

/**
 * Find a model instance by id
 * @param {Function} callback - you must provide an array of results to the callback as an argument
 */
Cassandra.prototype.find = function find(model, id, callback) {
  debug('find(model:%j id:%j)', model, id);
  callback && callback();
};

/**
 * Update a model instance or create a new model instance if it doesn't exist
 */
Cassandra.prototype.updateOrCreate = function updateOrCreate(model, data, callback) {
  debug('model:%j\ndata:%j', model, data);
  callback && callback(data);
};

/**
 * Delete a model instance by id
 */
Cassandra.prototype.destroy = function destroy(model, id, callback) {
  debug('destroy(model:%j id:%j)', model, id);
  callback && callback();
};

/**
 * Query model instances by the filter
 */
Cassandra.prototype.all = function all(model, filter, callback) {
  debug('all(model:%j filter:%j)', model, filter);
  callback && callback();
};

/**
 * Delete all model instances
 */
Cassandra.prototype.destroyAll = function destroyAll(model, where, callback) {
  debug('destroyAll(model:%j where:%j)', model, where);
  // TODO (Krassi): where can be callback as well
  if (typeof(where) === "function") {
    callback = where;
  }
  callback && callback();
};

/**
 * Count the model instances by the where criteria
 */
Cassandra.prototype.count = function count(model, callback, where) {
  debug('count(model:%j where:%j)', model, where);
  callback(null, 1);
};

/**
 * Update the attributes for a model instance by id
 */
Cassandra.prototype.updateAttributes = function updateAttrs(model, id, data, callback) {
  debug('updateAttributes(model:%j id:%j data:%j)', model, id, data);
  callback(null, data);
};
