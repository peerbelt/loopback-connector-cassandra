/**
 * Created by abj on 5/8/15.
 */

/*!
 * Module dependencies
 */
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:cassandra');
var cassandra = require( "cassandra-driver" );

exports.Cassandra = Cassandra;

/**
 * @constructor
 * Constructor for Cassandra connector
 * @param {Object} client The cassandra client object
 * @param Cassandra settings The settings
 */
function Cassandra( client, settings ) {
  this.name = 'cassandra';
  this._models = {};
  this.ids = {};
  this.client = client;
  this.settings = settings;
}

// TODO (Emo): Consider using SqlConnector
require('util').inherits(Cassandra, Connector);

/**
 * see http://docs.strongloop.com/display/public/LB/Advanced+topics%3A+data+sources
 */
exports.initialize = function initializeSchema( dataSource, callback ) {
  // check dependencies
  var poolOptions;
  if (!( poolOptions = dataSource.settings ) ) {
    return;
  }

  // open client connection
  dataSource.client = new cassandra.Client(poolOptions);

  // handle client connection errors
  dataSource.client.on('error', function (err) {
    dataSource.emit('error', err);
    dataSource.connected = false;
    dataSource.connecting = false;
  });

  dataSource.client.on("log", function ( level, className, message, furtherInfo ) {
    if ( debug.enabled && ( level === "error") ) {
      console.error("log event: %s [%s] - %s, %s", level, className, message, furtherInfo);
    }
  });

  if (debug.enabled) {
    debug('Settings: %j', dataSource.settings);
  }

  // create connector
  dataSource.connector = new Cassandra( dataSource.client, poolOptions );
  dataSource.connector.dataSource = dataSource;

  // connect
  dataSource.connector.connect( callback );

};

var getType = function( value ) {
  var propertyType, temp;
  switch( propertyType = typeof( value ) ) {

    case "string":
    case "number":
      break;

    case "object":
      if ( value ) {
        if ( ( temp = value.constructor ) && ( temp = temp.name ) && ( temp = temp.toLowerCase() ) && ( temp === "date" ) ) {
          propertyType = "date";
        }
        else {
          value = JSON.stringify( value );
        }
        break;
      }

    default:
      return;
      break;
  }
  return { type: propertyType, value: value };
};

var getTypedMaps = function( data, idName ) {
  var typedMaps = {}, propertyType, map, value;
  for( var propertyName in data ) {

    if ( propertyName === idName ) {
      continue;
    }

    if ( !( value = getType( data[ propertyName ] ) ) ) {
      continue;
    }

    propertyType = value.type;
    value = value.value;

    if ( !( map = typedMaps[ propertyType ] ) ) {
      typedMaps[ propertyType ] = map = {};
    }
    map[ propertyName ] = value;
  }
  return typedMaps;
};

var getQueryFieldsAndParams = (function(){

  var knownMaps = { "string": [ "text", "text" ],
    "number": [ "text", "double" ],
    "object": [ "text", "text" ],
    "date": [ "text", "timestamp" ] };

  return function( model, data, idName, flatten ) {

    var maps = getTypedMaps( data, idName );
    var cql = [], params = [], paramTypes = [];
    var id = data[ idName ];
    var map, property, type;

    for( var mapType in knownMaps ) {
      if ( flatten ) {
        map = maps[mapType];
        type =  knownMaps[ mapType ];
        if ( map ) {
          for( property in map ) {
            cql.push( mapType );
            params.push( property, map[ property ] );
            paramTypes.push( type[ 0 ], type[ 1 ] );
          }
        }
      }
      else {
        cql.push( mapType );
        params.push(maps[mapType]);
        paramTypes.push("map<" + knownMaps[mapType].join(",") + ">" );
      }
    }

    cql.push( "model", "id", "type" );
    params.push( String( model ), String( id ), "." );
    paramTypes.push( "text", "text", "text" );

    cql = [ cql, params, paramTypes ];
    cql.primaryKeyFields = 3;

    return cql;

  };
})();

Cassandra.prototype.collectionSeq = function( model, val ) {
  if (arguments.length > 1) this.ids[model] = val;
  return this.ids[model];
};

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Cassandra.prototype._create = function ( model, data, callback, createAlways ) {

  var currentId = this.collectionSeq(model);
  if (currentId === undefined) { // First time
    currentId = this.collectionSeq( model, ( new Date().valueOf() ) );
  }
  var id = this.getIdValue(model, data) || currentId;
  if (id > currentId) {
    // If the id is passed in and the value is greater than the current id
    currentId = id;
  }
  this.collectionSeq( model, Number(currentId) + 1 );

  var props = this._models[model].properties;
  var idName = this.idName(model);
  id = (props[idName] && props[idName].type && props[idName].type(id)) || id;
  this.setIdValue(model, data, id);

  var cql = getQueryFieldsAndParams( model, data, idName), fields;
  this.client.execute( "insert into data ( " + ( fields = cql[ 0 ] ).join( ", " ) +
      " ) values ( " + ( new Array( fields.length ) ).join( "?, " ) + "? )" + ( createAlways ? "": " if not exists" ),
      cql[ 1 ], { "hints": cql[ 2 ] }, function( err, result ) {
        if ( err ) {
          callback( err );
        }
        else if ( result && ( result = result.rows ) && ( result = result[ 0 ] ) && !result["[applied]"] ) {
          callback(new Error('Duplicate entry for ' + model + '.' + idName), null, { isNewInstance: false } );
        }
        else {
          callback( null, data, { isNewInstance: true } );
        }
      } );
};

/**
 * Save a model instance
 */
Cassandra.prototype._save = function ( model, data, callback ) {

  var props = this._models[model].properties;
  var idName = this.idName(model);

  var bundle = getQueryFieldsAndParams( model, data, idName, true ), fields;
  var params = bundle[ 1 ], paramTypes = bundle[ 2 ];
  cql = bundle[ 0 ];

  cql = "update data set " + cql.slice( 0, cql.length - bundle.primaryKeyFields).join( "[?] = ?, " ) + "[?] = ?"
      + " where " + cql.slice( cql.length - bundle.primaryKeyFields).join( " = ? and " ) + " = ?";
  this.client.execute( cql, params, { "hints": paramTypes }, function( err, result ) {
    if ( err ) {
      callback( err );
    }
    else {
      callback( null, data, { isNewInstance: false } );
    }
  } );
};

/**
 * Connects the pool to Cassandra
 */
Cassandra.prototype.connect = (function() {

  var callbacks = [];

  var notify = function () {
    var cb;
    while (cb = callbacks.shift()) {
      try {
        cb();
      }
      catch (e) {
        ;
      }
    }
  };

  return function (callback) {

    callbacks.push(callback);

    var self = this;
    var dataSource = self.dataSource;
    var pool = self.client;

    if (dataSource.initializing) {
      return;
    }
    if (dataSource.initialized) {
      process.nextTick(notify);
    }
    dataSource.initializing = true;
    pool.connect(function (er, keySpace) {
          delete dataSource.initializing;
          if (!er) {
            dataSource.initialized = true;
            notify();
          }
        }
    );
  }
})();

/**
 * Update a model instance or create a new model instance if it doesn't exist
 */
Cassandra.prototype.updateOrCreate = function ( model, data, callback ) {
  this._create( model, data, callback, true );
};

Cassandra.prototype.create = Cassandra.prototype._create;

/**
 * Check if a model instance exists by id
 */
Cassandra.prototype.exists = function exists(model, id, callback) {
  this.client.execute( "select id from data where model = ? and id = ?", [ String( model ) , String( id ) ] , { hints: [ "text", "text" ] }, function( err, result ) {
    if ( result && ( result = result.rows ) ) {
      callback( err, !!result.length );
    }
    else {
      callback( err );
    }
  } );
};

/**
 * Update the attributes for a model instance by id
 */
Cassandra.prototype.updateAttributes = function updateAttributes(model, id, data, cb) {
  var self = this;
  self.exists( model, id, function( err, exists ) {
    if ( err ) {
      cb( err );
    }
    else if (!exists) {
      cb(new Error('Could not update attributes. Object with id ' + id + ' does not exist!')); }
    else {
      self._save( model, id, data, cb );
    }
  } );
};

Cassandra.prototype.save = Cassandra.prototype._save;

/**
 * Delete a model instance by id
 */
Cassandra.prototype.destroy = function destroy(model, id, callback) {
  this.client.execute( "delete from data where model = ? and id = ?", [ String( model ), String( id ) ],
      { hints: [ "text", "text" ] }, callback );
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
