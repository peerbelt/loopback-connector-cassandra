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
 * TODO: Need to initialize schema with the CFs already in Cassandra
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

/**
 * Creates new array and initializes all elements with particular value
 * @param {Number} the new array size
 * @param the value to initilialize each and every element with
 */
var fill = function( sz, value ){
  var r = [];
  while(sz) {
    r.push( value );
    sz--;
  }
  return r;
};

/**
 * Merges two objects and returns the result
 * @param the base object
 * @param the object to extend the base with
 */
var merge = function(base, update) {
  // note: we need to copy all the
  // properties into a new object. the
  // custom objects may have "strange" toJSON
  // implementation that hides some of the data
  if (!base) {
    if ( Array.isArray( update ) ) {
      return update;
    }
    else {
      base = { };
    }
  }
  if ( update ) {
    if ( Array.isArray( update ) ) {
      for( var i = 0, l = update.length, it; i < l; i ++ ) {
        if ( ( it = update[ i ] ) === undefined ) {
          continue;
        }
        base[ i ] = update[ i ];
      }
    }
    else {
      // We cannot use Object.keys(update) if the update is an instance of the model
      // class as the properties are defined at the ModelClass.prototype level
      for (var key in update) {
        var val = update[key];
        if (typeof val === 'function') {
          continue; // Skip methods
        }
        base[key] = val;
      }
    }
  }
  return base;
};

/**
 * Creates new array and initializes all elements with particular value
 * @param {Number} the new array size
 * @param the value to initilialize each and every element with
 */
var getValue = function (obj, path) {
  if (obj == null) {
    return undefined;
  }
  var keys = path.split('.');
  var val = obj;
  for (var i = 0, n = keys.length; i < n; i++) {
    val = val[keys[i]];
    if (val == null) {
      return val;
    }
  }
  return val;
};

/**
 * Resultset sorting helper
 * @param the object to compare
 * @param the object to compare
 */
var sorting = function (a, b) {
  var undefinedA, undefinedB;

  for (var i = 0, l = this.length; i < l; i++) {
    var aVal = getValue(a, this[i].key);
    var bVal = getValue(b, this[i].key);
    undefinedB = bVal === undefined && aVal !== undefined;
    undefinedA = aVal === undefined && bVal !== undefined;

    if (undefinedB || aVal > bVal) {
      return 1 * this[i].reverse;
    } else if (undefinedA || aVal < bVal) {
      return -1 * this[i].reverse;
    }
  }
};

/**
 * Builds a filter, returning only the elements that satisfy it
 * @param {Object} the filter to apply
 */
var applyFilter = function(filter) {
  var where = filter.where;
  if (typeof where === 'function') {
    return where;
  }
  var keys = Object.keys(where);
  return function (obj) {
    var pass = true;
    keys.forEach(function (key) {
      if (key === 'and' || key === 'or') {
        if (Array.isArray(where[key])) {
          if (key === 'and') {
            pass = where[key].every(function (cond) {
              return applyFilter({where: cond})(obj);
            });
            return pass;
          }
          if (key === 'or') {
            pass = where[key].some(function (cond) {
              return applyFilter({where: cond})(obj);
            });
            return pass;
          }
        }
      }
      if (!test(where[key], getValue(obj, key))) {
        pass = false;
      }
    });
    return pass;
  }
};

/**
 * Filter helper
 * @param the value to look for
 * @param the value to examine
 */
var test = function (example, value) {
  if (typeof value === 'string' && (example instanceof RegExp)) {
    return value.match(example);
  }
  if (example === undefined) {
    return undefined;
  }

  if (typeof example === 'object' && example !== null) {
    // ignore geo near filter
    if (example.near) {
      return true;
    }

    if (example.inq) {
      // if (!value) return false;
      for (var i = 0; i < example.inq.length; i++) {
        if (example.inq[i] == value) {
          return true;
        }
      }
      return false;
    }

    if (example.nin) {
      // if (!value) return false;
      for (var i = 0; i < example.nin.length; i++) {
        if (example.nin[i] == value) {
          return false;
        }
      }
      return true;
    }
    if ('neq' in example) {
      return compare(example.neq, value) !== 0;
    }

    if ('between' in example ) {
      return ( testInEquality({gte:example.between[0]}, value) &&
      testInEquality({lte:example.between[1]}, value) );
    }

    if (example.like || example.nlike) {

      var like = example.like || example.nlike;
      if (typeof like === 'string') {
        like = toRegExp(like);
      }
      if (example.like) {
        return !!new RegExp(like).test(value);
      }

      if (example.nlike) {
        return !new RegExp(like).test(value);
      }
    }

    if (testInEquality(example, value)) {
      return true;
    }
  }
  // not strict equality
  return (example !== null ? example.toString() : example)
      == (value != null ? value.toString() : value);
};

/**
 * Compare two values
 * @param {*} val1 The 1st value
 * @param {*} val2 The 2nd value
 * @returns {number} 0: =, positive: >, negative <
 * @private
 */
var compare = function (val1, val2) {
  if(val1 == null || val2 == null) {
    // Either val1 or val2 is null or undefined
    return val1 == val2 ? 0 : NaN;
  }
  if (typeof val1 === 'number') {
    return val1 - val2;
  }
  if (typeof val1 === 'string') {
    return (val1 > val2) ? 1 : ((val1 < val2) ? -1 : (val1 == val2) ? 0 : NaN);
  }
  if (typeof val1 === 'boolean') {
    return val1 - val2;
  }
  if (val1 instanceof Date) {
    var result = val1 - val2;
    return result;
  }
  // Return NaN if we don't know how to compare
  return (val1 == val2) ? 0 : NaN;
};

/**
 * Evaluates inequalities
 * @param the value to look for
 * @param the value to examine
 */
function testInEquality(example, val) {
  if ('gt' in example) {
    return compare(val, example.gt) > 0;
  }
  if ('gte' in example) {
    return compare(val, example.gte) >= 0;
  }
  if ('lt' in example) {
    return compare(val, example.lt) < 0;
  }
  if ('lte' in example) {
    return compare(val, example.lte) <= 0;
  }
  return false;
};

/**
 * Builds a regular expression from plain text pattern
 * @param {String} pattern
 */
var toRegExp = function(pattern) {
  if (pattern instanceof RegExp) {
    return pattern;
  }
  var regex = '';
  // Escaping user input to be treated as a literal string within a regular expression
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions#Writing_a_Regular_Expression_Pattern
  pattern = pattern.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
  for (var i = 0, n = pattern.length; i < n; i++) {
    var char = pattern.charAt(i);
    if (char === '\\') {
      i++; // Skip to next char
      if (i < n) {
        regex += pattern.charAt(i);
      }
      continue;
    } else if (char === '%') {
      regex += '.*';
    } else if (char === '_') {
      regex += '.';
    } else if (char === '.') {
      regex += '\\.';
    } else if (char === '*') {
      regex += '\\*';
    }
    else {
      regex += char;
    }
  }
  return regex;
};

/**
 * Categorizes a value, returning its type and value representation, suitable for serialization
 * @param the value to category
 */
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
          value = JSON.stringify( merge( null, value ) );
        }
        break;
      }

    default:
      return;
      break;
  }
  return { type: propertyType, value: value };
};

/**
 * Splits the object into pieces, defined by the property value types
 * @param {Object} the data to split
 * @param the identifier property name
 */
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

/**
 * Splits the object into pieces, defined by the property value types, then prepares CQL
 * statement and params, getting ready for object persistence.
 * @param {String} the model the object falls into
 * @param {Object} the data to serialize
 * @param the identifier property name
 * @param controls the parameter output. when false, the params follow map<..> pattern, when true they are listed standalone.
 */
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

/**
 * Builds a JavaScript object from a Cassandra record
 * @param the identifier property name
 * @param {Object} the Cassandra record with maps by type
 * @param {Object} hash with the fields names to include during "deserialization"
 */
var objectFromRecord = (function() {
  var postProcess = function( type, obj, fields ) {
    if ( obj ) {
      if (type === "object") {
        for (var key in obj) {
          if (fields && !fields[key]) {
            continue;
          }
          obj[key] = JSON.parse(obj[key]);
        }
      }
      if (fields) {
        var filteredObj = {}, it;
        for (var key in fields) {
          if (( it = obj[key] ) !== undefined) {
            filteredObj[key] = it;
          }
        }
        obj = filteredObj;
      }
    }
    return obj;
  };
  return function (idName, rec, fields) {

    var obj = null;

    if (!rec) {
      return obj;
    }

    for (var type in {"object": 1, "number": 1, "string": 1, "date": 1}) {
      obj = merge( obj, postProcess( type, rec[type], fields ) );
    }

    if (!obj) {
      obj = {};
    }

    if ( ( !fields ) || ( fields && fields[ idName ] ) ) {
      var id = rec["id"];
      if (id) {
        obj[idName] = parseInt(id);
      }
    }

    return obj;

  };
})();

/**
 * Enables unique constrains, when in the model, in conjunction with _releaseOrphanLocks
 * @param {String} the data model
 * @param {Object} the data to evaluate against all other model instances
 * @param {Function} a client function to pass through into the feedback callback
 * @param {Function} the callback receiving feedback from the unique constraint eval. continuation is added as a second param
 */
Cassandra.prototype._enforceUniqueConstraint = function( model, data, continuation, callback ) {
  var self = this;
  var id = this.getIdValue( model, data );
  var props = this._models[model].properties;
  var index, value, cql = [], p = [], pt = [];
  for( var field in props ) {
    if ( ( index = ( props[field] || {} ).index ) && index.unique ) {
      if ( ( value = data[field] ) !== undefined ) {
        value = getType(value);
        value = String(value.value);
        cql.push("insert into constraint ( id, model, key, value ) values ( ?, ?, ?, ? ) if not exists");
        p.push(id, model, field, value);
        pt.push("text", "text", "text", "text");
      }
    }
  }
  if ( cql.length ) {
    self.client.execute( "begin batch " + cql.join( " " ) + " apply batch;", p, { hints: pt  },  function( err, all ) {
      var failed = false, ids = {}, keys = [];
      if ( all && ( all = all.rows ) ) {
        for ( var i = 0, l = all.length, it; i < l; i ++ ) {
          it = all[ i ];
          if ( !( it["[applied]"] || ( it.id === String( id ) ) ) ) {
            ids[ it.id ] = 1; keys.push( it.key );
            failed = true;
          }
        }
      }
      callback && callback( failed ? new Error( "Unique constraint violation " + model + ".[" + keys.join(", " ) + "]" ): undefined, function( err, p1, p2 ) {
        continuation( err, p1, p2 );
        process.nextTick( function() {
          self._releaseOrphanLocks(model, id);
        });
      } );
      if ( ( ids = Object.keys( ids ) ).length ) {
        // see if the locks point to existing items. if not,
        // release the lock with 1 sec delay
        var t = setTimeout( function() {
          clearTimeout( t );
          self._releaseOrphanLocks( model, ids );
        }, 1000 );
      }
    } );
  }
  else {
    callback && callback( undefined, continuation );
  }
};

/**
 * Inpects the object identified by model/index and removes unused entries from the index, allowing for others to enter the data store.
 * @param {String} the data model
 * @param {Object} the identifier uniquely identifying the data instance together with the model
 * @param {Function} the callback receiving feedback from the operation
 */
Cassandra.prototype._releaseOrphanLocks = function( model, id, callback ) {
  var self = this;
  var props = this._models[model].properties;
  var index, indexFields = { };
  for( var field in props ) {
    if (( index = ( props[field] || {} ).index ) && index.unique) {
      indexFields[ field ] = 1;
    }
  }
  if ( Object.keys( indexFields).length ) {
    if (!Array.isArray(id)) {
      id = [id];
    }
    var er, items;
    var processItem = function() {
      var item, id;
      if ( items.length ) {
        self.client.execute( "select key, value from constraint where id = ?", [ id = String( self.getIdValue( model, ( item = items.shift() ) ) ) ], function( err, all ) {
          var k, v, p = [], cql = [];
          if ( all && ( all = all.rows ) && ( all = all[ 0 ] ) ) {
            if ( ( ( v = item[ k = all.key ] ) === undefined  ) || ( String( getType( v ).value ) !== all.value ) ) {
              cql.push( "delete from constraint where model = ? and key = ? and value = ? if id = ?" );
              p.push( model, k, all.value, id );
            }
          }
          if (p.length) {
            self.client.execute( "begin batch " + cql.join( " " ) + " apply batch;", p, function( e ) {
              if ( e ) {
                debug('WARN: _releaseOrphanLocks(model:%j id:%j ) failed removing lock with \'%j\'', model, id, e );
              }
              processItem();
            } );
          }
          else {
            processItem();
          }
        } );
      }
      else {
        callback && callback(er);
      }
    };

    self.find( model, id, function ( err, all ) {
      items = all || [];
      if ( !Array.isArray( items ) ) {
        items = [ items ];
      }
      if ( !( er = er || err ) ) {
        // intersect with the identifiers passed in
        var ids = {}, i, l;
        for( i = 0, l = id.length; i < l; i ++ ) {
          ids[ id[ i ] ] = 1;
        }
        for( i = 0, l = items.length; i < l; i ++ ) {
          delete ids[ items[ i ] ];
        }
        for( i in ids ) {
          l = {}; self.setIdValue( model, l, i );
          items.push( l );
        }
      }
      processItem();
    } );
  }
  else {
    callback && callback();
  }
};

/**
 * Inpects the object looking for id and assigns one, if no id has been found
 * @param {String} the data model
 * @param {Object} the data to attach id to
 */
Cassandra.prototype._getOrCreateId = function( model, data ) {
  var currentId = this.collectionSeq(model);
  if (currentId === undefined) { // First time
    currentId = this.collectionSeq( model, ( new Date().valueOf() ) );
  }
  var id = parseInt( this.getIdValue( model, data ) ) || currentId;
  if (id > currentId) {
    // If the id is passed in and the value is greater than the current id
    currentId = id;
  }
  this.collectionSeq( model, Number(currentId) + 1 );
  return id;
}

/**
 * Inspects a filter and builds a CQL where clause, restricting the Cassandra output as much as possible. Portions of
 * the filter Cassandra has no support for are ignored here and applied later when data is in memory.
 * @param {String} the data model
 * @param {Object} the where portion of a filter
 * @param {Object} the context in which tha call takes place, allowing for recursion
 */
Cassandra.prototype._queryFromFilter = (function() {
  var typeMap = {
    "string": "text",
    "date": "timestamp",
    "number": "double",
    "object": "text" };
  return function( model, where, ctx ) {
    var idField = this.idName( model );
    var callIn = false, value, it;
    if ( !ctx ) {
      ctx = { cql: [ "model = ?" ],
        param: [ model ],
        paramType: [ "text" ],
        _push: function( c, p, pt ) {
          this.cql.push( c );
          this.param.push( p );
          this.paramType.push( pt );
        },
        _needsMemoryFilter: false
      };
    }
    for ( var key in where ) {
      callIn = false;
      switch ( key ) {

        case idField:
          ctx._push( "id = ?", where[key], "text" );
          break;

        // not equal cannot be handled
        case "neq":

        // like and nlike cannot be handled
        case "like":
        case "nlike":

        // inequalities cannot be handled
        case "gt":
        case "gte":
        case "lt":
        case "lte":
          debug('WARN: queryFromFilter(model:%j where:%j ctx:%j) with unsupported operation \'%j\'', model, where, ctx, key);
          ctx._needsMemoryFilter = true;
          break;

        // geo near is simply ignored
        case "near":
          break;

        // and is the only boolean item we can handle
        case "and":
          callIn = true;
        // or, in, not in cannot be handled
        case "or":
        case "inq":
        case "nin":
        // between cannot be handled
        case "between":
          if ( Array.isArray( value = where[key] ) ) {
            if ( callIn ) {
              for ( var i = 0, l = value.length; i < l; i ++ ) {
                this._queryFromFilter( model, value[i], ctx );
              }
              callIn = false;
            }
            else {
              debug('WARN: queryFromFilter(model:%j where:%j ctx:%j) with unsupported operation \'%j\'', model, where, ctx, key);
              ctx._needsMemoryFilter = true;
            }
            break;
          }

        default:
          value = getType( it = where[ key ] );
          if ( ( value.type === "object" ) && ( ( it = Object.keys( it ) ).length === 1 ) ) {
            it = it[ 0 ] || "pass";
            switch ( it ) {
              case "neq":
              // like and nlike cannot be handled
              case "like":
              case "nlike":
              // inequalities cannot be handled
              case "gt":
              case "gte":
              case "lt":
              case "lte":
              // geo near is simply ignored
              case "near":
              // and is the only boolean item we can handle
              case "and":
              // or, in, not in cannot be handled
              case "or":
              case "inq":
              case "nin":
              // between cannot be handled
              case "between":
                it = null;
                break;

              default:
                break;
            }
            if (!it) {
              // skip the filters we cannot
              // handle within Cassandra
              ctx._needsMemoryFilter = true;
              continue; // for (...)
            }
          }
          ctx._push( value.type + " contains ?", value.value, typeMap[ value.type ] );
          // since Cassandra can only tell a particular value does exist in
          // colleciton, rather the value exists AND associats with a particular
          // field, we need to run the more restrictive filter once the data arrives
          ctx._needsMemoryFilter = true;
          break;
      }
    }
    return ctx;
  }
} )();

/**
 * Keeps the latest sequence value for a model
 * @param {String} the data model
 * @param the last known sequence value in use
 */
Cassandra.prototype.collectionSeq = function( model, val ) {
  if (arguments.length > 1) this.ids[model] = val;
  return this.ids[model];
};

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Cassandra.prototype._create = function ( model, data, cb, createAlways ) {

  var self = this;

  var id = self._getOrCreateId(model, data);
  var props = self._models[model].properties;
  var idName = self.idName(model);
  id = (props[idName] && props[idName].type && props[idName].type(id)) || id;
  self.setIdValue( model, data, id );

  self._enforceUniqueConstraint( model, data, cb, function( er, callback ) {

    if ( er ) {
      callback && callback( er );
      return;
    }

    var cql = getQueryFieldsAndParams(model, data, idName), fields;
    self.client.execute("insert into data ( " + ( fields = cql[0] ).join(", ") +
        " ) values ( " + ( new Array(fields.length) ).join("?, ") + "? )" + ( createAlways ? "" : " if not exists" ),
        cql[1], {"hints": cql[2]}, function (err, result) {
          if (err) {
            callback && callback(err);
          }
          else if (result && ( result = result.rows ) && ( result = result[0] ) && !result["[applied]"]) {
            callback && callback(new Error('Duplicate entry for ' + model + '.' + idName), null, {isNewInstance: false});
          }
          else {
            callback && callback(null, data, {isNewInstance: true});
          }
        });
  });
};

/**
 * Save a model instance
 */
Cassandra.prototype._save = function ( model, data, cb ) {

  var self = this;
  self._enforceUniqueConstraint( model, data, cb, function( er, callback ) {

    if ( er ) {
      callback && callback( er );
      return;
    }

    var props = self._models[model].properties;
    var idName = self.idName(model);

    var bundle = getQueryFieldsAndParams(model, data, idName, true), fields;
    var params = bundle[1], paramTypes = bundle[2];
    cql = bundle[0];

    cql = "update data set " + cql.slice(0, cql.length - bundle.primaryKeyFields).join("[?] = ?, ") + "[?] = ?"
        + " where " + cql.slice(cql.length - bundle.primaryKeyFields).join(" = ? and ") + " = ?";
    self.client.execute(cql, params, {"hints": paramTypes}, function (err, result) {
      if (err) {
        callback && callback(err);
      }
      else {
        callback && callback(null, data, {isNewInstance: false});
      }
    });
  });
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

  var initStorage = function( pool, callback ) {
    // we've got the system namespace
    // now look for the keyspace init script
    var sp = pool;

    var cql = "\
            create table data ( \
                model text, \
                id text, \
                type text, \
                number map<text,double>, \
                string map<text,text>, \
                date   map<text,timestamp>, \
                object map<text,text>, \
                primary key ( ( model, id ), type ) );\
            create index idx_data_numeric   on data(number); \
            create index idx_data_string    on data(string); \
            create index idx_data_date      on data(date); \
            create index idx_data_model     on data(model); \
            create index idx_data_id        on data(id); \
            create table constraint ( \
                model text, \
                key text, \
                value text, \
                id text, \
                primary key ( ( model, key ), value ) ); \
            create index idx_constraint_id    on constraint( id ); \
            create index idx_constraint_model on constraint(model); \
            create index idx_constraint_key   on constraint(key); ";

    cql = cql.replace( /\/\/.*$/mg, "" );
    cql = cql.split( ";" );

    var simpleCql, errors = [];

    var executionWrapper = function( errs ) {
      callback && callback( errs ); }

    var runCql = function() {
      if ( !( simpleCql = ( simpleCql || "" ).replace( /^\s{1,}|\s{1,}$/g, "" ) ) ) {
        if ( cql.length ) {
          simpleCql = cql.shift();
          runCql(); }
        else {
          executionWrapper( errors.length ? errors: null );
          return; } }
      sp.execute( simpleCql, function( er ) {
        if ( er ) {
          if ( !( ( er.message.indexOf( "already existing column family" ) > -1 ) || ( er.message.indexOf( "already exists" ) > -1 ) ) ) {
            errors.push( { cql: simpleCql, error: er } );
          }
        }
        simpleCql = cql.shift( );
        runCql(); } ); }

    simpleCql = cql.shift();
    runCql();

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
      initStorage( pool, function( errors ) {

          // TODO: Check the error(s) before calling notify()
          delete dataSource.initializing;
          dataSource.initialized = true;
          notify();

        } );
      }
    );
  }
})();

/**
 * Update a model instance or create a new model instance if it doesn't exist
 */
Cassandra.prototype.updateOrCreate = function ( model, data, callback ) {
  var id = this._getOrCreateId( model, data );
  var self = this;
  self.exists( model, id, function( err, exists ) {
    if ( err ) {
      cb( err );
    }
    else if (!exists) {
      self._create( model, data, callback, true ); }
    else {
      self._save( model, data, callback );
    }
  } );
};

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Cassandra.prototype.create = function ( model, data, callback ) {
  var self = this;
  this._create( model, data, function( err, data, status ) {
    callback && callback( err, ( model && data && self.getIdValue( model, data ) ), status );
  }, false );
};

/**
 * Check if a model instance exists by id
 */
Cassandra.prototype.exists = function exists(model, id, callback) {
  this.client.execute( "select id from data where model = ? and id = ?", [ String( model ) , String( id ) ] , { hints: [ "text", "text" ] }, function( err, result ) {
    if ( result && ( result = result.rows ) ) {
      callback && callback( err, !!result.length );
    }
    else {
      callback && callback( err );
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
      self._save( model, data, cb );
    }
  } );
};

/**
 * Saves instance changes, leaving existing object properties intact.
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Cassandra.prototype.save = Cassandra.prototype._save;

/**
 * Delete a model instance by id
 */
Cassandra.prototype.destroy = function destroy(model, id, callback) {
  var self = this;
  self.client.execute( "delete from data where model = ? and id = ?", [ String( model ), String( id ) ], { hints: [ "text", "text" ] },
      function( e ) {
        callback( e );
        process.nextTick( function() {
          self._releaseOrphanLocks(model, id);
        } );
      } );
};

/**
 * Find a model instance by id
 * @param {Function} callback - you must provide an array of results to the callback as an argument
 */
Cassandra.prototype.find = function find(model, id, callback) {
  var self = this;
  var idName = self.idName( model );
  if ( !Array.isArray( id ) ) {
    id = [ id ];
  }
  else {
    id = id.slice();
  }
  id.unshift( model );
  id = id.map(String);
  self.client.execute( "select id, date, number, object, string from data where model = ? and id in ( " + fill( id.length - 1, "?" ).join( "," ) + ")", id,
      { hints: fill( id.length, "text" ) }, function( err, all ) {
        var items = [];
        if ( all && ( all = all.rows ) ) {
          for( var i = 0, l = all.length; i < l; i ++ ) {
            items.push(objectFromRecord(idName, all[i]));
          }
        }
        callback && callback(err, ( items.length <= 1 ) ? items[ 0 ]: items );
      } );
};

/**
 * Query model instances by the filter
 */
Cassandra.prototype.all = function all( model, filter, callback ) {
  debug('all(model:%j filter:%j)', model, filter);
  var queryDef = this._queryFromFilter( model, filter.where );
  var self = this, fields = null, fld;
  if ( ( fld = filter.fields ) && fld.length ) {
    fields = { };
    for( var i = 0, l = fld.length; i < l; i ++ ) {
      fields[ fld[ i ] ] = 1;
    }
  }
  var items = [], idName = self.idName( model );
  self.client.eachRow( "select id, date, number, object, string from data where " + queryDef.cql.join( " and " ) + " allow filtering", queryDef.param, { hints: queryDef.paramType, autoPage: true },
    function( n, rec ) {
      items.push( objectFromRecord( idName, rec ) );
    },
    function( err ) {
      if ( !filter ) {
        filter = { };
      }
      if ( queryDef._needsMemoryFilter ) {
        // do we need some filtration?
        if ( filter.where && items ) {
          items = items.filter( applyFilter( filter ) );
        }
      }
      if (!filter.order) {
        var idNames = self.idNames(model);
        if (idNames && idNames.length) {
          filter.order = idNames;
        }
      }
      // do we need some sorting?
      if (filter.order) {
        var orders = filter.order;
        if (typeof filter.order === "string") {
          orders = [filter.order];
        }
        orders.forEach(function (key, i) {
          var reverse = 1;
          var m = key.match(/\s+(A|DE)SC$/i);
          if (m) {
            key = key.replace(/\s+(A|DE)SC/i, '');
            if (m[1].toLowerCase() === 'de') reverse = -1;
          }
          orders[i] = {"key": key, "reverse": reverse};
        });
        items = items.sort(sorting.bind(orders));
      }

      // limit/skip
      var skip = filter.skip || filter.offset || 0;
      var limit = filter.limit || items.length;
      if ( ( skip !== 0 ) || ( limit !== items.length ) ) {
        items = items.slice(skip, skip + limit);
      }

      // strip, preserving only the fields the user requested
      if ( fields ) {
        for (var i = 0, l = items.length, it, key, value, filtered; i < l; i++) {
          it = items[i];
          filtered = {};
          for (key in fields) {
            if (( value = it[key] ) !== undefined) {
              filtered[key] = value;
            }
          }
          items[i] = filtered;
        }
      }

      callback( err, items );
    });
};

/**
 * Delete all model instances
 */
Cassandra.prototype.destroyAll = function destroyAll(model, where, callback) {
  debug('destroyAll(model:%j where:%j)', model, where);
  // TODO (Krassi): where can be callback as well
  var self = this, idName = this.idName(model);
  if (typeof(where) === "function") {
    callback = where;
    where = undefined;
  }
  if ( where ) {
    self.all( model, { where: where, fields: [ idName ] }, function( err, items ) {
      if ( err ) {
        callback( err );
      }
      else if ( items.length > 0 ) {
        var p = [ model ], pt = [ "text" ];
        for( var i = 0, l = items.length; i < l; i ++ ) {
          p.push( String( items[i][ idName ] ) );
          pt.push( "text" );
        }
        var ids = p.slice( 1 );
        self.client.execute( "delete from data where model = ? and id in ( " + fill( p.length - 1, "?" ).join( "," ) + " );", p, { hints: pt }, function( er ) {
          callback( er );
          process.nextTick( function() {
            self._releaseOrphanLocks( model, ids );
          } );
        } );
      }
      else {
        callback();
      }
    });
  }
  else {
    var p = [ model], pt = [ "text"];
    self.client.eachRow( "select id from data where model = ?", [model], { hints: [ "text" ] },
      function( n, row ) {
        p.push( row.id );
        pt.push( "text" );
      },
      function( err, result ) {
        var wrap = function(er) {
          p = [ model];
          pt = [ "text"];
          if ( er ) {
            debug('ERROR: destroyAll(model:%j where:%j) has failed with %j', model, where, er);
            callback && callback(er);
          }
          else if ( result.nextPage ) {
            result.nextPage();
          }
          else {
            callback && callback();
          }
        };
        if ( p && ( p.length > 1 ) ) {
          var ids = p.slice(1);
          self.client.execute( "delete from data where model = ? and id in ( " + fill( p.length - 1, "?" ).join( "," ) + " );", p, { hints: pt }, function( er ) {
            wrap( er );
            process.nextTick( function() {
              self._releaseOrphanLocks(model, ids);
            } );
          } );
        }
        else {
          wrap( err );
        }
      } );
  }
};

/**
 * Count the model instances by the where criteria
 */
Cassandra.prototype.count = function count(model, callback, where) {
  debug('count(model:%j where:%j)', model, where);
  this.all( model, ( ( where ) ? { where: where }: undefined ), function( err, items ) {
    callback( err, ( ( items && items.length ) || 0 ) );
  } );
};
