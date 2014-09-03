/**
 * Created by pavelk on 9/1/14.
 */

var pg = require("pg");
var util = require("util");
var _ = require("lodash");
var async = require("async");

(function(){

    var redshift = {};

    redshift.connect = function redshiftConnect(options, callback){
        this.pgClient = new pg.Client(options.connectionString);
        if (options.password) {
            this.pgClient.password = options.password;
        }
        console.log("connecting to redshift");
        this.pgClient.connect(callback);
    };

    redshift.analyzeCompression = function redshiftAnalyzeCompression(options, callback){
        console.log("start analyze compression");

        var query = util.format("ANALYZE COMPRESSION %s.%s;",
            options.schema, options.table);

        this.query(query, function(err, result){
            if (err) {
                console.log("error in analyzeCompression");
                callback(err);
            } else {
                var result = _.map(result.rows, function(row){
                    return { column : row.Column, encoding : row.Encoding };
                });
                callback(null, result);
            }
        });

    };


    redshift.getTableDetails = function redshiftGetTableDetails(options, callback){

        //todo: check if it will work without user and public
        var searchPath = util.format("set search_path=\"$user\", public, %s",
            options.schema);

        var query = util.format("%s; SELECT * from pg_table_def " +
            "where schemaname='%s' and tablename='%s';",
        searchPath, options.schema, options.table);

        this.query(query, function(err, data){
            if (err) {
                console.log("error in getTableDetails");
                callback(err);
            } else {
                callback(null, data.rows);
            }
        });
    };

    redshift.query = function(query, callback){
        console.log("Running query -->", query);
        this.pgClient.query(query, callback);
    };

    redshift.createColumn = function(options, callback){
        options.tempColumnName = options.column + "_encoding_temp_name";

        //console.log(options);

        var query = util.format("ALTER TABLE %s.%s " +
            "ADD COLUMN %s %s ENCODE %s",
            options.schemaname, options.tablename, options.tempColumnName,
            options.type, options.encoding
        );

        options.notnull ? query += " NOT NULL " : 0;
        //options.sortkey ? query += " SORT KEY " + options.sortkey : 0;
        //options.distkey ? query += " DIST KEY " : 0;

        this.query(query, callback);
    };

    redshift.copyColumn = function(options, callback){

        var query = util.format("UPDATE %s.%s SET %s = %s;",
            options.schemaname, options.tablename, options.tempColumnName,
            options.column);

        this.query(query, callback);
    };

    redshift.dropColumn = function(options, callback){

        var query = util.format("ALTER TABLE %s.%s DROP COLUMN %s;",
            options.schemaname, options.tablename, options.column);

        this.query(query, callback);
    };

    redshift.renameColumn = function(options, callback){

        var query = util.format("ALTER TABLE %s.%s RENAME COLUMN %s TO %s;",
            options.schemaname, options.tablename,
            options.tempColumnName, options.column);

        this.query(query, callback);
    };

    redshift.runEncodeColumn = function redshiftRunEncodeColumn(column, parentCallback){
        console.log(util.format("start encode process on column: %s",
            column.column));

        var self = this;
        async.waterfall([
            function createTempColumn(callback){
                self.createColumn(column, callback);
            },
            function copyColumnData(data, callback){
                self.copyColumn(column, callback);
            },
            function removeOldColumn(data, callback){
                self.dropColumn(column, callback);
            },
            function renameNewColumn(data, callback){
                self.renameColumn(column, callback);
            }
        ], function(err, data){
            if (err) {
                parentCallback(err);
            } else {
                console.log("finished encoding column %s", column.column);
                parentCallback(null, null);
            }
        });
    };

    module.exports = redshift;

}());