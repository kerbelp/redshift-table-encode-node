/**
 * redshift-table-encode
 *
 * steps:
 * 1. connect to redshift using pg
 * 2. analyze copmpression - to get the recommended encoding to all columns
 * 3. get current status of columns
 * 4. filter only the columns that are not encoded
 * 5. encode every column separately:
 *     5.1 create a new encoded column with a temp name
 *     5.2 update the new column with all the data from the original column
 *     5.3 drop the original column
 *     5.4 rename new column to original name
 *
 */

var async = require("async");
var _ = require("lodash");
//var util = require('util');

var redshift = require("./redshift");


var redshift_table_encode = function(){};

/**
 * options should be in this format:
 {
     "schema": "",
     "table": "",
     "connectionString": "",
     "password": only required if contains ":"
 }
 */
redshift_table_encode.prototype.encode = function(options, parentCallback){
    var self = this;

    if (!options.schema || !options.table || !options.connectionString) {
        return parentCallback("Error: missing one of the following fields:" +
            " schema, table or connectionString");
    }

    async.waterfall([
        function connectToRedshift(callback){
            redshift.connect(options, callback);
        },
        function runAnalyzeCompression(connection, callback){
            redshift.analyzeCompression(options, callback);
        },
        function getTableCurrentDetails(encodingRecommendation, callback){
            redshift.getTableDetails(options, function(err, result) {
                if (err) {
                    callback(err);
                } else{
                    callback(null, encodingRecommendation, result);
                }
            });
        },
        function findColumnsToEncode(encodingRecommendation, tableDetails, callback){

            console.log("-----------");
            console.log("total columns:" + encodingRecommendation.length);

            var columns = self.filterColumnsToEncode(encodingRecommendation, tableDetails);
            callback(null, columns);
        },
        function iterateOverColumnsAndEncode(columns, encodeTableCallback){
            console.log("-----------");
            console.log("total columns to encode: " + columns.length);
            console.log("-----------");
            async.eachSeries(columns, function(column, callback){
                redshift.runEncodeColumn(column, callback);
            }, function(err){
                if (err) {
                    encodeTableCallback(err);
                } else {
                    encodeTableCallback();
                }
            });
        }
    ], function(err, data){
        if (err) {
            parentCallback(err);
        } else {
            parentCallback(null, "finished encoding all columns");
        }
    });
};

redshift_table_encode.prototype.filterColumnsToEncode = function(recommended, current){

    var columnsToEncode = [];

    _(recommended).forEach(function(recommendedColumnData){
        var column = _.findWhere(current, function(currentColumnData){
            return recommendedColumnData.column === currentColumnData.column &&
                recommendedColumnData.encoding !== currentColumnData.encoding
        });
        if (column) {
            column.encoding = recommendedColumnData.encoding;
            columnsToEncode.push(column);
        }
    });

    return columnsToEncode;
};

module.exports = new redshift_table_encode();
//encodeTable(options);