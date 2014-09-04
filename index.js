

var redshiftEncodeTable = require('./redshift-table-encode');

var options = {
    'schema': 'tableschema',
    'table': 'tablename',
    'connectionString': 'connection string as used in pg',
    'password': 'password'
};

/**
 * example 1:
 * encode whole table using a temp table.
 * useful for first time encoding (most of the columns are not encoded)
 */
redshiftEncodeTable.tabularEncode(options, function(err, data){
    if (err) {
        console.log(err);
    } else {
        console.log(data);
    }
});

/**
 * example 2:
 * encode only the columns in the table that are not encoded.
 * useful for encoding new columns that were added to an existing table.
 * a good practice is to run a vacuum on the table after the process.
 */
redshiftEncodeTable.columnarEncode(options, function(err, data){
    if (err) {
        console.log(err);
    } else {
        console.log(data);
    }
});
