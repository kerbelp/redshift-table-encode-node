

var RedshiftEncodeTable = require("./redshift-table-encode");

var options = {
    "schema": "tableschema",
    "table": "tablename",
    "connectionString": "connectio string as used in pg",
    "password": "password"
};

RedshiftEncodeTable.encode(options, function(err, data){
    if (err) {
        console.log(err);
    } else {
        console.log(data);
    }
});
