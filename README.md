redshift-table-encode-node
=====================

A node example to encode columns of a redshift table.

Best Practice when using Amazon Redshift is to have your columns encoded
with the recommendation from ANALYZE COMPRESSION command.

"redshift_table_encode" is a module that is used to encode the table in two
ways:<br><br>
A. tabularEncode -<br><br> useful for first time encoding (most of the columns are not encoded)
    encode the whole table with the following steps:<br>
    1. connect to redshift using pg - should be in this format:<br>
            postgres://USER:PASSWORD@DOMAIN-OF-REDSHIFT:PORT/DBNAME?tcpKeepAlive=true<br>
    2. analyze copmpression - to get the recommended encoding to all columns<br>
    3. create new temp table with the encoded columns<br>
    4. insert all the data from the table to the temp table<br>
    5. drop the not encoded table<br>
    6. rename the temp table to the original name<br>



B. columnarEncode<br><br>

  1. connect to redshift using pg - should be in this format:
           postgres://USER:PASSWORD@DOMAIN-OF-REDSHIFT:PORT/DBNAME?tcpKeepAlive=true
  2. analyze copmpression - to get the recommended encoding to all columns<br>
  3. get current status of columns<br>
  4. filter only the columns that are not encoded<br>
  5. encode every column separately:<br>
      5.1 create a new encoded column with a temp name<br>
      5.2 update the new column with all the data from the original column<br>
      5.3 drop the original column<br>
      5.4 rename new column to original name<br>

I'm open to suggestions, code reviews and contributions :)

hope this would be useful to you.