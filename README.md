redshift-table-encode-node
=====================

A node example to encode columns of a redshift table.

Best Practice when using Amazon Redshift is to have your columns encoded
with the recommendation from ANALYZE COMPRESSION command.

"redshift_table_encode" is a module that gets the details of table
and do the following steps:

  1. connect to redshift using pg - should be in this format:
           postgres://USER:PASSWORD@DOMAIN-OF-REDSHIFT:PORT/DBNAME?tcpKeepAlive=true
  2. analyze copmpression - to get the recommended encoding to all columns
  3. get current status of columns
  4. filter only the columns that are not encoded
  5. encode every column separately:
      5.1 create a new encoded column with a temp name
      5.2 update the new column with all the data from the original column
      5.3 drop the original column
      5.4 rename new column to original name

I'm open to suggestions, code reviews and contributions :)

hope this would be useful to you.