CREATE EXTERNAL TABLE table2
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.url'='hdfs:///user/root/config/table2.avsc')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/root/tables/table2'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/root/config/table2.avsc');
