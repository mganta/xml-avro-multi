CREATE EXTERNAL TABLE table3
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.url'='hdfs:///user/root/config/table3.avsc')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/root/tables/table3'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/root/config/table3.avsc');
