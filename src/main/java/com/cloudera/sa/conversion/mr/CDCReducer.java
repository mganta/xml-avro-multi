package com.cloudera.sa.conversion.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CDCReducer extends Reducer<Text, MapWritable, AvroKey<GenericRecord>, NullWritable> {
	private AvroMultipleOutputs avroMultipleOutputs;
	private Map<String, Schema> schemaMap;
	private String dataSetHour;
	private String dataSetDate;
	 public void setup(Context context) {
		 avroMultipleOutputs = new AvroMultipleOutputs(context);
		 
		 String configPath = context.getConfiguration().get("SchemaConfig");
		 dataSetHour = context.getConfiguration().get("DataSetHour");
		 dataSetDate = context.getConfiguration().get("DataSetDate");
		 //TO-DO handle path issues
		 if(configPath == null) 
			 System.out.print("Config path is null. Exiting...");
		 
		 schemaMap = new HashMap<String, Schema>();
		 
		 try {
		 FileSystem fs = FileSystem.get(context.getConfiguration());
         FileStatus[] files = fs.listStatus(new Path(configPath));
         for(FileStatus file : files) {
            Schema sch = new Schema.Parser().parse(fs.open(file.getPath()));
         	schemaMap.put(sch.getName(), sch);
         }
		 } catch (IOException e) {
			 e.printStackTrace();
		 }
		
		 }
	 
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException {
		
		for (MapWritable event : values) {
			try {
				if(schemaMap.get(key.toString()) != null)
				avroMultipleOutputs.write(key.toString(), new AvroKey<GenericRecord>(generateAvroRecord(schemaMap.get(key.toString()), event)), NullWritable.get(), key.toString() + "/" + key.toString() + "_" + dataSetDate + "_" + dataSetHour);
			} catch (InterruptedException e) {
				System.out.print("schema issue at " + key.toString());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private GenericRecord generateAvroRecord(Schema schema, MapWritable event) {
		
		if(schema == null)
			System.out.println("Schema is null ....");
		
		GenericRecord datum = new GenericData.Record(schema);
		
		//load known fields
		List<Field> flds = schema.getFields();
		for (Field fld : flds) {
			String fldName = fld.name();
			Text txtFld = new Text(fldName);
			if(event.containsKey(new Text(fldName))) {
				switch (fld.schema().getType()) {
				case STRING: 
				     datum.put(fldName, new String(((BytesWritable)event.get(txtFld)).getBytes()).trim()); 
				     event.remove(txtFld);
					 break;
				case UNION: 
					for (Schema fldType : fld.schema().getTypes())
						if(!fldType.getName().equals("null")) {
							if (fldType.getName().equals("double"))
							    datum.put(fldName, Double.valueOf(new String(((BytesWritable)event.get(txtFld)).getBytes()))); 
							if (fldType.getName().equals("int"))	
							 datum.put(fldName, Integer.valueOf(new String(((BytesWritable)event.get(txtFld)).getBytes()))); 
							if (fldType.getName().equals("string"))
							    datum.put(fldName, new String(((BytesWritable)event.get(txtFld)).getBytes()).trim()); 
							event.remove(txtFld);
							break;
						}
					break;
				default:
					break;
				
				}
			}
		}
		
		//load others map
		Map<String,Object> othersMap = new HashMap<String,Object>();
		for (Map.Entry<Writable, Writable> e : event.entrySet())
		{
		    othersMap.put(e.getKey().toString(), new String(((BytesWritable) e.getValue()).getBytes()).trim()); 
		}
		datum.put("others", othersMap);
		return datum;
	}
	
	public void cleanup(Context context) throws IOException {
		 try {
			 avroMultipleOutputs.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 }
}
