package com.cloudera.sa.conversion.mr;


import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CDCTransformerJob extends Configured implements Tool 
{
	        private Configuration config = new Configuration();

	        /**
	         * Tool interface
	         */
	        public Configuration getConf()
	        {
	                return config;
	        }

	        /**
	         * Tool interface
	         */
	        public void setConf(Configuration config)
	        {
	                this.config = config;
	        }

	        public int run(String[] args) throws Exception
	        {
	    		if (args.length != 3) {
	    			System.err.printf("Usage: %s [generic options] <input> <output> <config> \n",
	    					getClass().getSimpleName());
	    			ToolRunner.printGenericCommandUsage(System.err);
	    			return -1;
	    		}

	        	    config.set("SchemaConfig", args[2]);
	        	    String dataSetDate = args[0].substring(args[0].indexOf('=') + 1, args[0].lastIndexOf('-'));
	        	    String dataSetHour = args[0].substring(args[0].lastIndexOf('-') + 1);
	        	    
	        	    System.out.println("processing data for date: "+ dataSetDate + " & hour: " + dataSetHour);
	        	    
	        	    config.set("DataSetDate", dataSetDate);
	        	    config.set("DataSetHour", dataSetHour);
	        	    
	                Job job = Job.getInstance(config);
	                
	                job.setJarByClass(CDCTransformerJob.class);
	                job.setJobName("CDCTransformJob for data Collected on : " + dataSetDate + "-" + dataSetHour);

	                job.setInputFormatClass(SequenceFileInputFormat.class);

	                job.setMapOutputKeyClass(Text.class);
	                job.setMapOutputValueClass(MapWritable.class);

	                job.setMapperClass(CDCMapper.class);
	                job.setReducerClass(CDCReducer.class);
	                
	                FileSystem fs = FileSystem.get(config);
	                
	                if (fs.exists(new Path(args[1])))
	        			fs.delete(new Path(args[1]), true);
	                
	                FileStatus[] files = fs.listStatus(new Path (args[2]));
	                
	                for(FileStatus file : files) {
	                	System.out.println(file.getPath().getName());
	                	Schema sch = new Schema.Parser().parse(fs.open(file.getPath()));
	                	AvroMultipleOutputs.addNamedOutput(job, sch.getName(), AvroKeyOutputFormat.class, sch, null);
	                }

	                SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
	                AvroKeyOutputFormat.setOutputPath(job, new Path (args[1]));
	                AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	                AvroKeyOutputFormat.setCompressOutput(job, true);

	                return (job.waitForCompletion(true) ? 0 : 1);
	        }
	        /**
	         * @param args
	         */
	        public static void main(String[] args) throws Exception
	        {
	                CDCTransformerJob processor = new CDCTransformerJob();
	                String[] otherArgs = new GenericOptionsParser(processor.getConf(), args).getRemainingArgs();
	                System.exit(ToolRunner.run(processor.getConf(), processor, otherArgs));
	        }

}
