package com.matthewrathbone.example;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class RawMapreduce {
  
  public static void main( String[] args ) throws Exception {
    Path transactions = new Path(args[0]);
    Path users = new Path(args[1]);
    Path staging = new Path(args[2]);
    Path output = new Path(args[3]);
    main(new Configuration(), transactions, users, staging, output );
  }

  // this allows testing easier as I can pass in a configuration
  // object.
  public static void main(
    Configuration conf, Path transactions, Path users, Path staging, Path output) 
  throws Exception {

    runFirstJob(transactions, users, staging, new Configuration(conf));
    runSecondJob(staging, output, new Configuration(conf));
  }

  protected static void runFirstJob(Path transactions, Path users, Path output, Configuration conf) 
  throws Exception {
    Job job = new Job(conf);
    job.setJarByClass(RawMapreduce.class);
    job.setJobName("Raw Mapreduce Step 1");

    job.setPartitionerClass(SecondarySort.SSPartitioner.class);
    job.setGroupingComparatorClass(SecondarySort.SSGroupComparator.class);
    job.setSortComparatorClass(SecondarySort.SSSortComparator.class);

    job.setReducerClass(JoinReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, TransactionMapper.class);
    MultipleInputs.addInputPath(job, users, TextInputFormat.class, UserMapper.class);

    job.setMapOutputKeyClass(TextTuple.class);
    job.setMapOutputValueClass(TextTuple.class);
    FileOutputFormat.setOutputPath(job, output);
    
    if (job.waitForCompletion(true)) return;
    else throw new Exception("First Job Failed");
  }

  protected static void runSecondJob(Path input, Path output, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJarByClass(RawMapreduce.class);
    job.setJobName("Raw Mapreduce Step 2");
    
    FileInputFormat.addInputPath(job, input);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
    
    job.setPartitionerClass(SecondarySort.SSPartitioner.class);
    job.setGroupingComparatorClass(SecondarySort.SSGroupComparator.class);
    job.setSortComparatorClass(SecondarySort.SSSortComparator.class);
    
    job.setMapperClass(SecondStage.SecondMapper.class);

    job.setMapOutputKeyClass(TextTuple.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(SecondStage.SecondReducer.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    FileOutputFormat.setOutputPath(job, output);
    if (job.waitForCompletion(true)) return;
    else throw new Exception("First Job Failed");
  }
}
