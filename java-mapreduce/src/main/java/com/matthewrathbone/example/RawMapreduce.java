package com.matthewrathbone.example;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class RawMapreduce 
{
    public static void main( String[] args ) throws Exception {
        Path inputTransactions = new Path(args[0]);
        Path inputUsers = new Path(args[1]);
        Path staging = new Path(args[2]);
        Path output = new Path(args[3]);

        runFirstJob(inputTransactions, inputUsers, staging);
        runSecondJob(staging, output);

        
    }

    private static void runFirstJob(Path transactions, Path users, Path output) 
    throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(RawMapreduce.class);
        job.setJobName("raw Mapreduce Example");
        MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, TransactionMapper.class);
        MultipleInputs.addInputPath(job, users, TextInputFormat.class, UserMapper.class);
        job.setPartitionerClass(SecondarySort.SSPartitioner.class);
        job.setGroupingComparatorClass(SecondarySort.SSGroupComparator.class);
        job.setSortComparatorClass(SecondarySort.SSSortComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        if (job.waitForCompletion(true)) return;
        else throw new Exception("First Job Failed");
    }

    private static void runSecondJob(Path input, Path output) {

    }
}
