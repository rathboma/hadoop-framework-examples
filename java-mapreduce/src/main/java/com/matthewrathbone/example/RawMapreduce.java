package com.matthewrathbone.example;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
/**
 * Hello world!
 *
 */
public class RawMapreduce 
{
    public static void main( String[] args ) throws IOException {
        Path inputTransactions = new Path(args[0]);
        Path inputUsers = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job = new Job(new Configuration());
        job.setJarByClass(RawMapreduce.class);
        job.setJobName("raw Mapreduce Example");
        MultipleInputs.addInputPath(job, inputTransactions, TextInputFormat.class, TransactionMapper.class);
        MultipleInputs.addInputPath(job, inputUsers, TextInputFormat.class, UserMapper.class);

    }
}
