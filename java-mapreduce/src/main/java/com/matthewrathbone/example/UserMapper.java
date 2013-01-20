package com.matthewrathbone.example;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

  public void Map(LongWritable key, Text value, Context context) {
    
  }

}