package com.matthewrathbone.example;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class SecondStage {

  public static class SecondMapper extends Mapper<Text, Text, TextTuple, Text> {

    TextTuple outputKey = new TextTuple();

    @Override
    public void map(Text key, Text value, Mapper.Context context) 
    throws java.io.IOException, InterruptedException {
      outputKey.set(key, value);
      context.write(outputKey, value);
    }
  }

  public static class SecondReducer extends Reducer<TextTuple, Text, Text, LongWritable> {

    LongWritable valueOut = new LongWritable();

    @Override
    public void reduce(TextTuple product, Iterable<Text> locations, Context context)
    throws java.io.IOException, InterruptedException {
      String previous = null;
      long totalLocations = 0;
      for (Text location: locations) {
        if (previous == null || !location.toString().equals(previous)) {
          totalLocations += 1;
          previous = location.toString();
        }
      }
      valueOut.set(totalLocations);
      context.write(product.left, valueOut);
    }
  }
}