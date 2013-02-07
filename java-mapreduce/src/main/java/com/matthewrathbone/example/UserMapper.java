package com.matthewrathbone.example;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// structure of a user record is this:
// id, email, language, location
// in tab delimited format
// example record:
// 1  matthew@example.com EN US
// We're assuming all records are good.
public class UserMapper extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  String sortChar = "a";

  @Override
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split("\t");
    String uid = record[0];
    String location = record[3];
    outKey.set(uid, sortChar);
    outValue.set("location", location);
    context.write(outKey, outValue);
  }

}