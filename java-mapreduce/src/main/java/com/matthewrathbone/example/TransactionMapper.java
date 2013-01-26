package com.matthewrathbone.example;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

// a transaction contains these fields:
// transactionId, productId, userId, purchaseAmount, productDescription
// data is tab delimited
// 

public class TransactionMapper extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  
  public void Map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split("\t");
    String uid = record[2];
    String productId = record[1];
    outKey.set(uid, "b");
    outValue.set("product", productId);
    context.write(outKey, outValue);
  }

}