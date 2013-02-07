package com.matthewrathbone.example;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// a transaction contains these fields:
// transactionId, productId, userId, purchaseAmount, productDescription
// data is tab delimited
// 

public class TransactionMapper extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  
  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split("\t");
    String productId = record[1];
    String uid = record[2];
    outKey.set(uid, "b");
    outValue.set("product", productId);
    context.write(outKey, outValue);
  }

}