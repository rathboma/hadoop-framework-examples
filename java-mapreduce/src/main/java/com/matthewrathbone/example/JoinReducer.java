package com.matthewrathbone.example;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


// the first value is location
// if it's not, we don't have a user record, so we'll 
// record the location as UNKNOWN
public class JoinReducer extends Reducer<TextTuple, TextTuple, Text, Text> {

  Text location = new Text("UNKNOWN");
  @Override
  public void reduce(TextTuple key, Iterable<TextTuple> values, Context context) 
  throws java.io.IOException, InterruptedException {
    for (TextTuple value: values) {
      if (value.left.toString().equals("location")) {
        location = new Text(value.right);
        continue;
      }

      Text productId = value.right;
      context.write(productId, location);
    }
  }

}