package com.matthewrathbone.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTuple implements WritableComparable<TextTuple> {

  public Text left;
  public Text right;

  public TextTuple() {
    this(new Text(), new Text());
  }

  public TextTuple(String a, String b) {
    this(new Text(a), new Text(b));
  }

  public TextTuple(Text a, Text b) {
    this.left = a;
    this.right = b;
  }

  public void readFields(DataInput in) throws IOException {
    left.readFields(in);
    right.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    left.write(out);
    right.write(out);
  }

  public int compareTo(TextTuple other) {
    int leftCompare = left.compareTo(other.left);
    if (leftCompare == 0) return right.compareTo(other.right);
    else return leftCompare;
  }

  @Override
  public boolean equals(Object otherRaw) {
    if (otherRaw instanceof TextTuple) {
      TextTuple other = (TextTuple) otherRaw;
      return left == other.left && right == other.right;
    }
    return false;
  }

  public void set(String a, String b) {
    left.set(a);
    right.set(b);
  }

  public void set(Text a, Text b) {
    left.set(a.toString());
    right.set(b.toString());
  }

  @Override
  public String toString() {
    return String.format("(%s,%s)", left.toString(), right.toString());
  }

  @Override
  public int hashCode() {
    return left.hashCode() + 101 * right.hashCode();
  } 



}