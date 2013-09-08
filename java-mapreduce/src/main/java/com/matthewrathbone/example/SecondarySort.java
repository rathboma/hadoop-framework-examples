package com.matthewrathbone.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;

public class SecondarySort {

  private static abstract class TTRawComparator implements RawComparator<TextTuple> {
    DataInputBuffer buffer = new DataInputBuffer();
    TextTuple a = new TextTuple();
    TextTuple b = new TextTuple();

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ) {
      try {
        buffer.reset(b1, s1, l1);
        a.readFields(buffer);
        buffer.reset(b2, s2, l2);
        b.readFields(buffer);
        return compare(a,b);  
      } catch(Exception ex) {
        return -1;
      }  
    }
  }

  // Partition only by UID
  public static class SSPartitioner extends Partitioner<TextTuple, Object> {
    @Override
    public int getPartition(TextTuple k, Object value, int partitions) {
      return (k.left.hashCode() & Integer.MAX_VALUE) % partitions;
    }
  }

  // Group only by UID
  public static class SSGroupComparator extends TTRawComparator  {

    public int compare(TextTuple first, TextTuple second) {
      return first.left.compareTo(second.left);
    }
  }

  // But sort by UID and the sortCharacter
  // remember location has a sort character of 'a'
  // and product-id has a sort character of 'b'
  // so the first record will be the location record!
  public static class SSSortComparator extends TTRawComparator {

    public int compare(TextTuple first, TextTuple second) {
      int lCompare = first.left.compareTo(second.left);
      if (lCompare == 0) return first.right.compareTo(second.right);
      else return lCompare;
    }
  }

}