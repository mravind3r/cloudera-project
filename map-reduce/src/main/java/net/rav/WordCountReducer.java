package net.rav;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    // this is easy: just loop through all of the counts that we got, adding them up
    int total = 0;
    for (IntWritable i : values) {
      total += i.get();
    }

    // and then send the counts to the reducer
    System.out.println("key:" + key + " -- val:" + total);
    context.write(key, new IntWritable(total));

  }
}
