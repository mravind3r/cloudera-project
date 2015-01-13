package net.rav;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

  private MultipleOutputs<Text, NullWritable> multipleOutputs;

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    // this is easy: just loop through all of the counts that we got, adding them up
    int total = 0;
    for (IntWritable i : values) {
      total += i.get();
    }

    // and then send the counts to the reducer
    // context.write(key, new IntWritable(total)); -- change this to get the right output

    multipleOutputs.write(key, NullWritable.get(), String.valueOf(total) + "/" + String.valueOf(total));

  }

  @Override
  public void setup(Context context) {
    multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
  }

  @Override
  public void cleanup(final Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
  }
}
