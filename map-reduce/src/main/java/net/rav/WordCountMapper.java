package net.rav;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  // create these guys up here for speed
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  // create a Pattern object to parse each line
  private final Pattern wordPattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9]+");

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    // get a String version of the line
    String line = value.toString();

    // and a Matcher to parse it
    Matcher myMatcher = wordPattern.matcher(line);

    // while there are more tokens in the line
    while (myMatcher.find()) {

      // get the next pattern, and convert it to lower case
      String returnVal = myMatcher.group();
      returnVal = new String(returnVal.toLowerCase());
      word.set(returnVal);

      // send it on to the reducer!
      context.write(word, one);
    }

  }
}
