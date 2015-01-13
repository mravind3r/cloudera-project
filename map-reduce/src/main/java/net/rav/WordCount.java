package net.rav;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

  public static void main(String[] args) throws Exception {
    // Get the default configuration object
    Configuration conf = new Configuration();

    // delete the output folder if it already exists
    // FileSystem hfs = FileSystem.get(conf);
    // if (hfs.exists(new Path(args[1]))) {
    // hfs.delete(new Path(args[1]), true);
    // }

    // now create the MapReduce job
    Job job = new Job(conf);
    job.setJobName("WordCount");

    // we'll output text/int pairs (since we have words as keys and counts as values)
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // again we'll output text/int pairs (since we have words as keys and counts as values)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // tell Hadoop the mapper and the reducer to use
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);

    // we'll be reading in a text file, so we can use Hadoop's built-in TextInputFormat
    job.setInputFormatClass(TextInputFormat.class);

    // we can use Hadoop's built-in TextOutputFormat for writing out the output text file
    // job.setOutputFormatClass(TextOutputFormat.class); // -- this create a partm-0000 file
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "myoutputdir", TextOutputFormat.class, Text.class, NullWritable.class);

    // set the input and output paths
    TextInputFormat.setInputPaths(job, args[0]);
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    // this tells Hadoop to ship around the jar file containing "WordCount.class" to all of the different
    // nodes so that they can run the job
    job.setJarByClass(WordCount.class);

    // submit the job and wait for it to complete!
    int exitCode = job.waitForCompletion(true) ? 0 : 1;
    System.exit(exitCode);

  }
}
