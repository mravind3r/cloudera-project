package net.rav;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

  public static void main(String[] args) throws Exception {

    // if we got the wrong number of args, then exit
    // if (args.length != 4 || !args[0].equals("-r")) {
    // System.out.println("usage: WordCount -r <num reducers> <input> <output>");
    // System.exit(0);
    // }

    // -r 1 /Users/a-rmandal/repositories/cloudera-project/input /Users/a-rmandal/repositories/cloudera-project/output

    // Get the default configuration object
    Configuration conf = new Configuration();

    // now create the MapReduce job
    Job job = new Job(conf);
    job.setJobName("WordCount");

    // we'll output text/int pairs (since we have words as keys and counts as values)
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // again we'll output text/int pairs (since we have words as keys and counts as values)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // tell Hadoop the mapper and the reducer to use
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);

    // we'll be reading in a text file, so we can use Hadoop's built-in TextInputFormat
    job.setInputFormatClass(TextInputFormat.class);

    // we can use Hadoop's built-in TextOutputFormat for writing out the output text file
    job.setOutputFormatClass(TextOutputFormat.class);

    // set the input and output paths
    TextInputFormat.setInputPaths(job, args[0]);
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    // set the number of reduce paths
    // try {
    // job.setNumReduceTasks(Integer.parseInt(args[1]));
    // } catch (Exception e) {
    // System.out.println("usage: WordCount -r <num reducers> <input> <output>");
    // System.exit(-1);
    // }

    // force the mappers to handle one megabyte of input data each
    TextInputFormat.setMinInputSplitSize(job, 1024 * 1024);
    TextInputFormat.setMaxInputSplitSize(job, 1024 * 1024);

    // this tells Hadoop to ship around the jar file containing "WordCount.class" to all of the different
    // nodes so that they can run the job
    job.setJarByClass(WordCount.class);

    // submit the job and wait for it to complete!
    int exitCode = job.waitForCompletion(true) ? 0 : 1;
    System.exit(exitCode);

  }

}
