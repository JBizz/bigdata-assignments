package edu.umd.JBizz;

import java.io.IOException;

import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

import edu.umd.JBizz.*;


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static TextPair PAIRS = new TextPair();
    private final static TextPair MARGE = new TextPair();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      Text prev;
      Text cur;
      Text marge = "*";
      StringTokenizer itr = new StringTokenizer(line);

      while (itr.hasMoreTokens()) {
        if(prev == null){
          prev = itr.nextToken();
          cur = itr.nextToken();
        }
        else{
          cur = itr.nextToken();
        }
        PAIRS.set(cur, prev);
        MARGE.set(cur, "*");
        context.write(PAIRS,ONE);
        context.write(MARGE,ONE);
      }
    }
  }

  //public static class PairPartitionFromFirst extends Partitioner<TextPair, IntWritable> {
    //@Override
    //public int getPartition(TextPair key, IntWritable value, int numOfReducers){

    //}
  //}

  private static class MyCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable>{
    private final static IntWritable COUNT = new IntWritable();
    private final static TextPair PAIRS = new TextPair();

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        SUM.set(sum);
        context.write(key, SUM);
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();
    private final static IntWritable MARGESUM = new IntWritable();

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      int margeSum = 0;
      while (iter.hasNext()) {
        if(key.getSecond().equals("*")){
          margeSum += iter.next().get();
          MARGESUM.set(margeSum);
        }
        else{
        
          sum += iter.next().get();
        }
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /*public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(TextPair.class, true); 
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
      TextPair tp1 = (TextPair) w1;
      TextPair tp2 = (TextPair) w2;
      if(tp1.getSecond.equals("*") && tp2.getSecond.equals("*")){
        return 0;
      }
      else{
        if (tp1.getSecond().equals("*")){
          return -1;
        } 
        if (tp.getSecond.equals("*")){
          return 1;
        }
      }
      return tp1.compareTo(tp2);
    }
  }*/

  public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(TextPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
      TextPair tp1 = (TextPair) w1;
      TextPair tp2 = (TextPair) w2;

      return tp1.getFirst().compareTo(tp2.getSecond());
    }
  }

  protected static class MyPartitioner extends Partitioner<TextPair, IntWritable>{
    @Override
    public int getPartition(TextPair key, IntWritable value, int numReduceTasks){
      return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner .class);
    job.setSortComparatorClass(KeyComparator.class);
    job.setGroupingComparatorClass(GroupComparator.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}