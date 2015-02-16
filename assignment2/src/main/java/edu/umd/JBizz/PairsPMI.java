package edu.umd.JBizz;

import java.io.IOException;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Arrays;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfStrings;



public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    //private final static TextPair PAIRS = new TextPair();
    private final static PairOfStrings MARGE = new PairOfStrings();
    private static final PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = ((Text) value).toString();
        String[] terms = line.split("\\s+");

        String prev = null;
        String cur;
        String marge = "!";

        for (int i = 0; i < terms.length; i++){
          String term = terms[i];
          String[] usedTerms = new String[terms.length];

          if(term.length() == 0)
            continue;
          for(int j = i - 2; j < i + 3; j++){
            if(j == i || j <0)
              continue;

            if(j >= terms.length)
              break;

            if(terms[j].length() == 0)
              continue;

            //no double reporting a single line coocurrence
            if(Arrays.asList(usedTerms).contains(terms[j]))
              continue;

            PAIR.set(term, terms[j]);
            context.write(PAIR, ONE);
            PAIR.set(term, marge);
            context.write(PAIR,ONE);

            usedTerms[j] = new String(terms[j]);
          }
        }
      }
    }
  

  private static class MyCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable>{
    private final static IntWritable COUNT = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        COUNT.set(sum);
        context.write(key, COUNT);
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();
    private final static IntWritable MARGESUM = new IntWritable();
    private final static DoubleWritable PMI = new DoubleWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      int margeSum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if(key.getValue().equals("!")){
          MARGESUM.set(sum);
          PMI.set((float) sum);
          context.write(key, PMI);
      }
      if(!key.getValue().equals("!")){
        float sumFloat = sum;
        float margeFLoat = MARGESUM.get();
        double pmi = Math.log10(sumFloat/margeFLoat);
        PMI.set(pmi);
        context.write(key, PMI);
      }
    }
  }

  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(PairOfStrings.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        PairOfStrings tp1 = (PairOfStrings) w1;
        PairOfStrings tp2 = (PairOfStrings) w2;
      
      int cmp = tp1.getKey().compareTo(tp2.getKey());
      if(cmp != 0){
        return cmp;
      }
      if(tp1.getValue().equals("*") && tp2.getValue().equals("*")){
            return 0;
      }
      else{
        if (tp1.getValue().equals("*")){
          return -1;
        }
        if (tp2.getValue().equals("*")){
          return 1;
        }
      }
      return tp1.getValue().compareTo(tp2.getValue());
    }
  }

  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(PairOfStrings.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
      PairOfStrings tp1 = (PairOfStrings) w1;
      PairOfStrings tp2 = (PairOfStrings) w2;
      return tp1.getKey().compareTo(tp2.getKey());
    }
  }

  public static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable>{
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks){
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
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
 
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner .class);
    //job.setSortComparatorClass(KeyComparator.class);
    //job.setGroupingComparatorClass(GroupComparator.class);
    job.setCombinerClass(MyCombiner.class);
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