package edu.umd.JBizz;

import java.io.IOException;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Set;

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
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HMapStFW;



public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {

    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = ((Text) value).toString();
        String[] terms = line.split("\\s+");

        for (int i = 0; i < terms.length; i++){
          String term = terms[i];

          if(term.length() == 0)
            continue;

          MAP.clear();

          for(int j = i - 2; j < i + 3; j++){
            if(j == i || j <0)
              continue;

            if(j >= terms.length)
              break;

            if(terms[j].length() == 0)
              continue;

            MAP.increment(terms[j]);
          }

          KEY.set(term);
          context.write(KEY,MAP);
        }
      }
    }
  

  private static class MyCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW>{
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context) 
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  // Reducer: sums up all the counts.
  //REDUCER inspired by code from http://codingjunkie.net/cooccurrence/
  //And of course Prof Lin's code.
  private static class MyReducer extends Reducer<Text, HMapStIW, Text, HMapStFW> {

    private final static HMapStIW adder = new HMapStIW();
    private final static HMapStFW PMIMAP = new HMapStFW();
    private final static IntWritable COUNT = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      adder.clear();
      for(HMapStIW value : values){
        addAll(value);
      }
      for(HMapStIW value : values){
        calcPMI(value);
      }
      //now do pmi
      context.write(key, PMIMAP);

    }
    private void addAll(HMapStIW map){
      Set<String> keys = map.keySet();
      for (String key : keys){
        int fromCount = map.get(key);
        if(adder.containsKey(key)){
          int count = adder.get(key);
          COUNT.set(COUNT.get() + count;
          count = count + fromCount;
        } else {
          adder.put(key, fromCount);
        }
      }
    }
    private void calcPMI(HMapStIW map){
      Set<String> keys = map.keySet();
      for(String key : keys){
        float pmi = (float) Math.log10(map.get(key)/COUNT.get());
        PMIMAP.put(key, pmi);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

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

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapStIW.class);

    job.setMapperClass(MyMapper.class);
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
    ToolRunner.run(new StripesPMI(), args);
  }
}