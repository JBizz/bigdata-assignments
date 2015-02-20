package edu.umd.JBizz;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Set;
import java.util.HashMap;

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
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final IntWritable TOTAL = new IntWritable(0);

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
          TOTAL.set(TOTAL.get() + 1);
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
  private static class MyReducer extends Reducer<Text, HMapStIW, Text, FloatWritable> {

    //private final static HMapStIW adder = new HMapStIW();
    private final static FloatWritable SINGLEPROB = new FloatWritable();
    private final static IntWritable COUNT = new IntWritable(0);

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      //adder.clear();
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW adder = new HMapStIW();
      while(iter.hasNext()) {
        adder.plus(iter.next());
      }
      //for(HMapStIW value : values){
        //adder.plus(value);
      //}
      Set<String> mapkeys = adder.keySet();
      int sum = 0;
      for (String mapkey : mapkeys){
        sum = sum + adder.get(mapkey);
      }
      COUNT.set(sum);

      float countHold = COUNT.get();
      float totalHold = TOTAL.get();
      SINGLEPROB.set((countHold/totalHold));

      context.write(key, SINGLEPROB);
    }
  }

  private static class MyReducer2 extends Reducer<Text, HMapStIW, Text, HMapStFW> {
    //private final static IntWritable SUM = new IntWritable();
    //private final static IntWritable MARGESUM = new IntWritable();
    private final static FloatWritable PMI = new FloatWritable();
    private static final FloatWritable MAPVALUE = new FloatWritable();
    private final static HashMap<String, FloatWritable> keyToProb = new HashMap<String, FloatWritable>();

    @Override
    public void setup(Context context) throws IOException {

      FileSystem fs = FileSystem.get(context.getConfiguration());
      File path = new File("tempFile");
      for (File f : path.listFiles()){
        Path fPath = new Path(f.getAbsolutePath());
        //LOG.info(fPath.toString());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath)));
        try{
          String line;
          line = br.readLine();
          while(line != null){

            if(!(fPath.toString().contains("_") || fPath.toString().contains("."))){
              String[] lineSplit = line.split("\\s+");
              String key = lineSplit[0];
              float value = Float.parseFloat(lineSplit[1]);
              MAPVALUE.set(value);
              keyToProb.put(key, MAPVALUE);
            }
            line = br.readLine();
          }
        }finally {
          br.close();
        }
      }
    }


      @Override
      public void reduce(Text key, Iterable<HMapStIW> values, Context context)
          throws IOException, InterruptedException {
        
        Iterator<HMapStIW> iter = values.iterator();
        HMapStIW adder = new HMapStIW();
        while(iter.hasNext()) {
          adder.plus(iter.next());
        }
        HMapStFW pmiMap = new HMapStFW();
        //for(HMapStIW value : values){
          //adder.plus(value);
        //}
        Set<String> mapkeys = adder.keySet();
        int sum = 0;
        for (String mapkey : mapkeys){
          float countHold = adder.get(mapkey);
          float totalHold = TOTAL.get();
          float pairProb = countHold/totalHold;
          //LOG.info(key.toString());
          float leftString = keyToProb.get(key.toString()).get();
          float rightString = keyToProb.get(mapkey).get();
          float pmi = (float) Math.log10(pairProb/(leftString*rightString));
          pmiMap.put(mapkey, pmi);
        }

        context.write(key, pmiMap);
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

    String tempOut = "tempFile";

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(tempOut));
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStIW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(tempOut);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);

    Configuration conf2 = getConf();
    Job job2 = Job.getInstance(conf2);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(inputPath));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));
    
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStIW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HMapStFW.class);

    job2.setMapperClass(MyMapper.class);
    job2.setCombinerClass(MyCombiner.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(outputPath);
    FileSystem.get(conf2).delete(outputDir2, true);
    job2.waitForCompletion(true);

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