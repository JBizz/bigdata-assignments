package edu.umd.JBizz;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Arrays;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
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
  private final static IntWritable TOTAL = new IntWritable(0);
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
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
            TOTAL.set(TOTAL.get() + 1);
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
  private static class MyReducer extends Reducer<PairOfStrings, IntWritable, Text, FloatWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();
    private final static IntWritable MARGESUM = new IntWritable();
    private final static FloatWritable PMI = new FloatWritable();

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
          float countHolder = sum;
          float totalHolder = TOTAL.get();
          PMI.set((float) countHolder/totalHolder);
          Text textKey = new Text(key.getLeftElement());
          context.write(textKey, PMI);
      }
    }
  }

  private static class MyReducer2 extends Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable> {
    private final static IntWritable SUM = new IntWritable();
    private final static IntWritable MARGESUM = new IntWritable();
    private final static FloatWritable PMI = new FloatWritable();
    private static final FloatWritable MAPVALUE = new FloatWritable();
    private final static HashMap<String, FloatWritable> keyToProb = new HashMap<String, FloatWritable>();

    @Override
    public void setup(Context context) throws IOException {

      FileSystem fs = FileSystem.get(context.getConfiguration());
      //File path = new File("tempFile");
      FileStatus[] status_list = fs.listStatus(new Path("tempFile"));
      for(FileStatus status : status_list){
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
        String line = br.readLine();
        while(line != null){
          String[] lineSplit = line.split("\\s+");
          String key = lineSplit[0];
          float value = Float.parseFloat(lineSplit[1]);
          MAPVALUE.set(value);
          keyToProb.put(key, MAPVALUE);
          line = br.readLine();
        }
        br.close();
      }
    }

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
          //float pmiHolder = keyToProb.get(key.getLeftElement()).get();
          PMI.set(keyToProb.get(key.getLeftElement()).get());
          context.write(key, PMI);
      }
      if(!key.getValue().equals("!")){
        float sumFloat = sum;
        float pairProb = sumFloat / TOTAL.get();
        //float margeFLoat = MARGESUM.get();
        float leftString = keyToProb.get(key.getLeftElement()).get();
        float rightString = keyToProb.get(key.getRightElement()).get();
        float pmi = (float)Math.log10(pairProb/(leftString*rightString));
        PMI.set(pmi);
        context.write(key, PMI);
      }
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

    String tempOut = "tempFile";

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(tempOut));
    //MapFileOutputFormat.setOutputPath(job, new Path(tempOut));
    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    //job.setOutputFormatClass(MapFileOutputFormat.class);
    

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner .class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(tempOut);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    //run second here


    Configuration conf2 = getConf();
    Job job2 = Job.getInstance(conf2);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(inputPath));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));
    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(FloatWritable.class);


    job2.setMapperClass(MyMapper.class);
    job2.setPartitionerClass(MyPartitioner .class);
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
    ToolRunner.run(new PairsPMI(), args);
  }
}