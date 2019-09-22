package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// First Job // Create simple class 

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
    public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  
      private static final IntWritable ONE = new IntWritable(1);
      private static final Text WORD = new Text();
  
      @Override
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

        private Map<String, Integer> AlphaTrack; // keep an eye on what's been emitted so far
        
        for (String word : Tokenizer.tokenize(value.toString())) {

          if (!AlphaTrack.containsKey(word)) { // if already been emitted for this line ignore it
            AlphaTrack.put(word, 1) // add new word in with value 1  
            WORD.set(word);
            context.write(WORD, ONE); 
          }
  
        }
      }
    }
  
    //////////////////////////////////////////////////////////////////////////////////////////////////// IGNORE 

    public static final class MyMapperIMC extends Mapper<LongWritable, Text, Text, IntWritable> {
      private Map<String, Integer> counts;

      @Override
      public void setup(Context context) throws IOException, InterruptedException {
        counts = new HashMap<>(); // keep a local count of the word in a hash yourself and emit the totals at the end
      }
  
      @Override
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

        boolean flag = false;
        
        for (String word : Tokenizer.tokenize(value.toString())) {

          if (word.equals("perfect")) { // same as before, if you find an correct instance, set the flag on
            flag = true;
            continue;
          }
          
          if (flag) {
            if (counts.containsKey(word)) {
                counts.put(word, counts.get(word)+1);
              } else {
                counts.put(word, 1);
              }
	          flag = false; // reset this for the next word
          }
        }
      }
  
      @Override
      public void cleanup(Context context) throws IOException, InterruptedException {
        
        IntWritable cnt = new IntWritable(); // data types that hadoop expects
        Text token = new Text();
  
        for (Map.Entry<String, Integer> entry : counts.entrySet()) { 
          token.set(entry.getKey());
          cnt.set(entry.getValue()); // setters and getters
          context.write(token, cnt); // push values to reducer
        }
      }
    }

    // IGNORE /////////////////////////////////////////////////////////////////////////////////////////////////

    public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private static final IntWritable SUM = new IntWritable();
  
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
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
  
    private PairsPMI() {}
  
    private static final class Args {
      @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
      String input;
  
      @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
      String output;
  
      @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
      int numReducers = 1;
  
      @Option(name = "-imc", usage = "use in-mapper combining")
      boolean imc = false;
    }
  
    @Override
    public int run(String[] argv) throws Exception {
      final Args args = new Args();
      CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
  
      try {
        parser.parseArgument(argv);
      } catch (CmdLineException e) {
        System.err.println(e.getMessage());
        parser.printUsage(System.err);
        return -1;
      

  
      LOG.info("Tool: " + PairsPMI.class.getSimpleName());
      LOG.info(" - input path: " + args.input);
      LOG.info(" - output path: " + args.output);
      LOG.info(" - number of reducers: " + args.numReducers);
      LOG.info(" - use in-mapper combining: " + args.imc);
  
      Configuration conf = g
  
      etConf();
      Job job = Job.getInstance
  
      (conf);
      job.setJobName(PairsPMI.class.getSimpleName());
      job.setJarByClass(PairsPMI.class);
  
      job.setNumReduceTasks(args.numReducers);
  
      FileInputFormat.setInputPaths(job, new Path(args.input));
      FileOutputFormat.setOutputPath(job, new Path(args.output));
  
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setOutputFormatClass(TextOutputFormat.class);
  
      job.setMapperClass(args.imc ? MyMapperIMC.class : MyMapper.class);
      // job.setCombinerClass(MyReducer.class); ->->->->-> ENABLE THIS FOR USING THE REDUCER AS A COMBINER <-<-<-<-
      job.setReducerClass(MyReducer.class);
  
      // Delete the output directory if it exists already.
      Path outputDir = new Path(args.output);
      FileSystem.get(conf).delete(outputDir, true);
  
      long startTime = System.currentTimeMillis();
      job.waitForCompletion(true);
      LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  
      return 0;
    }
  }