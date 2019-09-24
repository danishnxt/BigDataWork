/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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

import java.lang.*;
import java.io.IOException;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;

import tl.lin.data.pair.PairOfStrings;

/*
  INTERESTING NOTE 
  ----------------

    I didn't check the Bespin file for BigramFrequency until the last night, so some intermediate tasks have been done
    in a different way, they all still work just fine. 

    Update: The partioner expects a pair of strings as well. Looks like my emit(WORD /t WORD) solution is going to have to 
    change now for consistency...

*/ 

public class PairsPMI extends Configured implements Tool { 

  private static int redSplit = 1; // global var for how many reducers engaged
  private static String tempDir = "TempFile"; // global var to hold location for temp - meta data directory - 
  private static PairOfStrings myGram = new PairOfStrings(); // all your bigrams are belong to me

  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    ///////////////// MAPPER 1 /////////////////
  public static final class MyMapperA extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private static final FloatWritable ONE = new FloatWritable(1);
    private static final Text WORD = new Text();
      
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Integer> AlphaTrack = new HashMap<String, Integer>();

      if (((value.toString()).compareTo("")) != 0) {
          WORD.set("*");
          context.write(WORD, ONE);
      }

      for (String word : Tokenizer.tokenize(value.toString())) {

        if (!AlphaTrack.containsKey(word)) { // if already been emitted for this line ignore it
          AlphaTrack.put(word, 1); // add new word in with value 1  
          WORD.set(word);
          context.write(WORD, ONE);
        }

      }
    }
  }

    ///////////////// MAPPER 2 /////////////////
  public static final class MyMapperB extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings WORDS = new PairOfStrings();
      
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Integer> AlphaTrack = new HashMap<String, Integer>(); // will concat strings with tab

      List<String> tokens; // match the tokenizer return type and get the full list at once
      tokens = Tokenizer.tokenize(value.toString()); // whole list with us now
      int listSize = tokens.size(); // get the size out and run loops with "clever indexing"?
      
      String l1_temp = "";
      String l2_temp = "";

      for (int i = 0; i < listSize; i++) {
        
        l1_temp = tokens.get(i); // do this get action a single time

        for (int j = 0; j < listSize; j++) {

          if (i == j) {
            continue; // same letter, not a pair
          }

          l2_temp = tokens.get(j);

          if (!AlphaTrack.containsKey(l1_temp + l2_temp)) { // if exist in the hash map -> ignore it!
            AlphaTrack.put(l1_temp + l2_temp, 1); // add this and emit it

            // switching this out for a pair of Strings data type

            WORDS.set(l1_temp, l2_temp); //
            context.write(WORDS, ONE); // sending out a tuple instead
          }
        }
      }

      return;

    }
  }

  ///////////////// REDUCER A /////////////////

  public static final class MyReducerA extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      Iterator<FloatWritable> iter = values.iterator();
      int sum = 0;
      
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      
      SUM.set(sum);
      context.write(key, SUM);
      
    }
  }

  ///////////////// REDUCER B /////////////////

  public static final class MyReducerB extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    
    private static final FloatWritable SUM = new FloatWritable();
    private static final FloatWritable flt_result = new FloatWritable();

    private HashMap<String, Integer> AlphaCount; // initialized here for global across all map jobs

    @Override // override the default implemetations
    public void setup(Context context) throws IOException, InterruptedException {

      AlphaCount = new HashMap<String, Integer>();
      String start = "/part-t-0000";

      if (redSplit > 9) { // more reducers -> fix the file
        start = "/part-t-000"; // will append double digits here
      } else if (redSplit > 99) {
        start = "/part-t-00";
      }

      for (int i = 0; i < redSplit; i++) {

        File file = new File(tempDir + start + Integer.toString(i));
        BufferedReader br = new BufferedReader(new FileReader(file)); 
        
        String st; 
        
        while ((st = br.readLine()) != null) {
          int temp = Integer.parseInt(st.split("\t", 2)[1]);
          AlphaCount.put(st.split("\t", 2)[0], temp);
        }
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      Iterator<FloatWritable> iter = values.iterator();
      int sum = 0;
      
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      // possible optimization -> split the string just once and save it, accessing it directly later on

      String fw = key.getLeftElement(); // these should just work themselves
      String sw = key.getRightElement();

      // now what -> so we compute all the things -> we know the total count
      // by this point we know the total per pair - we need the counts for each of them individually

      int total = AlphaCount.get("*");
        // System.out.print("total:  -> ");
        // System.out.print(total);
        // System.out.print("\n");

      double num = (sum * 1.0 / total * 1.0);
        // System.out.print("num:  -> ");
        // System.out.print(num);
        // System.out.print("\n");

      double denom = (AlphaCount.get(fw) * 1.0 / total * 1.0) * (AlphaCount.get(sw) * 1.0 / total * 1.0);
        // System.out.print("denom:  -> ");
        // System.out.print(denom);
        // System.out.print("\n");

      double to_log = (num/denom); // should be ok
        // System.out.print("frac:  -> ");
        // System.out.print(to_log);
        // System.out.print("\n");

      float final_result = (float)Math.log10(to_log); // final result here

      System.out.print("key:  -> ");
      System.out.print(key.toString());
      System.out.print("\n");

      System.out.print("Value:  -> ");
      System.out.print(final_result);
      System.out.print("\n");

      SUM.set(sum); // will need the sum
      flt_result.set(final_result);
      context.write(key, flt_result); // try this out for size huh
      
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private PairsPMI() {} // create an instance of the tool inside the object?

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
  }


  /////////////////  COMBINER CLASSES HERE  ///////////////// 

  // NEED A DIFFERENT COMBINER SINCE THE WORK OVERALL IS DIFFERENT


  /////////////////  MAIN JOB SETUP  ///////////////// 

  @Override
  public int run(String[] argv) throws Exception { // Fired first - SETUP function

    final Args args = new Args(); // create an object to hold God damn arguments
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    // we have all the arguments we'd need at this poin

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info(" Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    redSplit = args.numReducers; // we need to know how many files to read;

    /// SET GLOBAL REDUCER COUNT /// 

    Configuration conf = getConf();

    /////////////// JOB A CONFIG ///////////////

    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);
    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(tempDir)); // MANUAL TEMP FILE OVERRIDE
    // FileOutputFormat.setOutputPath(job, new Path(args.output)); 

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapperA.class);
    job.setCombinerClass(MyReducerA.class);
    job.setReducerClass(MyReducerA.class);

    /////////////// JOB B CONFIG ///////////////

    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);
    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(FloatWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(FloatWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    
    job2.setMapperClass(MyMapperB.class);
    // job.setCombinerClass(MyReducerB.class);
    job2.setReducerClass(MyReducerB.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    // RUN JOB 1

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true); // blocking call -> so we can have the code written async
    LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // RUN JOB 2 

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true); // blocking call -> so we can have the code written async
    LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Delete temp folder -> once we know how many files to mulch up
    // Path tempDelete = new Path(tempDir);
    // FileSystem.get(conf).delete(tempDelete, true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args); // tool runner class runs the 'run' function -> inside the class
  }

}
