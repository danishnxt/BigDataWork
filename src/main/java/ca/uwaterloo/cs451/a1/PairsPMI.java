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
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;

public class PairsPMI extends Configured implements Tool { 

  // private static String tempDir = "TempFile"; // saving this here for global access -> POSSIBLE FLAG
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  ///////////////// MAPPER 1 /////////////////

  public static final class MyMapperA extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
      
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Integer> AlphaTrack = new HashMap<String, Integer>();

      // the thing below will go for a per line basis we can test this pretty easily 

      // WORD.set("*"); // send this along as full count
      // context.write(WORD, ONE); // emit this as a line count as well

      for (String word : Tokenizer.tokenize(value.toString())) {

        // if (!AlphaTrack.containsKey(word)) { // if already been emitted for this line ignore it
          AlphaTrack.put(word, 1); // add new word in with value 1  
          WORD.set(word);
          context.write(WORD, ONE);
        // }

      }
    }
  }

  ///////////////// MAPPER 2 /////////////////

  public static final class MyMapperB extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD_1 = new Text();
      
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Integer> AlphaTrack = new HashMap<String, Integer>();
      // just concat the strings and that should be the same thing then

      List<String> tokens; // match the tokenizer return type and get the full list at once
      tokens = Tokenizer.tokenize(value.toString()); // whole list with us now
      int listSize = tokens.size(); // get the size out and run loops with "clever indexing"?
      
      // God I really hope it really hurts like hell
      // for each line emit one thing now
      
      String l1_temp = ""; // init this yourself 
      String l2_temp = ""; // init this yourself 

      for (int i = 0; i < listSize; i++) {
        
        l1_temp = tokens.get(i); // do this get action a single time

        for (int j = 0; j < listSize; j++) {

          if (i == j) {
            continue; // same letter, not a pair
          }

          l2_temp = tokens.get(j);

          if (!AlphaTrack.containsKey(l1_temp + l2_temp)) { // if exist in the hash map -> ignore it!
            AlphaTrack.put(l1_temp + l2_temp, 1); // add this and emit it
            WORD_1.set(l1_temp + "\t" + l2_temp); // tab added to use tuple
            context.write(WORD_1, ONE); // sending out a tuple instead
          }
        }
      }

      return;
          
      // for (String word : Tokenizer.tokenize(value.toString())) {

      //   // if (!AlphaTrack.containsKey(word)) { // if already been emitted for this line ignore it
      //     AlphaTrack.put(word, 1); // add new word in with value 1  
      //     WORD.set(word);
      //     context.write(WORD, ONE); 
      //   // }

      // }
    }
  }

  ///////////////// REDUCER A /////////////////

  public static final class MyReducerA extends Reducer<Text, IntWritable, Text, IntWritable> {
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

  ///////////////// REDUCER B /////////////////

  public static final class MyReducerB extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override // override the default implemetations
    public void setup(Context context) throws IOException, InterruptedException {
      private static HashMap<String, Integer> AlphaCount = new HashMap<String, Integer>(); // initialized here for global across all map jobs

      // will need to read more lines if there are more reducers // global variable?

      // int totalLines = 0;
      // File file = new File(tempDir + "/part-r-00000"); // hardcoded here, remove the hard coded value
      // BufferedReader br = new BufferedReader(new FileReader(file)); 
      
      // String st; 
      
      // while ((st = br.readLine()) != null) {
      //   // st contains the word and the count
        
      //   System.out.println(st.split("\t", 2)[0]);

      //   int temp = Integer.parseInt(st.split("\t", 2)[1]);
      //   AlphaCount.put(st.split("\t", 2)[0], temp));

      //   totalLines += temp;

      //   System.out.println(temp);
      //   System.out.println("**************************");

      // }

    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) // this is standard
        throws IOException, InterruptedException {

      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      String full_value = key.toString();

        // possible optimization -> split the string just once and save it, accessing it directly later on

      String fw = full_value.split("\t", 2)[0];
      String sw = full_value.split("\t", 2)[1];

      // now what -> so we compute all the things -> we know the total count
      // by this point we know the total per pair - we need the counts for each of them individually
      
      SUM.set(sum);
      context.write(key, SUM);
      
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

    // @Option(name = "-imc", usage = "use in-mapper combining")
    boolean imc = false;
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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    // LOG ABOVE IS FOR VERBOSE OUTPUT // 

    Configuration conf = getConf();

    /////////////// JOB B META DATA ///////////////

    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);
    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    // FileOutputFormat.setOutputPath(job, new Path(tempDir); // MANUAL TEMP FILE OVERRIDE
    FileOutputFormat.setOutputPath(job, new Path(args.output)); 

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapperA.class);
    job.setCombinerClass(MyReducerA.class);
    job.setReducerClass(MyReducerA.class);

    /////////////// JOB B META DATA ///////////////

    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);
    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapperClass(MyMapperB.class);
    // job.setCombinerClass(MyReducerB.class);
    job.setReducerClass(MyReducerB.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true); // blocking call -> so we can have the code written async
    LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    

    // DELETE THE RANDOM TEMP FILE 

    // RUN JOB 2 

    // REPORT ON JOB TIME AND EXIT

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args); // tool runner class runs the 'run' function -> inside the class
  }

}