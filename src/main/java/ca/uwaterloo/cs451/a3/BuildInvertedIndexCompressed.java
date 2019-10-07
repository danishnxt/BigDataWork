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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {

    private static final IntWritable EMIT_COUNT = new IntWritable();
    private static final PairOfStringInt KEY = new PairOfStringInt();

    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // EMIT ((),).
      for (PairOfObjectInt<String> e : COUNTS) {
        EMIT_COUNT.set(e.getRightElement());
        KEY.set(e.getLeftElement(), (int) docno.get());
        context.write(KEY, EMIT_COUNT);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {

    private static final IntWritable DF = new IntWritable();

    private static final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    private static final DataOutputStream dataStream = new DataOutputStream(byteStream);
    // the second needs the first byteStream to support it //
    // variables to maintain counts //

    String currentRunningWord = ""; // word we're currently building list for
    int cumDF = 0;
    int priorGapID = 0; // start at zero and increment

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

      if (!currentRunningWord.equals(""))  { // if yes then we've hit first word, no emit
        if (!currentRunningWord.equals(key.getLeftElement())) { // word complete emit

          dataStream.flush();
          byteStream.flush();

          ByteArrayOutputStream byteEmitVal = new ByteArrayOutputStream(); // same as before, only for reordering
          DataOutputStream dataEmitVal = new DataOutputStream(byteEmitVal);

          WritableUtils.writeVInt(dataEmitVal, cumDF); // this need to be first thing that is read
          dataEmitVal.write(byteStream.toByteArray()); // add the rest after

          BytesWritable emitList = new BytesWritable(byteEmitVal.toByteArray());
          context.write(new Text(currentRunningWord), emitList);

          // reset all
          byteStream.reset(); // go again for next list
          priorGapID = 0;
          cumDF = 0;

        }
      }

      while (iter.hasNext()) {
        // push values onto list for current word
        WritableUtils.writeVInt(dataStream, (key.getRightElement() - priorGapID)); // reduced value
        WritableUtils.writeVInt(dataStream, iter.next().get());
        priorGapID = key.getRightElement();

        cumDF++; // keep an eye on total
      }

      // done with all these values, update current word (will override if next value = same word)
      currentRunningWord = key.getLeftElement(); // to compare to next time

    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {

      dataStream.flush();
      byteStream.flush();

      ByteArrayOutputStream byteEmitVal = new ByteArrayOutputStream(); // same as before, only for reordering
      DataOutputStream dataEmitVal = new DataOutputStream(byteEmitVal);

      WritableUtils.writeVInt(dataEmitVal, cumDF); // this need to be first thing that is read
      dataEmitVal.write(byteStream.toByteArray()); // add the rest after

      BytesWritable emitList = new BytesWritable(byteEmitVal.toByteArray());
      context.write(new Text(currentRunningWord), emitList);

      byteStream.close();
      dataStream.close();
    }

  }

  // this should sort it itself
  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[path]", required = false, usage = "num reducers")
    int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
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
    }

    LOG.info("Tool: " + BuildInvertedIndex.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndex.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndex.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
