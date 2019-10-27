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

package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import java.io.IOException;
import java.sql.Array;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Dictionary;
import java.util.Iterator;
import java.util.HashMap;
import java.lang.Math;


/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPageRankBasic extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(RunPageRankBasic.class);

    // To maintain list of sources
  private static ArrayList<Integer> sourceCheck = new ArrayList<Integer>(); // add the sources here for a quick lookup
  private static int layerCount = 0; // first one here

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {

      String source_strings[] = context.getConfiguration().getStrings("sources");

      for (int i = 0; i < source_strings.length; i++) {
        layerCount = layerCount + 1;
        sourceCheck.add(Integer.parseInt(source_strings[i])); //
      }

    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacencyList());
      intermediateStructure.setlayers(layerCount); // total numbers of node layers

      context.write(nid, intermediateStructure);
      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacencyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacencyList();
//        float mass = node.getPageRank() - (float) StrictMath.log(list.size());

        ArrayList<Float> old_mass = node.getPageRank();
        ArrayList<Float> new_mass = new ArrayList<Float>();
        // run loop to recompute the overall

        for (int i = 0; i < layerCount; i++) {
           new_mass.add(old_mass.get(i) - (float) StrictMath.log(list.size())); // add at end
        }

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          intermediateMass.setlayers(layerCount);
          intermediateMass.setPageRank(new_mass);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1); // this will remain
      context.getCounter(PageRank.massMessages).increment(massMessages); // this should vary on an array
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {

      int massMessages = 0;

      ArrayList<Float> massValue = new ArrayList<Float>();
      // Remember, PageRank mass is stored as a log prob.

      for (int i = 0; i < layerCount; i++) {
        massValue.add(Float.NEGATIVE_INFINITY); // INITIALIZING THIS UP
      }

      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          for (int i = 0; i < layerCount; i++) {
            massValue.set(i, sumLogProbs(massValue.get(i), n.getPageRank().get(i)));
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setlayers(layerCount); // WE CAN REMOVE THIS IF THIS CAUSES PROBLEMS!
        intermediateMass.setPageRank(massValue); // setting up the value as the array

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.

    private ArrayList<Float> totalMass = new ArrayList<Float>();

    @Override
    public void setup(Context context) {
      for (int i = 0; i < layerCount; i++) {
        totalMass.add(Float.NEGATIVE_INFINITY); // maintained across the values for the list
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {

      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      ArrayList<Float> mass = new ArrayList<Float>(); // not messing with anyway the code already functioned
      for (int i = 0; i < layerCount; i++) {
        mass.add(Float.NEGATIVE_INFINITY); // INIT ALL VALUES TO AVOID ADDITION ERRORS
      }

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());
      node.setlayers(layerCount);

      int massMessagesReceived = 0;
      int structureReceived = 0;

      while (values.hasNext()) { // Why does this have a different loop type than the previous one lol
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacencyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.

          for (int i = 0; i < layerCount; i++) {
            mass.set(i, sumLogProbs(mass.get(i), n.getPageRank().get(i)));
          }

          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRank(totalMass);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        for (int i = 0; i < layerCount;i++) {
          totalMass.set(i, sumLogProbs(totalMass.get(i), mass.get(i))); // update total mass across the board
        }


      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);

      for (int i = 0; i < layerCount; i++) {
        out.writeFloat(totalMass.get(i));
      } // writing more values but we know exactly how many to read as well

      out.close();
    }
  }

  // ============================================================================================================

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    private ArrayList<Float> missingMass;

    private int nodeCnt = 0;
    private int layerCountB = 0; // not taking a chance on different jobs running in parallel, we should move
    private ArrayList<Integer> sourceCheckB = new ArrayList<Integer>();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      missingMass = new ArrayList<Float>();

      // PHASE 2 IS A SEPARATE JOB AND HENCE THIS NEEDS TO BE RUN AGAIN
      String source_strings[] = context.getConfiguration().getStrings("sources");
      String missingMass_Str[] = context.getConfiguration().getStrings("MissingMass");

      for (int i = 0; i < source_strings.length; i++) {
        layerCountB = layerCountB + 1;
        sourceCheckB.add(Integer.parseInt(source_strings[i]), 1); //

        // adding to the loop
        missingMass.add(0.0f + Float.parseFloat(missingMass_Str[i]));
      }

      nodeCnt = conf.getInt("NodeCount", 0);
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {

      // only process if anything to be done

      ArrayList<Float> jump = new ArrayList<Float>();
      ArrayList<Float> link = new ArrayList<Float>();

      for (int i = 0; i < layerCountB; i++) {
        jump.add(Float.NEGATIVE_INFINITY);
        jump.add(Float.NEGATIVE_INFINITY);
      }

      ArrayList<Float> p = node.getPageRank(); // for every layer

      for (int i = 0; i < layerCountB; i++) {
        if (sourceCheckB.get(i) == nid.get()) {
          jump.set(i, (float) (Math.log(ALPHA))); // random jump factoring
          link.set(i, (float) Math.log(1.0f - ALPHA) // all missing mass re-distributed
                  + sumLogProbs(p.get(i), (float) (Math.log(missingMass.get(i)))));
        } else {
          // else
          link.set(i, (float) Math.log(1.0f - ALPHA) + p.get(i));

        }
      }

      for (int i = 0; i < layerCountB; i++) {
        p.set(i, sumLogProbs(jump.get(i), link.get(i)));
        node.setPageRank(p);
      }

      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPageRankBasic(), args);
  }

  public RunPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String COMBINER = "useCombiner";
  private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  private static final String RANGE = "range";
  private static final String SOURCE_NODES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(COMBINER, "use combiner"));
    options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
            .withDescription("Source nodes").create(SOURCE_NODES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCE_NODES) ) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    boolean useCombiner = cmdline.hasOption(COMBINER);
    boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
    boolean useRange = cmdline.hasOption(RANGE);

    String sources = cmdline.getOptionValue(SOURCE_NODES);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    LOG.info(" - user range partitioner: " + useRange);
    LOG.info(" - Sources: " + sources);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    ArrayList<Float> mass = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, sources);
    ArrayList<Float> missing = new ArrayList<Float>();

    // Find out how much PageRank mass got lost at the dangling nodes.
//    float missing = 1.0f - (float) StrictMath.exp(mass); // to be computed in Phase 2 now

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, mass, basePath, numNodes, sources);
  }

  private ArrayList<Float> phase1(int i, int j, String basePath, int numNodes,
                                  boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {

    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - useCombiner: " + useCombiner);
    LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info("computed number of partitions: " + numPartitions);
    LOG.info("Source Nodes: " + sources);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    job.getConfiguration().setStrings("sources", sources); // will pass the string list directly

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);

    if (useCombiner) {
      job.setCombinerClass(CombineClass.class);
    }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    ArrayList<Float> mass = new ArrayList<Float>();

    int srcCountLocal = job.getConfiguration().getStrings("sources").length;

    for (int alpha = 0; alpha < srcCountLocal; alpha++) {
      mass.add(Float.NEGATIVE_INFINITY);
    }

    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());

      for (int beta = 0; beta < srcCountLocal; beta++) {
        mass.set(beta, sumLogProbs(mass.get(beta), fin.readFloat()));
      }

      fin.close();
    }

    return mass;
  }

  private void phase2(int i, int j, ArrayList<Float> mass, String basePath, int numNodes, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPageRankBasic.class);

    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);

    job.getConfiguration().setInt("NodeCount", numNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    // ==========================================================================
      // The tricky business of dealing with the String to send back in config file

    ArrayList<Float> missing = new ArrayList<Float>();
    int srcLocalCount = job.getConfiguration().getStrings("sources").length;

    // fix missing value

    String temp = "";

    for (int alphaB = 0; alphaB < srcLocalCount; alphaB++) {
      missing.add((1.0f - (float) StrictMath.exp(mass.get(alphaB)))); // missing now contains all left mass per layer
      LOG.info("missing PageRank mass: " + alphaB + " - " + missing.get(alphaB));
      temp = (temp + Float.toString(missing.get(alphaB)) + ","); // update the string itself // BREAK CHECK POINT
    }

    temp = temp.substring(0, temp.length()-1); // remove the last comma

    // By this point we have out STRING OF missing values
    job.getConfiguration().setStrings("MissingMass", temp);

    // ==========================================================================


    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);



    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
