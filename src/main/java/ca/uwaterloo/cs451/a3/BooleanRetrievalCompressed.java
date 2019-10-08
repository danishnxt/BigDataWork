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

import org.apache.arrow.vector.complex.reader.BaseReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListOfInts;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.*;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int reducers;
  private String stringInit = "";
  private MapFile.Reader [] indexArr;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {

    ContentSummary cs = fs.getContentSummary(new Path(indexPath));
    reducers = ((((int) cs.getFileCount()) - 1)/2); // removing the single extra file
    indexArr = new MapFile.Reader[reducers]; // declared

//    System.out.println("Hello there -> Here are out files " + Integer.toString(reducers));

    for (int i = 0; i < reducers; i++) {

      if (i < 10) {
        stringInit = "/part-r-0000";
      } else {
        stringInit = "/part-r-000";
      }

      indexArr[i] = new MapFile.Reader(new Path(indexPath + stringInit + Integer.toString(i)), fs.getConf());
    }

//    System.out.println("ARE WE OK THIS FAR? I1"); // decoding happens after this we need to be ok to this point
//    index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
//    System.out.println("ARE WE OK THIS FAR? I2"); // decoding happens after this we need to be ok to this point
    collection = fs.open(new Path(collectionPath));
//    System.out.println("ARE WE OK THIS FAR? I3"); // decoding happens after this we need to be ok to this point
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {

    // THIS IS WHERE MOST OF THE CODE CHANGE WILL TAKE PLACE AS WE DECODE FROM THE STOP GAP FORMAT INTO THIS

    Text key = new Text();
    BytesWritable value = new BytesWritable(); // need to load it into the correct datatype

    // decide which index to check for value -> replicate behaviour of the partioner from the other file
    key.set(term);
    int partitionSelect = ((term.hashCode() & Integer.MAX_VALUE) % reducers);
    indexArr[partitionSelect].get(key, value);

    // DECODE DATA
    byte[] dataReadBytes = value.getBytes(); // pull data into array
    ArrayListWritable<PairOfInts> returnValue = new ArrayListWritable<PairOfInts>(); // will populate results here

    ByteArrayInputStream byteStreamRead = new ByteArrayInputStream(dataReadBytes);
    DataInputStream dataStreamRead = new DataInputStream(byteStreamRead); // ready to access

    int cumDocVal = 0; // will keeping adding in this to get old values back
    int cumDF = WritableUtils.readVInt(dataStreamRead);

    // For all values, add up cummulatively the dataGap and return from VINT to int the freq counts

    for (int i = 0; i < cumDF; i++) {
      int valueGap = WritableUtils.readVInt(dataStreamRead);
      int freq = WritableUtils.readVInt(dataStreamRead);

//      System.out.println("  DEBUG POINT 2 ->" + Integer.toString(valueGap));
//      System.out.println("  DEBUG POINT 3 ->" + Integer.toString(freq));

      cumDocVal = cumDocVal + valueGap;

//      System.out.println("  DEBUG POINT 4 ->" + Integer.toString(cumDocVal));
      returnValue.add(new PairOfInts(cumDocVal, freq)); //
    }

    return returnValue; //
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
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

//    System.out.println("ARE WE OK THIS FAR? 1"); // decoding happens after this we need to be ok to this point

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();

//    System.out.println("ARE WE OK THIS FAR? 4"); // decoding happens after this we need to be ok to this point

    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}