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

import org.apache.hadoop.io.Writable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

  private Type type;
  private int nodeid;
  private int layers; // will indicate how many levels of parallel map reduce jobs running / data contained
  private ArrayList<Float> pagerank;
  private ArrayListOfIntsWritable adjacencyList;

  // LAYERS ADDS A SPACE OVER HEAD BUT SAVES US FROM A PERFORMANCE PENALTY AS GIVEN FOR FUNCTIONS BELOW

  public PageRankNode() {}

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    this.nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacencyList() {
    return adjacencyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    this.adjacencyList = list;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  // MODIFIED // THE LATTER IS THERE FOR FUTURE PROOFING MORE

  public ArrayList<Float> getPageRank() {
    return pagerank;
  }
  public void setPageRank(ArrayList<Float> p) {
    this.pagerank = p;
  }

  public int getlayers() {
    return layers;
  }
  public void setlayers(int p) {
    this.layers = p;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   * @throws IOException if any exception is encountered during object deserialization
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    int b = in.readByte();
    type = mapping[b];
    nodeid = in.readInt();
//    layers = in.readInt();

    pagerank = new ArrayList<Float>();

    // not quite sure where these are used but am going with the required template for now

    if (type.equals(Type.Mass)) {
      for (int i = 0; i < 3; i++) {
        pagerank.add(in.readFloat());
      }
    }

    if (type.equals(Type.Complete)) {
      for (int i = 0; i < 3; i++) {
        pagerank.add(in.readFloat());
      }
    }

    adjacencyList = new ArrayListOfIntsWritable();
    adjacencyList.readFields(in);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   * @throws IOException if any exception is encountered during object serialization
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);
//    out.writeInt(layers);

    float flt_val_hold = 0;

    if (type.equals(Type.Mass)) {
      System.out.println("VALCHK");
      Iterator<Float> iter = pagerank.iterator();
      while (iter.hasNext()){
        flt_val_hold = iter.next(); // get value
        out.writeFloat(flt_val_hold);
      }

      return;
    }

    if (type.equals(Type.Complete)) {
      System.out.println("VALCHKB");
      Iterator<Float> iter = pagerank.iterator();
      while (iter.hasNext()){
        flt_val_hold = iter.next(); // get value
        out.writeFloat(flt_val_hold);
      }
    }

    adjacencyList.write(out);
  }

  @Override
  public String toString() {
    return String.format("{%d %.4f %s}", nodeid, pagerank, (adjacencyList == null ? "[]"
        : adjacencyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException if any exception is encountered during object serialization
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
