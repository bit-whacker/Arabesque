package io.arabesque.utils;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

public class MainGraphPartitioner implements Runnable, Serializable {
    private String inputGraphPath;
    private String dataPartitionDir;
    private int numPartitions;
    private ArrayList<Integer> vertexIndex = new ArrayList<>();
    private ArrayList<Integer> edgeIndex = new ArrayList<>();
    private long verticesIndexLabel;
    private HashIntObjMap<IntArrayList> reverseVertexlabel;
    private HashIntIntMap reverseVertexlabelCount;
    static final sun.misc.Unsafe UNSAFE;
    public long totalVertices;
    public long totalEdges;
    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("UnsafeArrayReads: Failed to " +
                    "get unsafe", e);
        }
    }
    final static long INT_SIZE_IN_BYTES = 4;

    public MainGraphPartitioner(SparkConfiguration config) {
        inputGraphPath = config.getString(config.SEARCH_MAINGRAPH_PATH,config.SEARCH_MAINGRAPH_PATH_DEFAULT);
        dataPartitionDir = config.getString(config.DATA_PARTITION_DIR,"");
        numPartitions = config.numPartitions();
        totalVertices = config.getInteger(config.SEARCH_NUM_VERTICES, config.SEARCH_NUM_VERTICES_DEFAULT);
        totalEdges = config.getInteger(config.SEARCH_NUM_EDGES, config.SEARCH_NUM_EDGES_DEFAULT);
        verticesIndexLabel = UNSAFE.allocateMemory((totalVertices + 1L) * INT_SIZE_IN_BYTES);
        reverseVertexlabel = HashIntObjMaps.newMutableMap();
        reverseVertexlabelCount = HashIntIntMaps.newMutableMap();
        try {
            FileUtils.cleanDirectory(new File(dataPartitionDir));
        } catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public MainGraphPartitioner() {
        //Created for testing
        inputGraphPath = "hdfs://localhost:8020/input/citeseer.graph";
        dataPartitionDir = "/Users/ambermadvariya/src/Arabesque/data/partitions/";
        numPartitions = 4;
        totalVertices = 3312;
        totalEdges = 9072;
        verticesIndexLabel = UNSAFE.allocateMemory((totalVertices + 1L) * INT_SIZE_IN_BYTES);
        reverseVertexlabel = HashIntObjMaps.newMutableMap();
        reverseVertexlabelCount = HashIntIntMaps.newMutableMap();
        try {
            FileUtils.cleanDirectory(new File(dataPartitionDir));
        } catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    private void setVertexLabel(long index, int value) {
        if (index > totalVertices || index < 0 ){
            throw new RuntimeException("Above limit vertex label: " + index + ", numVertices = " + totalVertices);
        }

        UNSAFE.putInt(verticesIndexLabel + (index*INT_SIZE_IN_BYTES), value);
    }

    public int getVertexLabel(int index) {
        if (index > totalVertices || index < 0 ){
            throw new RuntimeException("Above limit vertex: Get "+index);
        }

        return UNSAFE.getInt(verticesIndexLabel+(index*INT_SIZE_IN_BYTES));
    }

    public IntArrayList getVerticesWithLabel(int vertexLabel) {
        if (vertexLabel < 0){
            // should not invoke this method if we don't look for a specific label
            return null;
        }
        return reverseVertexlabel.get(vertexLabel);
    }

    public long getNumberVerticesWithLabel(int label) {
        int numVerticesWithLabel = reverseVertexlabelCount.getOrDefault(label,-1);
        if (numVerticesWithLabel == -1){
            return totalVertices;
        } else {
            return numVerticesWithLabel;
        }
    }


    public InputStream readFile(String path) throws IOException {
        Path filePath = new Path(path);
        FileSystem fs = filePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        return fs.open(filePath);
    }

    private void copyToHDFS(String path, String hdfsPath) {
        try {
            Path targetPath = new Path(hdfsPath);
            FileSystem fs = targetPath.getFileSystem(new org.apache.hadoop.conf.Configuration());
            fs.copyFromLocalFile(new Path(path), targetPath);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getIdxByVertex(int vertexId) {
        for(int i = 0; i < vertexIndex.size(); i++) {
            int partition = vertexIndex.get(i);
            if(partition >= vertexId) {
                return i;
            }
        }
        throw new RuntimeException("Vertex index out of bounds");
    }

    public int getIdxByEdge(int edgeId) {
        for(int i = 0; i < edgeIndex.size(); i++) {
            int partition = edgeIndex.get(i);
            if(partition >= edgeId) {
                return i;
            }
        }
        throw new RuntimeException("Edge index out of bounds");
    }

    public InputStream getFileStream(int idx) {
        try {
            String fileName = inputGraphPath + "-" + idx;
            Path filePath = new Path(fileName);
            FileSystem fs = filePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
            return fs.open(filePath);
        }
        catch(IOException e) {
            System.out.println("invalid fileIdx: " + idx);
            throw new RuntimeException(e);
        }
    }

    private int getPartitionCount() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(readFile(inputGraphPath)));
        String line = reader.readLine();
        int count = 1;
        while (line != null) {
            count++;
            line = reader.readLine();
        }
        int partitionCount = count/numPartitions;
        if(partitionCount == 0) {
            partitionCount = count;
            numPartitions = 1;
        }
        return partitionCount;
    }

    private int getEdgeCount(StringTokenizer tokenizer) {
        int numEdges = 0;
        while (tokenizer.hasMoreTokens()) {
            int neighborId = Integer.parseInt(tokenizer.nextToken());
            numEdges++;
        }
        return numEdges;
    }
    private PrintWriter getFileWriterByIdx(Integer fileIdx) {
        try {
            String fileName = dataPartitionDir + fileIdx.toString() + ".txt";
            File outFile = new File(fileName);
            outFile.createNewFile();
            FileWriter fw = new FileWriter(fileName, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            String first = String.join("", Collections.nCopies(60, " "));
            out.println(first);
            return out;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addGraphMetaData(int numVertices, int numEdges, int vertexOffset, int edgeOffset, int fileIdx) {
        try {
            RandomAccessFile writer = new RandomAccessFile(new File(dataPartitionDir + fileIdx + ".txt"), "rw");
            writer.seek(0);
            writer.write(("#" + numVertices + " " + numEdges + " " + vertexOffset + " " + edgeOffset).getBytes());
            writer.close();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        try {
            int partitionCount = getPartitionCount();
            BufferedReader reader = new BufferedReader(new InputStreamReader(readFile(inputGraphPath)));;
            String line = reader.readLine();
            int currCount = 1;
            int fileIdx = 0;
            int numVertices = 0;
            int numEdges = 0;
            int vertexOffset = 0;
            int edgeOffset = 0;
            int edgeCount = -1;
            PrintWriter out = getFileWriterByIdx(fileIdx);
            while (line != null){
                if (line.startsWith("#")) {
                    line = reader.readLine();
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(line);
                int vertexId = Integer.parseInt(tokenizer.nextToken());
                int vertexLabel = Integer.parseInt(tokenizer.nextToken());
                reverseVertexlabelCount.addValue(vertexLabel,1,0);
                IntArrayList list = reverseVertexlabel.get(vertexLabel);
                if (list == null){
                    list = new IntArrayList(1024);
                    reverseVertexlabel.put(vertexLabel,list);
                }
                list.add(vertexId);
                numVertices++;
                int currEdges = getEdgeCount(tokenizer);
                numEdges += currEdges;
                edgeCount += currEdges;
                setVertexLabel(vertexId, vertexLabel);
                out.println(line);
                line = reader.readLine();
                if(currCount%partitionCount == 0) {
                    out.close();
                    addGraphMetaData(numVertices, numEdges, vertexOffset, edgeOffset, fileIdx);
                    vertexIndex.add(currCount - 1);
                    edgeIndex.add(edgeCount);
                    if(line!=null) {
                        vertexOffset = currCount;
                        edgeOffset = edgeCount;
                        numEdges = 0;
                        numVertices = 0;
                        fileIdx += 1;
                        out = getFileWriterByIdx(fileIdx);
                    } else {
                        break;
                    }
                }
                currCount += 1;
            }

            if(currCount%partitionCount!=0) {
                out.close();
                addGraphMetaData(numVertices, numEdges, vertexOffset, edgeOffset, fileIdx);
                edgeIndex.add(edgeCount);
                vertexIndex.add(currCount - 2);
            }

            System.out.println(vertexIndex.toString() + " " + edgeIndex.toString());

            for(int i = 0; i < numPartitions; i++) {
                String path = dataPartitionDir + i + ".txt";
                String hdfsPath = inputGraphPath + "-" + i;
                copyToHDFS(path, hdfsPath);
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
