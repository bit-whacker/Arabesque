package io.arabesque.utils;

import io.arabesque.conf.SparkConfiguration;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.*;

public class MainGraphPartitioner implements Runnable, Serializable {
    private String inputGraphPath;
    protected String dataPartitionDir;
    private int numPartitions;
    private ArrayList<Integer> vertexIndex = new ArrayList<>();
    private ArrayList<Integer> edgeIndex = new ArrayList<>();
    private long verticesIndexLabel;
    private HashMap<Integer, ArrayList<Integer>> reverseVertexlabel;
    private HashMap<Integer, Integer> reverseVertexlabelCount;
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
    protected String partitionedPath;

    final static long INT_SIZE_IN_BYTES = 4;

    public MainGraphPartitioner(SparkConfiguration config) {
        inputGraphPath = config.getString(config.SEARCH_MAINGRAPH_PATH,config.SEARCH_MAINGRAPH_PATH_DEFAULT);
        partitionedPath = config.getString(config.PARTITION_PATH, "");
        dataPartitionDir = config.getString(config.DATA_PARTITION_DIR,"");
        int numWorkers = config.getInteger(config.NUM_WORKERS, 1);
        int numThreads = config.getInteger(config.NUM_THREADS,1);
        numPartitions = numWorkers*numThreads;
        totalVertices = config.getInteger(config.SEARCH_NUM_VERTICES, config.SEARCH_NUM_VERTICES_DEFAULT);
        totalEdges = config.getInteger(config.SEARCH_NUM_EDGES, config.SEARCH_NUM_EDGES_DEFAULT);
        verticesIndexLabel = UNSAFE.allocateMemory((totalVertices + 1L) * INT_SIZE_IN_BYTES);
        reverseVertexlabel = new HashMap<>();
        reverseVertexlabelCount = new HashMap<>();
        try{
            setPartitionDir();
        } catch(URISyntaxException e) {
            throw new RuntimeException(e);
        }
        File partitionDir = new File(dataPartitionDir);
        if(!partitionDir.exists()) {
            partitionDir.mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(partitionDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try{
            String currPath = new File(MainGraphPartitioner.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            System.out.println("Current class path for partitioner: " + currPath);
        } catch(URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public MainGraphPartitioner() {
        inputGraphPath = "hdfs://localhost:8020/input/citeseer.graph";
        partitionedPath = "hdfs://localhost:8020/input/partitions/";
        dataPartitionDir = "data/partitions";
        int numWorkers = 1;
        int numThreads = 5;
        numPartitions = numWorkers*numThreads;
        totalVertices = 3312;
        totalEdges = 9072;
        verticesIndexLabel = UNSAFE.allocateMemory((totalVertices + 1L) * INT_SIZE_IN_BYTES);
        reverseVertexlabel = new HashMap<>();
        reverseVertexlabelCount = new HashMap<>();
        try{
            setPartitionDir();
        } catch(URISyntaxException e) {
            throw new RuntimeException(e);
        }
        File partitionDir = new File(dataPartitionDir);
        if(!partitionDir.exists()) {
            partitionDir.mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(partitionDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try{
            String currPath = new File(MainGraphPartitioner.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            System.out.println("Current class path for partitioner: " + currPath);
        } catch(URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNumPartitions() {return numPartitions;}

    private void setPartitionDir() throws URISyntaxException {
        String currPath = new File(MainGraphPartitioner.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
        ArrayList<String> splitPath = new ArrayList<>(Arrays.asList(currPath.split("/")));
        if(splitPath.size() < 2) { throw new RuntimeException("Something wrong with JAR path: " + currPath); }
        splitPath.remove(splitPath.size()-1);
        splitPath.remove(splitPath.size()-1);
        splitPath.add("");
        dataPartitionDir = String.join("/",splitPath) + dataPartitionDir + "/";
        System.out.println("Data partition dir: " + dataPartitionDir);
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
        return new IntArrayList(reverseVertexlabel.get(vertexLabel));
    }

    public long getNumberVerticesWithLabel(int label) {
        int numVerticesWithLabel = reverseVertexlabelCount.getOrDefault(label,-1);
        if (numVerticesWithLabel == -1){
            return totalVertices;
        } else {
            return numVerticesWithLabel;
        }
    }


    protected InputStream readFile(String path) {
        try {
            Path filePath = new Path(path);
            FileSystem fs = filePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
            return fs.open(filePath);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void serializeGraph(Integer idx) {
        try{
            UnsafeCSRGraphSearch dataGraph = new UnsafeCSRGraphSearch(dataPartitionDir + idx + ".txt",0);
            FileOutputStream fout = new FileOutputStream(dataPartitionDir + idx + ".ser");
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(dataGraph);
        } catch(Exception e) {
            System.out.println("Error serializing graph");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected void copyFileToStore(String hdfsPath, String path)  throws IOException {
        Path targetPath = new Path(hdfsPath);
        FileSystem fs = targetPath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        fs.copyFromLocalFile(new Path(path), targetPath);
    }

    protected void deleteFile(String path) {
        try {
            Files.deleteIfExists(new File(path).toPath());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void copyPartitionToDataStore(int fileIdx) {
        try {
            String path = dataPartitionDir + fileIdx + ".txt";
            String hdfsPath = partitionedPath + fileIdx;
            copyFileToStore(hdfsPath, path);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void serializePartitions() {
        for (int idx = 0; idx < numPartitions; idx++) {
            try {
                serializeGraph(idx);
                String path = dataPartitionDir + idx + ".ser";
                String targetPath = partitionedPath + idx + ".ser";
                copyFileToStore(targetPath, path);
                deleteFile(dataPartitionDir + idx + ".txt");
                deleteFile(path);
            } catch(IOException e) {
                System.out.println("Error serializing partition: " + idx);
                throw new RuntimeException(e);
            }
        }
    }

    public int binarySearch(ArrayList<Integer> currList, int start, int end, int id) {
        if(start==end) {return start;}
        int mid = (start + end)/2;
        if(mid + 1 < end) {
            if(id >= currList.get(mid) && id < currList.get(mid + 1)) { return mid + 1; }
        }
        if(mid - 1 >= 0) {
            if(id < currList.get(mid) && id >= currList.get(mid - 1)) { return mid; }
        }
        if(id < currList.get(mid)) {
            return binarySearch(currList, start, mid, id);
        }
        else {
            return binarySearch(currList, mid, end, id);
        }
    }


    public int getIdxByVertex(int vertexId) {
        if(vertexId >= totalVertices) { throw new RuntimeException("Vertex index out of bounds: " + vertexId); }
        try {
            return binarySearch(vertexIndex, 0, vertexIndex.size(), vertexId);
        }
        catch(StackOverflowError e) {
            System.out.println("Vertex id: " + vertexId);
            throw new RuntimeException(e);
        }
    }

    public int getIdxByEdge(int edgeId) {
        if(edgeId >= totalEdges) { throw new RuntimeException("Edge index out of bounds: " + edgeId); }
        try {
            return binarySearch(edgeIndex, 0, edgeIndex.size(), edgeId);
        }
        catch(StackOverflowError e) {
            System.out.println("Edge id: " + edgeId);
            throw new RuntimeException(e);
        }
    }


    private int getPartitionCount() {
        if(totalVertices%numPartitions==0) {
            return (int)totalVertices/numPartitions;
        }
        else {
            int partitionCount = (int)Math.ceil((double)totalVertices /(double)numPartitions);
            if (partitionCount == 0 || partitionCount == 1) {
                partitionCount = (int) totalVertices;
                numPartitions = 1;
            }
            System.out.println("Partition count is: " + partitionCount);
            return partitionCount;
        }
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
            int totalVertices = 0;
            int fileIdx = 0;
            int partitionVertices = 0;
            int partitionEdges = 0;
            int vertexOffset = 0;
            int edgeOffset = 0;
            int totalEdges = 0;
            PrintWriter out = getFileWriterByIdx(fileIdx);
            while (line != null){
                if (line.startsWith("#")) {
                    line = reader.readLine();
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(line);
                int vertexId = Integer.parseInt(tokenizer.nextToken());
                int vertexLabel = Integer.parseInt(tokenizer.nextToken());
                if(!reverseVertexlabelCount.containsKey(vertexLabel)) { reverseVertexlabelCount.put(vertexLabel,0); }
                else {
                    int count = reverseVertexlabelCount.get(vertexLabel);
                    count++;
                    reverseVertexlabelCount.put(vertexLabel, count);
                }
                ArrayList<Integer> list = reverseVertexlabel.get(vertexLabel);
                if (list == null){
                    list = new ArrayList<>();
                    reverseVertexlabel.put(vertexLabel,list);
                }
                list.add(vertexId);
                totalVertices++;
                partitionVertices++;
                int currEdges = getEdgeCount(tokenizer);
                partitionEdges += currEdges;
                totalEdges += currEdges;
                setVertexLabel(vertexId, vertexLabel);
                out.println(line);
                line = reader.readLine();
                if(totalVertices%partitionCount == 0) {
                    out.close();
                    addGraphMetaData(partitionVertices, partitionEdges, vertexOffset, edgeOffset, fileIdx);
                    copyPartitionToDataStore(fileIdx);
                    vertexIndex.add(totalVertices);
                    edgeIndex.add(totalEdges);
                    if(line!=null) {
                        vertexOffset = totalVertices;
                        edgeOffset = totalEdges;
                        partitionEdges = 0;
                        partitionVertices = 0;
                        fileIdx += 1;
                        out = getFileWriterByIdx(fileIdx);
                    } else {
                        break;
                    }
                }
            }

            if(totalVertices%partitionCount!=0) {
                out.close();
                addGraphMetaData(partitionVertices, partitionEdges, vertexOffset, edgeOffset, fileIdx);
                copyPartitionToDataStore(fileIdx);
                edgeIndex.add(totalEdges);
                vertexIndex.add(totalVertices);
            }

            System.out.println(vertexIndex.toString() + " " + edgeIndex.toString());

        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String args[]) {
        MainGraphPartitioner graphObj = new MainGraphPartitioner();
        Thread thread = new Thread(graphObj);
        thread.start();
        try {
            thread.join();
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(graphObj.getIdxByVertex(3311));
        System.out.println(graphObj.getIdxByVertex(73));
        /*
        System.out.println(graphObj.testVertexByIndex(828));
        System.out.println(graphObj.testVertexByIndex(3311));
        System.out.println(graphObj.testVertexByIndex(0));
        System.out.println(graphObj.testVertexByIndex(1523));
        System.out.println(graphObj.testEdgeByIndex(780));
        System.out.println(graphObj.testEdgeByIndex(6712));
        System.out.println(graphObj.testEdgeByIndex(9071));
        System.out.println(graphObj.testEdgeByIndex(0));
        assert(graphObj.getIdxByVertex(0)==graphObj.testVertexByIndex(0));
        assert(graphObj.getIdxByVertex(3311)==graphObj.testVertexByIndex(3311));
        assert(graphObj.getIdxByVertex(828)==graphObj.testVertexByIndex(828));
        assert(graphObj.getIdxByVertex(1523)==graphObj.testVertexByIndex(1523));
        assert(graphObj.getIdxByEdge(780)==graphObj.testEdgeByIndex(780));
        assert(graphObj.getIdxByEdge(6712)==graphObj.testEdgeByIndex(6712));
        assert(graphObj.getIdxByEdge(9071)==graphObj.testEdgeByIndex(9071));
        assert(graphObj.getIdxByEdge(0)==graphObj.testEdgeByIndex(0));
        */
    }
}
