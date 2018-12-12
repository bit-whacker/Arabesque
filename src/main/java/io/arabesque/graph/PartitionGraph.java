package io.arabesque.graph;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntIterator;
import com.koloboke.function.IntConsumer;
import io.arabesque.conf.Configuration;
import io.arabesque.utils.AwsS3Utils;
import io.arabesque.utils.MainGraphPartitioner;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nonnull;
import java.io.*;

public class PartitionGraph implements SearchGraph, Externalizable {
    private LRUMap<Integer, UnsafeCSRGraphSearch> graphMap;
    private MainGraphPartitioner partitioner;
    private Configuration config;
    private int numLabels;
    private boolean fastNeighbors;
    private String partitionedPath;
    private AwsS3Utils s3Obj;
    private boolean isBinary;

    public PartitionGraph(Configuration config) {
        partitioner = config.getPartitioner();
        double partitionRatio = config.getDouble(config.PARTITION_RATIO,0.5);
        int numPartitions = (int)Math.floor(partitionRatio * partitioner.getNumPartitions());
        numLabels = config.getInteger(config.SEARCH_NUM_LABELS, config.SEARCH_NUM_LABELS_DEFAULT);
        this.config = config;
        fastNeighbors = config.getBoolean(config.SEARCH_FASTNEIGHBORS, config.SEARCH_FASTNEIGHBORS_DEFAULT);
        partitionedPath = config.getString(config.PARTITION_PATH, "");
        graphMap = new LRUMap<>(numPartitions);
        s3Obj = new AwsS3Utils();
        isBinary = config.getBoolean(config.IS_BINARY, config.IS_BINARY_DEFAULT);
    }

    private UnsafeCSRGraphSearch readPartitionFromBinary(int partition) throws IOException, ClassNotFoundException {
        String path = partitionedPath + partition + ".ser";
        if(path.startsWith(config.S3_SUBSTR)) {
            InputStream is = s3Obj.readFromPath(path);
            ObjectInputStream reader = new ObjectInputStream(is);
            UnsafeCSRGraphSearch dataGraph = (UnsafeCSRGraphSearch) reader.readObject();
            reader.close();
            is.close();
            return dataGraph;
        }
        else {
            org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path);
            FileSystem fs = hdfsPath.getFileSystem(new org.apache.hadoop.conf.Configuration());
            ObjectInputStream reader = new ObjectInputStream(fs.open(hdfsPath));
            UnsafeCSRGraphSearch dataGraph = (UnsafeCSRGraphSearch) reader.readObject();
            reader.close();
            fs.close();
            return dataGraph;
        }
    }

    private UnsafeCSRGraphSearch readPartitionFromFile(int partition) throws IOException {
        String path = partitionedPath + partition;
        if(path.startsWith(config.S3_SUBSTR)) {
            return new UnsafeCSRGraphSearch(path, true);
        }
        else {
            return new UnsafeCSRGraphSearch(new org.apache.hadoop.fs.Path(path), true);
        }

    }

    public void readPartition(int partition) {
        UnsafeCSRGraphSearch dataGraph;
        try {
            if(isBinary) {
                dataGraph = readPartitionFromBinary(partition);
            }
            else {
                dataGraph = readPartitionFromFile(partition);
            }
            setPartition(partition, dataGraph);
        }
        catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected UnsafeCSRGraphSearch getPartition(int partitionId) {
        return graphMap.get(partitionId);
    }

    protected void setPartition(int partition, UnsafeCSRGraphSearch dataGraph) {
        graphMap.put(partition, dataGraph);
    }

    protected boolean isPartitionInMemory(int partition) {
        return graphMap.containsKey(partition);
    }

    private int getPartition(int vertexId, boolean vertexFlag) {
        int partitionId = 0;
        if(vertexFlag) { partitionId = partitioner.getIdxByVertex(vertexId); }
        else { partitionId = partitioner.getIdxByEdge(vertexId); }
        if(!isPartitionInMemory(partitionId)) {
            long startTime = System.currentTimeMillis();
            readPartition(partitionId);
            long deltaTime = System.currentTimeMillis() - startTime;
            //System.out.println("Partition " + partitionId + " read time: " + deltaTime);
        }
        return partitionId;
    }

    @Override
    public int getNeighborhoodSizeWithLabel(int i, int label) {
        int partitionId = getPartition(i, true);
        return getPartition(partitionId).getNeighborhoodSizeWithLabel(i, label);
    }

    @Override
    public long getNumberVerticesWithLabel(int label) {
        return partitioner.getNumberVerticesWithLabel(label);
    }

    @Override
    public boolean hasEdgesWithLabels(int source, int destination, IntArrayList edgeLabels) {
        throw new RuntimeException("Shouldn't be called");
    }

    @Override
    public void setIteratorForNeighborsWithLabel(int vertexId, int vertexLabel, IntIterator _one) {
        PartitionedIterator one = (PartitionedIterator) _one;
        one.setVertexLabel(vertexId, vertexLabel);
    }

    @Override
    public IntArrayList getVerticesWithLabel(int vertexLabel) {
        return partitioner.getVerticesWithLabel(vertexLabel);
    }

    public IntArrayList filterVerticesByPartition(IntArrayList vertexList, int partition) {
        IntArrayList filteredVertices = new IntArrayList();
        UnsafeCSRGraphSearch graph = getPartition(partition);
        long start = graph.getVertexOffset();
        long end = start + graph.getTotalVertices();
        for(int vertex: vertexList) {
            if(vertex >= start && vertex < end) {
                filteredVertices.add(vertex);
            }
        }
        return filteredVertices;
    }

    @Override
    public IntIterator createNeighborhoodSearchIterator() {
        return new PartitionedIterator(this);
    }

    @Override
    public boolean isNeighborVertexWithLabel(int sourceVertexId, int destVertexId, int destinationLabel) {
        //Rewrite this with new Binary search. Implement binary search here
        if(fastNeighbors) {
            int partitionId = getPartition(sourceVertexId, true);
            return getPartition(partitionId).isNeighborVertexWithLabel(sourceVertexId, destVertexId, destinationLabel);
        }
        return isNeighborVertexWithLabel_(sourceVertexId, destVertexId, destinationLabel);
    }

    private boolean isNeighborVertexWithLabel_(int sourceVertexId, int destVertexId, int destinationLabel) {
        int start;
        int end;

        // First check if destination has this label.
        if (numLabels > 1 && destinationLabel>=0){

            if (partitioner.getVertexLabel(destVertexId)!=destinationLabel){
                //Doesn't matter if they connect they don't have the same label.
                return false;
            }
        }

        if (destinationLabel >= 0){
            //Matches any labels, so we just need to have a common neighbor.
            start = getVertexNeighborLabelStart(sourceVertexId, destinationLabel);
            end = getVertexNeighborLabelEnd(sourceVertexId, destinationLabel);
            if (start < 0 || end > partitioner.totalEdges){
                throw new RuntimeException("Accessing above the limits: " + start + "  " + end);
            }
            final int key = binarySearchEdges(start, end, destVertexId);
            return key >= 0;
        }

        // If label is negative is a special label that matches all the labels.
        // No binary search is possible across labels. Only within a label.
        for (int i = 0; i < numLabels; i++){
            start = getVertexNeighborLabelStart(sourceVertexId, i);
            end = getVertexNeighborLabelEnd(sourceVertexId, i);

            if (start < 0 || end > partitioner.totalEdges){
                throw new RuntimeException("Accessing above the limits:"+start+"  "+end);
            }

            final int key = binarySearchEdges(start, end, destVertexId);
            if(key >= 0){
                return true;
            }
        }
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        throw new RuntimeException("Shouldn't be used for Search");
    }

    @Override
    public MainGraph addVertex(Vertex vertex) {
        throw new RuntimeException("Should not be used. Exists for testing other classes only");
    }

    @Override
    public Vertex[] getVertices() {
        return null;
    }

    @Override
    public Vertex getVertex(int vertexId) {
        return null;
    }

    @Override
    public int getNumberVertices() {
        return (int)partitioner.totalVertices;
    }

    @Override
    public Edge[] getEdges() {
        return null;
    }

    @Override
    public Edge getEdge(int edgeId) {
        return null;
    }

    @Override
    public int getNumberEdges() {
        return (int)partitioner.totalEdges;
    }

    @Override
    public ReclaimableIntCollection getEdgeIds(int v1, int v2) {
        return null;
    }

    @Override
    public MainGraph addEdge(Edge edge) {
        throw new RuntimeException("Using only for tests now and old code...");
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
        return (getEdgeSource(edge1Id) == getEdgeSource(edge2Id) ||
                getEdgeSource(edge1Id) == getEdgeDst(edge2Id) ||
                getEdgeDst(edge1Id) == getEdgeSource(edge2Id) ||
                getEdgeDst(edge1Id) == getEdgeDst(edge2Id));
    }

    @Override
    public boolean isNeighborEdge(int src1, int dest1, int edge2) {
        return false;
    }

    @Override
    public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
        return null;
    }

    @Override
    public IntCollection getVertexNeighbours(int vertexId) {
        return null;
    }

    @Override
    public boolean isEdgeLabelled() {
        return false;
    }

    @Override
    public boolean isMultiGraph() {
        return config.isGraphMulti();
    }

    private int getVertexPos(long index) {
        int partitionId = getPartition((int)index, true);
        return getPartition(partitionId).getVertexPos(index);
    }

    @Override
    public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {
        throw new RuntimeException("Shouldn't be used for Search");
    }

    @Override
    public int getVertexLabel(int v) {
        return partitioner.getVertexLabel(v);
    }

    @Override
    public int getEdgeLabel(int edgeId) {
        throw new RuntimeException("Doesn't have a label");
    }

    @Override
    public int getEdgeSource(int edgeId) {
        int partitionId = getPartition(edgeId,false);
        return getPartition(partitionId).getEdgeSource(edgeId);
    }

    @Override
    public int getEdgeDst(int edgeId) {
        int partitionId = getPartition(edgeId,false);
        return getPartition(partitionId).getEdgeDst(edgeId);
    }

    @Override
    public int neighborhoodSize(int vertexId) {
        return getVertexPos(vertexId+1) - getVertexPos(vertexId);
    }

    @Override
    public void processEdgeNeighbors(int vertexId, IntConsumer intAddConsumer) {
        final int start = getVertexPos(vertexId);
        final int end = getVertexPos(vertexId+1);

        for (int i = start; i<end; i++) {
            intAddConsumer.accept(i);
        }
    }

    protected int binarySearchEdges(int fromIndex, int toIndex,
                                int key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = getEdgeDst(mid);
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    @Override
    public void processVertexNeighbors(int vertexId, IntConsumer intAddConsumer) {
        final int start = getVertexPos(vertexId);
        final int end = getVertexPos(vertexId+1);

        for (int i = start; i<end; i++){
            intAddConsumer.accept(getEdgeDst(i));
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    protected int getVertexNeighborLabelStart(int i, int label){
        int partitionId = getPartition(i, true);
        return getPartition(partitionId).getVertexNeighborLabelStart(i,label);
    }

    protected int getVertexNeighborLabelEnd(int i, int label){
        int partitionId = getPartition(i, true);
        return getPartition(partitionId).getVertexNeighborLabelEnd(i,label);
    }

    public class PartitionedIterator implements IntIterator {
        protected PartitionGraph graph;
        protected int pos;
        protected int end;

        PartitionedIterator(PartitionGraph graph) {
            this.graph = graph;
        }


        void setVertexLabel(int vertexId, int vertexLabel){
            if (vertexLabel>=0) {
                pos = graph.getVertexNeighborLabelStart(vertexId, vertexLabel);
                end = graph.getVertexNeighborLabelEnd(vertexId, vertexLabel);
            }
            else{
                //Special label matches all the labels.
                pos = graph.getVertexNeighborLabelStart(vertexId, 0);
                end = graph.getVertexNeighborLabelEnd(vertexId, numLabels-1);

            }
        }

        @Override
        public int nextInt() {
            int vertexId = graph.getEdgeDst(pos);
            ++pos;
            return vertexId;
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean hasNext() {
            return end > pos;
        }

        @Override
        public Integer next() {
            throw new RuntimeException("Shouldn't be used");
        }

        @Override
        public void remove() {
            throw new RuntimeException("Shouldn't be used");
        }
    }
}
