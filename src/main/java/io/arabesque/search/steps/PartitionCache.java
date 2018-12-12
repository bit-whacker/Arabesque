package io.arabesque.search.steps;

import io.arabesque.graph.UnsafeCSRGraphSearch;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionCache {
    private static ConcurrentHashMap<Integer, UnsafeCSRGraphSearch> graphMap = new ConcurrentHashMap<>();

    public static void setGraphForPartition(int partitionId, UnsafeCSRGraphSearch dataGraph) {
        graphMap.put(partitionId, dataGraph);
    }

    public static UnsafeCSRGraphSearch getGraphForPartition(int partitionId) {
        return graphMap.get(partitionId);
    }

    public static boolean isPartitionInMemory(int partitionId) {
        return graphMap.containsKey(partitionId);
    }
}
