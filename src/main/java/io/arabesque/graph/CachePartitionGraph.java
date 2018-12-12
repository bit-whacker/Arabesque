package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.search.steps.PartitionCache;

public class CachePartitionGraph extends PartitionGraph {
    public CachePartitionGraph(Configuration config) {
        super(config);
    }

    @Override
    protected UnsafeCSRGraphSearch getPartition(int partitionId) {
        return PartitionCache.getGraphForPartition(partitionId);
    }

    @Override
    protected void setPartition(int partition, UnsafeCSRGraphSearch dataGraph) {
        PartitionCache.setGraphForPartition(partition, dataGraph);
    }

    @Override
    protected boolean isPartitionInMemory(int partition) {
        return PartitionCache.isPartitionInMemory(partition);
    }
}
