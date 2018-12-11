package io.arabesque.conf;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.aggregation.EndAggregationFunction;
import io.arabesque.aggregation.reductions.ReductionFunction;
import io.arabesque.computation.Computation;
import io.arabesque.computation.ExecutionEngine;
import io.arabesque.computation.MasterComputation;
import io.arabesque.computation.WorkerContext;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.computation.comm.CommunicationStrategyFactory;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.optimization.OptimizationSet;
import io.arabesque.optimization.OptimizationSetDescriptor;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.VICPattern;
import io.arabesque.utils.pool.Pool;
import io.arabesque.utils.pool.PoolRegistry;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;

public class Configuration<O extends Embedding> implements java.io.Serializable {
    
    // temp solution to cope with static configurations: changing soon
    protected UUID uuid = UUID.randomUUID();

    private static final Logger LOG = Logger.getLogger(Configuration.class);
    public static final int KB = 1024;
    public static final int MB = 1024 * KB;
    public static final int GB = 1024 * MB;

    public static final int SECONDS = 1000;
    public static final int MINUTES = 60 * SECONDS;
    public static final int HOURS = 60 * MINUTES;

    public static final int K = 1000;
    public static final int M = 1000 * K;
    public static final int B = 1000 * M;

    // "mining" for Arabesque and "search" for QFrag
    public static final String CONF_SYSTEM_TYPE = "system_type";
    public static final String CONF_SYSTEM_TYPE_DEFAULT = "mining";
    public static final String CONF_ARABESQUE_SYSTEM_TYPE = "mining";
    public static final String CONF_QFRAG_SYSTEM_TYPE = "search";

    public static final String CONF_LOG_LEVEL = "arabesque.log.level";
    public static final String CONF_LOG_LEVEL_DEFAULT = "info";
    public static final String CONF_MAINGRAPH_CLASS = "arabesque.graph.class";
    public static final String CONF_MAINGRAPH_CLASS_DEFAULT = "io.arabesque.graph.BasicMainGraph";
    public static final String CONF_MAINGRAPH_PATH = "arabesque.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = "main.graph";
    public static final String CONF_MAINGRAPH_SUBGRAPHS_PATH = "arabesque.graph.subgraphs.location";
    public static final String CONF_MAINGRAPH_SUBGRAPHS_PATH_DEFAULT = "None";
    public static final String CONF_MAINGRAPH_LOCAL = "arabesque.graph.local";
    public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;
    public static final String CONF_MAINGRAPH_EDGE_LABELLED = "arabesque.graph.edge_labelled";
    public static final boolean CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT = false;
    public static final String CONF_MAINGRAPH_MULTIGRAPH = "arabesque.graph.multigraph";
    public static final boolean CONF_MAINGRAPH_MULTIGRAPH_DEFAULT = false;

    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS = "arabesque.optimizations.descriptor";
    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT = "io.arabesque.optimization.ConfigBasedOptimizationSetDescriptor";

    private static final String CONF_COMPRESSED_CACHES = "arabesque.caches.compress";
    private static final boolean CONF_COMPRESSED_CACHES_DEFAULT = false;
    private static final String CONF_CACHE_THRESHOLD_SIZE = "arabesque.cache.threshold";
    private static final int CONF_CACHE_THRESHOLD_SIZE_DEFAULT = 1 * MB;

    public static final String CONF_OUTPUT_ACTIVE = "arabesque.output.active";
    public static final boolean CONF_OUTPUT_ACTIVE_DEFAULT = true;

    public static final String INFO_PERIOD = "arabesque.info.period";
    public static final long INFO_PERIOD_DEFAULT = 60000;

    public static final String CONF_COMPUTATION_CLASS = "arabesque.computation.class";
    public static final String CONF_COMPUTATION_CLASS_DEFAULT = "io.arabesque.computation.ComputationContainer";

    public static final String CONF_MASTER_COMPUTATION_CLASS = "arabesque.master_computation.class";
    public static final String CONF_MASTER_COMPUTATION_CLASS_DEFAULT = "io.arabesque.computation.MasterComputation";

    public static final String CONF_COMM_STRATEGY = "arabesque.comm.strategy";
    public static final String CONF_COMM_STRATEGY_DEFAULT = "odag_sp";

    public static final String CONF_COMM_STRATEGY_ODAGMP_MAX = "arabesque.comm.strategy.odagmp.max";
    public static final int CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT = 100;

    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS = "arabesque.comm.factory.class";
    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT = "io.arabesque.computation.comm.ODAGCommunicationStrategyFactory";

    public static final String CONF_PATTERN_CLASS = "arabesque.pattern.class";
    public static final String CONF_PATTERN_CLASS_DEFAULT = "io.arabesque.pattern.JBlissPattern";

    // TODO: maybe we should the name of this configuration in the future, use
    // odag instead of ezip ?
    public static final String CONF_EZIP_AGGREGATORS = "arabesque.odag.aggregators";
    public static final int CONF_EZIP_AGGREGATORS_DEFAULT = -1;

    public static final String CONF_ODAG_FLUSH_METHOD = "arabesque.odag.flush.method";
    public static final String CONF_ODAG_FLUSH_METHOD_DEFAULT = "flush_by_parts";

    private static final String CONF_2LEVELAGG_ENABLED = "arabesque.2levelagg.enabled";
    private static final boolean CONF_2LEVELAGG_ENABLED_DEFAULT = true;
    private static final String CONF_FORCE_GC = "arabesque.forcegc";
    private static final boolean CONF_FORCE_GC_DEFAULT = false;

    public static final String CONF_OUTPUT_PATH = "arabesque.output.path";
    public static final String CONF_OUTPUT_PATH_DEFAULT = "Output";

    public static final String CONF_DEFAULT_AGGREGATOR_SPLITS = "arabesque.aggregators.default_splits";
    public static final int CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT = 1;

    public static final String CONF_INCREMENTAL_AGGREGATION = "arabesque.aggregation.incremental";
    public static final boolean CONF_INCREMENTAL_AGGREGATION_DEFAULT = false;

    public static final String CONF_AGGREGATION_STORAGE_CLASS = "arabesque.aggregation.storage.class";
    public static final String CONF_AGGREGATION_STORAGE_CLASS_DEFAULT = "io.arabesque.aggregation.AggregationStorage";

    protected static Configuration instance = null;
    private ImmutableClassesGiraphConfiguration giraphConfiguration;

    private boolean useCompressedCaches;
    private int cacheThresholdSize;
    private long infoPeriod;
    private int odagNumAggregators;
    private boolean is2LevelAggregationEnabled;
    private boolean forceGC;
    private CommunicationStrategyFactory communicationStrategyFactory;

    private Class<? extends MainGraph> mainGraphClass;
    private Class<? extends OptimizationSetDescriptor> optimizationSetDescriptorClass;
    private Class<? extends Pattern> patternClass;
    private Class<? extends Computation> computationClass;
    private Class<? extends AggregationStorage> aggregationStorageClass;
    private Class<? extends MasterComputation> masterComputationClass;
    private Class<? extends Embedding> embeddingClass;

    private String outputPath;
    private int defaultAggregatorSplits;

    private transient Map<String, AggregationStorageMetadata> aggregationsMetadata;
    private transient MainGraph mainGraph;
    private boolean isGraphEdgeLabelled;
    protected boolean initialized = false;
    private boolean isGraphMulti = false;

    private UnsafeCSRGraphSearch searchMainGraph;

    public UnsafeCSRGraphSearch getSearchMainGraph() { return searchMainGraph; }
    public void setSearchMainGraph(UnsafeCSRGraphSearch _searchMainGraph) {
        this.searchMainGraph = _searchMainGraph;
    }

    //***** QFrag paramters

    public static final String S3_SUBSTR = "s3";

    public static final String NUM_WORKERS = "num_workers";
    public static final String NUM_THREADS = "num_compute_threads";

    public static final String SEARCH_MAINGRAPH_CLASS = "search.graph.class";
    public static final String SEARCH_MAINGRAPH_CLASS_DEFAULT = "io.arabesque.graph.UnsafeCSRGraphSearch";

    public static final String SEARCH_MAINGRAPH_PATH = "search_input_graph_path"; // no default - done
    public static final String SEARCH_MAINGRAPH_PATH_DEFAULT = null;

    public static final String SEARCH_QUERY_GRAPH_PATH = "search_query_graph_path"; // no default - done
    public static final String SEARCH_QUERY_GRAPH_PATH_DEFAULT = null;

    public static final String SEARCH_OUTLIERS_MIN_MATCHES = "search_outliers_min_matches";
    public static final int SEARCH_OUTLIERS_MIN_MATCHES_DEFAULT = 100;

    public static final String SEARCH_NUM_LABELS = "search_num_labels"; // no default - done
    public static final int SEARCH_NUM_LABELS_DEFAULT = -1;

    public static final String SEARCH_NUM_EDGES = "search_num_edges"; // no default - done
    public static final int SEARCH_NUM_EDGES_DEFAULT = -1;

    public static final String SEARCH_NUM_VERTICES = "search_num_vertices"; // no default - done
    public static final int SEARCH_NUM_VERTICES_DEFAULT = -1;

    public static final String SEARCH_MULTI_VERTEX_LABELS     = "search_multi_vertex_labels";
    public static final boolean SEARCH_MULTI_VERTEX_LABELS_DEFAULT = false;

    public static final String SEARCH_FASTNEIGHBORS   = "search_fastNeighbors";
    public static final boolean SEARCH_FASTNEIGHBORS_DEFAULT = true;

    public static final String SEARCH_INJECTIVE   = "search_injective";
    public static final boolean SEARCH_INJECTIVE_DEFAULT = false;

    public static final String SEARCH_OUTPUT_PATH = "search_output_path";
    public static final String SEARCH_OUTPUT_PATH_DEFAULT = "output_search";

    public static final String SEARCH_WRITE_IN_BINARY   = "search_write_in_binary";
    public static final boolean SEARCH_WRITE_IN_BINARY_DEFAULT = false;

    public static final String SEARCH_BUFFER_SIZE = "search_buffer_size";
    public static final int SEARCH_BUFFER_SIZE_DEFAULT = 8192;

    public static final String SEARCH_OUTPUT_PATH_ACTIVE   = "search_output_active";
    public static final boolean SEARCH_OUTPUT_PATH_ACTIVE_DEFAULT = true;

    public static final String SEARCH_OUTLIERS_PCT = "search_outliers_pct";
    public static final double SEARCH_OUTLIERS_PCT_DEFAULT = 0.001;

    public static final String CONF_MAINGRAPH_FLOAT_EDGE     = "arabesque.graph.float_edge";
    public static final boolean CONF_MAINGRAPH_FLOAT_EDGE_DEFAULT = false;
    public static final String CONF_MAINGRAPH_IS_BINARY      = "arabesque.graph.binary";
    public static final String CONF_MAINGRAPH_VERTICES       = "arabesque.graph.vertices";
    public static final String CONF_MAINGRAPH_EDGES          = "arabesque.graph.edges";
    public static final String CONF_MAINGRAPH_LABELS         = "arabesque.graph.labels";
    public static final String CONF_MAINGRAPH_NUM_EDGE_LABELS= "arabesque.graph.edge_labels";

    private boolean isFloatEdge;
    private boolean isBinary = false;

    //***** End of QFrag paramters

    public UUID getUUID() {
       return uuid;
    }

    public static boolean isUnset() {
       return instance == null;
    }

    public static <C extends Configuration> C get() {
        if (instance == null) {
           LOG.error ("instance is null");
            throw new RuntimeException("Oh-oh, Null configuration");
        }

        if (!instance.isInitialized()) {
           instance.initialize();
        }

        return (C) instance;
    }

    public static void unset() {
       instance = null;
    }

    public synchronized static void setIfUnset(Configuration configuration) {
        if (isUnset()) {
            set(configuration);
        }
    }

    public static void set(Configuration configuration) {
        instance = configuration;

        // Whenever we set configuration, reset all known pools
        // Since they might have initialized things based on a previous configuration
        // NOTE: This is essential for the unit tests
        for (Pool pool : PoolRegistry.instance().getPools()) {
            pool.reset();
        }
    }

    public Configuration(ImmutableClassesGiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;
    }

    public Configuration() {}

    public void initialize() {
        if (initialized) {
            return;
        }

        LOG.info("Initializing Configuration...");

        useCompressedCaches = getBoolean(CONF_COMPRESSED_CACHES, CONF_COMPRESSED_CACHES_DEFAULT);
        cacheThresholdSize = getInteger(CONF_CACHE_THRESHOLD_SIZE, CONF_CACHE_THRESHOLD_SIZE_DEFAULT);
        infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT);
        Class<? extends CommunicationStrategyFactory> communicationStrategyFactoryClass =
                (Class<? extends CommunicationStrategyFactory>) getClass(CONF_COMM_STRATEGY_FACTORY_CLASS, CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT);
        communicationStrategyFactory = ReflectionUtils.newInstance(communicationStrategyFactoryClass);
        odagNumAggregators = getInteger(CONF_EZIP_AGGREGATORS, CONF_EZIP_AGGREGATORS_DEFAULT);
        is2LevelAggregationEnabled = getBoolean(CONF_2LEVELAGG_ENABLED, CONF_2LEVELAGG_ENABLED_DEFAULT);
        forceGC = getBoolean(CONF_FORCE_GC, CONF_FORCE_GC_DEFAULT);
        mainGraphClass = (Class<? extends MainGraph>) getClass(CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT);
        isGraphEdgeLabelled = getBoolean(CONF_MAINGRAPH_EDGE_LABELLED, CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT);
        isGraphMulti = getBoolean(CONF_MAINGRAPH_MULTIGRAPH, CONF_MAINGRAPH_MULTIGRAPH_DEFAULT);
        optimizationSetDescriptorClass = (Class<? extends OptimizationSetDescriptor>) getClass(CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS, CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT);
        patternClass = (Class<? extends Pattern>) getClass(CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT);

        // TODO: Make this more flexible
        if (isGraphEdgeLabelled || isGraphMulti) {
            patternClass = VICPattern.class;
        }

        computationClass = (Class<? extends Computation>) getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT);
        masterComputationClass = (Class<? extends MasterComputation>) getClass(CONF_MASTER_COMPUTATION_CLASS, CONF_MASTER_COMPUTATION_CLASS_DEFAULT);

        aggregationsMetadata = new HashMap<>();

        outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT + "_" + computationClass.getName());

        defaultAggregatorSplits = getInteger(CONF_DEFAULT_AGGREGATOR_SPLITS, CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT);

        Computation<?> computation = createComputation();
        computation.initAggregations();

        OptimizationSetDescriptor optimizationSetDescriptor = ReflectionUtils.newInstance(optimizationSetDescriptorClass);
        OptimizationSet optimizationSet = optimizationSetDescriptor.describe();

        LOG.info("Active optimizations: " + optimizationSet);

        optimizationSet.applyStartup();

        if (mainGraph == null) {
            // Load graph immediately (try to make it so that everyone loads the graph at the same time)
            // This prevents imbalances if aggregators use the main graph (which means that master
            // node would load first on superstep -1) then all the others would load on (superstep 0).
            mainGraph = createGraph();
        }

        optimizationSet.applyAfterGraphLoad();
        initialized = true;
        LOG.info("Configuration initialized");
    }

    public boolean isInitialized() {
       return initialized;
    }

    public ImmutableClassesGiraphConfiguration getUnderlyingConfiguration() {
        return giraphConfiguration;
    }

    public String getString(String key, String defaultValue) {
        return giraphConfiguration.get(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return giraphConfiguration.getBoolean(key, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return giraphConfiguration.getInt(key, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        return giraphConfiguration.getLong(key, defaultValue);
    }

    public Float getFloat(String key, Float defaultValue) {
        return giraphConfiguration.getFloat(key, defaultValue);
    }

    public Class<?> getClass(String key, String defaultValue) {
        try {
            return Class.forName(getString(key, defaultValue));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getStrings(String key, String... defaultValues) {
        return giraphConfiguration.getStrings(key, defaultValues);
    }

    public Class<?>[] getClasses(String key, Class<?>... defaultValues) {
        return giraphConfiguration.getClasses(key, defaultValues);
    }

    public Class<? extends Pattern> getPatternClass() {
        return patternClass;
    }

    public void setPatternClass(Class<? extends Pattern> patternClass) {
       this.patternClass = patternClass;
    }

    public Pattern createPattern() {
        return ReflectionUtils.newInstance(getPatternClass());
    }

    public Class<? extends MainGraph> getMainGraphClass() {
       return mainGraphClass;
    }

    public void setMainGraphClass(Class<? extends MainGraph> graphClass) {
       mainGraphClass = graphClass;
    }

    public boolean isUseCompressedCaches() {
        return useCompressedCaches;
    }

    public int getCacheThresholdSize() {
        return cacheThresholdSize;
    }

    public String getLogLevel() {
       return getString (CONF_LOG_LEVEL, CONF_LOG_LEVEL_DEFAULT);
    }

    public String getMainGraphPath() {
        return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
    }

    public String getMainGraphSubgraphsPath() {
        return getString(CONF_MAINGRAPH_SUBGRAPHS_PATH, CONF_MAINGRAPH_SUBGRAPHS_PATH_DEFAULT);
    }

    public String getSearchMainGraphPath() {
        return getString(SEARCH_MAINGRAPH_PATH, SEARCH_MAINGRAPH_PATH_DEFAULT);
    }

    public long getInfoPeriod() {
        return infoPeriod;
    }

    public <E extends Embedding> E createEmbedding() {
        return (E) ReflectionUtils.newInstance(embeddingClass);
    }

    public Class<? extends Embedding> getEmbeddingClass() {
        return embeddingClass;
    }

    public void setEmbeddingClass(Class<? extends Embedding> embeddingClass) {
        this.embeddingClass = embeddingClass;
    }

    public <G extends MainGraph> G getMainGraph() {
        return (G) mainGraph;
    }

    public <G extends MainGraph> void setMainGraph(G mainGraph) {
        this.mainGraph = mainGraph;
    }

    protected MainGraph createGraph() {
        boolean useLocalGraph = getBoolean(CONF_MAINGRAPH_LOCAL, CONF_MAINGRAPH_LOCAL_DEFAULT);

        try {
            Constructor<? extends MainGraph> constructor;

            String subgraphsFile = getMainGraphSubgraphsPath();

            if (useLocalGraph) {
                if (subgraphsFile != "None") {
                    LOG.info("Creating disconnected graph");
                    constructor = mainGraphClass.getConstructor(java.nio.file.Path.class, java.nio.file.Path.class, boolean.class, boolean.class);
                    return constructor.newInstance(Paths.get(getMainGraphPath()), Paths.get(subgraphsFile), isGraphEdgeLabelled, isGraphMulti);
                } else {
                    constructor = mainGraphClass.getConstructor(java.nio.file.Path.class, boolean.class, boolean.class);
                    return constructor.newInstance(Paths.get(getMainGraphPath()), isGraphEdgeLabelled, isGraphMulti);
                }
            } else {
                if (subgraphsFile != "None") {
                    LOG.info("Creating disconnected graph");
                    constructor = mainGraphClass.getConstructor(Path.class, Path.class, boolean.class, boolean.class);
                    return constructor.newInstance(new Path(getMainGraphPath()), new Path(subgraphsFile), isGraphEdgeLabelled, isGraphMulti);
                } else {
                    constructor = mainGraphClass.getConstructor(Path.class, boolean.class, boolean.class);
                    return constructor.newInstance(new Path(getMainGraphPath()), isGraphEdgeLabelled, isGraphMulti);
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not load main graph", e);
        }
    }

    public boolean isOutputActive() {
        return getBoolean(CONF_OUTPUT_ACTIVE, CONF_OUTPUT_ACTIVE_DEFAULT);
    }

    public int getODAGNumAggregators() {
        return odagNumAggregators;
    }

    public String getOdagFlushMethod() {
       return getString(CONF_ODAG_FLUSH_METHOD, CONF_ODAG_FLUSH_METHOD_DEFAULT);
    }

    public int getMaxEnumerationsPerMicroStep() {
        return 10000000;
    }

    public boolean is2LevelAggregationEnabled() {
        return is2LevelAggregationEnabled;
    }

    public boolean isForceGC() {
        return forceGC;
    }

    public <M extends Writable> M createCommunicationStrategyMessage() {
        return communicationStrategyFactory.createMessage();
    }

    public CommunicationStrategy<O> createCommunicationStrategy(Configuration<O> configuration,
            ExecutionEngine<O> executionEngine, WorkerContext workerContext) {
        CommunicationStrategy<O> commStrategy = communicationStrategyFactory.createCommunicationStrategy();

        commStrategy.setConfiguration(configuration);
        commStrategy.setExecutionEngine(executionEngine);
        commStrategy.setWorkerContext(workerContext);

        return commStrategy;
    }

    public Set<String> getRegisteredAggregations() {
        return Collections.unmodifiableSet(aggregationsMetadata.keySet());
    }

    public Map<String, AggregationStorageMetadata> getAggregationsMetadata() {
        return Collections.unmodifiableMap(aggregationsMetadata);
    }

    public void setAggregationsMetadata(Map<String,AggregationStorageMetadata> aggregationsMetadata) {
       this.aggregationsMetadata = aggregationsMetadata;
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction, EndAggregationFunction<K, V> endAggregationFunction, int numSplits) {
        if (aggregationsMetadata.containsKey(name)) {
            return;
        }

        AggregationStorageMetadata<K, V> aggregationMetadata =
                new AggregationStorageMetadata<>(aggStorageClass,
                      keyClass, valueClass, persistent, reductionFunction, endAggregationFunction, numSplits);

        aggregationsMetadata.put(name, aggregationMetadata);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction) {
    	registerAggregation(name, getAggregationStorageClass(), keyClass, valueClass, persistent, reductionFunction, null, defaultAggregatorSplits);
    }
    
    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<? extends AggregationStorage> aggStorageClass, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction) {
    	registerAggregation(name, aggStorageClass, keyClass, valueClass, persistent, reductionFunction, null, defaultAggregatorSplits);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction, EndAggregationFunction<K, V> endAggregationFunction) {
    	registerAggregation(name, getAggregationStorageClass(), keyClass, valueClass, persistent, reductionFunction, endAggregationFunction, defaultAggregatorSplits);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<? extends AggregationStorage> aggStorageClass, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction, EndAggregationFunction<K, V> endAggregationFunction) {
    	registerAggregation(name, aggStorageClass, keyClass, valueClass, persistent, reductionFunction, endAggregationFunction, defaultAggregatorSplits);
    }

    public <K extends Writable, V extends Writable> AggregationStorageMetadata<K, V> getAggregationMetadata(String name) {
        return (AggregationStorageMetadata<K, V>) aggregationsMetadata.get(name);
    }

    public String getAggregationSplitName(String name, int splitId) {
        return name + "_" + splitId;
    }

    public <O extends Embedding> Computation<O> createComputation() {
        return ReflectionUtils.newInstance(computationClass);
    }
    
    public <K extends Writable, V extends Writable> AggregationStorage<K,V> createAggregationStorage(String name) {
        return ReflectionUtils.newInstance (getAggregationMetadata(name).getAggregationStorageClass());
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public MasterComputation createMasterComputation() {
        return ReflectionUtils.newInstance(masterComputationClass);
    }

    public boolean isGraphEdgeLabelled() {
        return isGraphEdgeLabelled;
    }

    public boolean isGraphMulti() {
        return isGraphMulti;
    }

    public Class<? extends Computation> getComputationClass() {
        return computationClass;
    }
    
    public Class<? extends AggregationStorage> getAggregationStorageClass() {
        return (Class<? extends AggregationStorage>) getClass(CONF_AGGREGATION_STORAGE_CLASS, CONF_AGGREGATION_STORAGE_CLASS_DEFAULT);
    }

    public void setMasterComputationClass(Class<? extends MasterComputation> masterComputationClass) {
       this.masterComputationClass = masterComputationClass;
    }

    public void setComputationClass(Class<? extends Computation> computationClass) {
       this.computationClass = computationClass;
    }

    public boolean isAggregationIncremental() {
       return getBoolean (CONF_INCREMENTAL_AGGREGATION, CONF_INCREMENTAL_AGGREGATION_DEFAULT);
    }

    public int getMaxOdags() {
       return getInteger (CONF_COMM_STRATEGY_ODAGMP_MAX, CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT);
    }

    public String getCommStrategy() {
       return getString (CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT);
    }

    //***** QFrag methods

    public long getNumberVertices() {
        return getLong(CONF_MAINGRAPH_VERTICES,-1L);
    }

    public long getNumberEdges(){
        return getLong(CONF_MAINGRAPH_EDGES,-1L);
    }

    public long getNumberLabels(){
        return getLong(CONF_MAINGRAPH_LABELS,-1L);
    }

    public boolean isFloatEdge() {
        return isFloatEdge;
    }

    public boolean isBinaryInputFile() {
        return isBinary;
    }

    public int getNumberEdgesLabels() {
        return getInteger(CONF_MAINGRAPH_NUM_EDGE_LABELS,-1);
    }

    public Double getDouble(String key, Double defaultValue) {
        return giraphConfiguration.getDouble(key, defaultValue);
    }

    //***** QFrag methods
}

