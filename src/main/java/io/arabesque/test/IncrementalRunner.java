package io.arabesque.test;

import io.arabesque.conf.SparkConfiguration;
import io.arabesque.conf.YamlConfiguration;
import io.arabesque.graph.PartitionGraph;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.utils.MainGraphPartitioner;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class IncrementalRunner {
    public static void main(String[] args) {

        YamlConfiguration yamlConfig = new YamlConfiguration(args);
        yamlConfig.load();

        SparkConfiguration config = new SparkConfiguration(JavaConversions.mapAsScalaMap(yamlConfig.getProperties()));
        JavaSparkContext sc = new JavaSparkContext(config.sparkConf());
        config.setHadoopConfig (sc.hadoopConfiguration());
        String inputGraphPath = config.getString(config.SEARCH_MAINGRAPH_PATH,config.SEARCH_MAINGRAPH_PATH_DEFAULT) + "-1" ;

        MainGraphPartitioner partitioner = new MainGraphPartitioner();

        Thread thread = new Thread(partitioner);
        thread.start();
        try {
            thread.join();
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Vertex ID : partition");
        System.out.println(0 + " : " + partitioner.getIdxByVertex(0));
        System.out.println(661 + " : " + partitioner.getIdxByVertex(661));
        System.out.println(1986 + " : " + partitioner.getIdxByVertex(1986));
        System.out.println(3310 + " : " + partitioner.getIdxByVertex(3310));

        System.out.println("edge ID : partition");
        System.out.println(0 + " : " + partitioner.getIdxByEdge(0));
        System.out.println(614 + " : " + partitioner.getIdxByEdge(614));
        System.out.println(1277 + " : " + partitioner.getIdxByEdge(1277));
        System.out.println(9071 + " : " + partitioner.getIdxByEdge(9071));


        config.setPartitioner(partitioner);
        PartitionGraph dataGraph = new PartitionGraph(config);
        System.out.println(dataGraph.getNeighborhoodSizeWithLabel(3300,2));
    }
}
