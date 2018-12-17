package io.arabesque.utils;

import io.arabesque.conf.SparkConfiguration;

import java.io.*;
import java.util.List;

public class AwsS3Partitioner extends MainGraphPartitioner {
    AwsS3Utils s3Object;

    public AwsS3Partitioner() {
        super();
    }

    public AwsS3Partitioner(SparkConfiguration conf) {
        super(conf);
        s3Object = new AwsS3Utils();
        clearPartitionDir();
    }

    @Override
    protected void initializeFS() {
        s3Object = new AwsS3Utils();
    }

    @Override
    protected InputStream readFile(String path) {
        return s3Object.readFromPath(path);
    }

    @Override
    protected void copyFileToStore(String s3Path, String path) throws IOException{
        s3Object.uploadFile(path, s3Path);
    }

    @Override
    protected void copyPartitionToDataStore(int fileIdx) {
        try {
            String path = dataPartitionDir + fileIdx + ".txt";
            String s3Path = partitionedPath + fileIdx;
            copyFileToStore(s3Path, path);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void clearPartitionDir() {
        List<String> paths = s3Object.listFiles(partitionedPath);
        for(String path: paths) {
            s3Object.deleteFile(path);
        }
    }

}
