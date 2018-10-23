package io.arabesque.utils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import javafx.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class AwsS3Utils {
    AmazonS3 s3Client;

    public AwsS3Utils() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

    private static Pair<String, String> getPathInfo(String path) {
        path = path.replace("s3://","");
        String[] pathInfo = path.split("/");
        String bucketName = pathInfo[0];
        String key = String.join("/", Arrays.copyOfRange(pathInfo,1,pathInfo.length));
        return new Pair<String, String>(bucketName, key);
    }

    public InputStream readFromPath(String s3Path) {
        Pair<String, String> pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo.getKey();
        String key = pathInfo.getValue();
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        return fullObject.getObjectContent();
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

    public static void main(String[] args) throws IOException{
        AwsS3Utils s3Client = new AwsS3Utils();
        InputStream s3Stream = s3Client.readFromPath("s3://qfrag/graphs/citeseer.graph");
        Path hdfsPath = new Path("hdfs://localhost:8020/input/citeseer.graph");
        FileSystem fs = hdfsPath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        InputStream is = fs.open(hdfsPath);
        System.out.println(s3Stream.equals(is));
        System.out.println(IOUtils.contentEquals(is,s3Stream));
    }
}
