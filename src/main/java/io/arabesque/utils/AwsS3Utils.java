package io.arabesque.utils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import javafx.util.Pair;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class AwsS3Utils implements Serializable {
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
            .build();;

    public AwsS3Utils() {
        /*
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        */
    }

    private static Pair<String, String> getPathInfo(String path) {
        path = path.replace("s3://","");
        String[] pathInfo = path.split("/");
        String bucketName = pathInfo[0];
        String key = String.join("/", Arrays.copyOfRange(pathInfo,1,pathInfo.length));
        return new Pair<>(bucketName, key);
    }

    public InputStream readFromPath(String s3Path) {
        Pair<String, String> pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo.getKey();
        String key = pathInfo.getValue();
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        return fullObject.getObjectContent();
    }

    public void uploadFile(String path, String s3Path) {
        Pair<String, String> pathInfo = getPathInfo(s3Path);
        String bucketName = pathInfo.getKey();
        String key = pathInfo.getValue();
        PutObjectRequest request = new PutObjectRequest(bucketName, key, new File(path));
        s3Client.putObject(request);
    }

    private String buildPath(String bucketName, String key) {
        return "s3://" + bucketName + "/" + key;
    }

    public List<String> listFiles(String path) {
        Pair<String, String> pathInfo = getPathInfo(path);
        String bucketName = pathInfo.getKey();
        String key = pathInfo.getValue();
        if(path.substring(path.length()-1).equals("/")) { key = key + "/"; }
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key).withDelimiter("/");
        List<String> result = new LinkedList<>();
        ListObjectsV2Result keys;
        do {
            keys = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : keys.getObjectSummaries()) {
                if(objectSummary.getKey().equals(key)) { continue; }
                result.add(buildPath(bucketName, objectSummary.getKey()));
            }
            String token = keys.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (keys.isTruncated());
        return result;
    }

    public void deleteFile(String path) {
        Pair<String, String> pathInfo = getPathInfo(path);
        String bucketName = pathInfo.getKey();
        String key = pathInfo.getValue();
        s3Client.deleteObject(new DeleteObjectRequest(bucketName, key));
    }

    public static void main(String args[]) {
        AwsS3Utils s3Object = new AwsS3Utils();
        s3Object.listFiles("s3://qfrag/graphs/");
    }
}
