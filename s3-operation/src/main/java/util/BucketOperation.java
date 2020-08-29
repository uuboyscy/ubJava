/**
 * Some operations to get buckets information or contents in certain bucket
 */

package util;

import conf.GetCredential;

//import com.amazonaws.regions.Regions;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BucketOperation {

    /** Get a list of bucket objects */
    public List<Bucket> getBucketList(){
        AmazonS3 conn = (new GetCredential()).conn;
        List<Bucket> buckets = conn.listBuckets();
        return buckets;
    }

    /** Get one bucket object */
    public Bucket getBucket(String bucketName){
        AmazonS3 conn = (new GetCredential()).conn;
        List<Bucket> buckets = conn.listBuckets();
        Bucket outputBucket = null;
        for (Bucket bucket : buckets){
            if (bucket.getName().equals(bucketName)){
                outputBucket = bucket;
                break;
            }
        }
        return outputBucket;
    }

    /** List all buckets */
    public void listAllBuckets(){
        AmazonS3 conn = (new GetCredential()).conn;
        List<Bucket> buckets = conn.listBuckets();
        for (Bucket bucket : buckets){
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }
    }

    /** Create new bucket */
    public void createBucket(String bucketName){
        AmazonS3 conn = (new GetCredential()).conn;
        Bucket bucket = conn.createBucket(bucketName);
    }

    /** Delete bucket */
    public void deleteBucket(String bucketName){
    }

    /** Get a list of content objects in certain bucket */
    public List<S3ObjectSummary> getBucketContents(String bucketName){
        List<S3ObjectSummary> contentsList = new ArrayList<S3ObjectSummary>();
        AmazonS3 conn = (new GetCredential()).conn;
        Bucket bucket = getBucket(bucketName);
        ObjectListing objects = conn.listObjects(bucket.getName());
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                contentsList.add(objectSummary);
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        return contentsList;
    }

    /** List all contents in certain bucket
     * come with size and modified date */
    public void listBucketContents(String bucketName){
        AmazonS3 conn = (new GetCredential()).conn;
        Bucket bucket = getBucket(bucketName);
        ObjectListing objects = conn.listObjects(bucket.getName());
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + "\t" +
                        objectSummary.getSize() + " Bytes" + "\t" +
                        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    /** Download one content object from certain bucket */
    public void downloadBucketContent(String bucketName, String contentName, String downloadPath){
        AmazonS3 conn = (new GetCredential()).conn;
        Bucket bucket = getBucket(bucketName);
        conn.getObject(
                new GetObjectRequest(bucket.getName(), contentName),
                new File(downloadPath)
        );
    }

    /** Delete one content object in certain bucket */
    public void deleteBucketContent(String bucketName, String contentName){
        AmazonS3 conn = (new GetCredential()).conn;
        conn.deleteObject(bucketName, contentName);
    }


    public static void main(String[] args){

        BucketOperation bopt = new BucketOperation();
        bopt.listBucketContents("some-bucket-name");

//        Bucket bucket = (new BucketOperation()).getBucket("some-bucket-name");
//        System.out.println(bucket.toString());

//        BucketOperation b = new BucketOperation();
//        b.listAllBuckets();

//        (new BucketOperation())
//                .downloadBucketContent("some-bucket-name", "test.seq", "/Users/uuboy.scy/testS3DownloadFile.seq");

    }

}
