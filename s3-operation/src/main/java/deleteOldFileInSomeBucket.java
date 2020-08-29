/**
 * Used to delete backup old files in some bucket
 * */

import util.BucketOperation;

import java.util.Date;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class deleteOldFileInSomeBucket {
    public static void main(String[] args){

        String bucketName = "some-bucket-name";
        Date currentDate = new Date();
        int totalReleasedSize = 0;
        int totalFileCount = 0;
        Double needToBeDeletedDays = 14.;

        // Get list of bucket content
        List<S3ObjectSummary> someBucketContents = (new BucketOperation()).getBucketContents(bucketName);
        for (S3ObjectSummary content : someBucketContents){
            String contentName = content.getKey();
            int contentSize = (int)(content.getSize()/1024/1024);
            Date contentModDate = content.getLastModified();
            int secondDelta = (int)((currentDate.getTime() - contentModDate.getTime())/1000/3600/24);

            if (secondDelta >= needToBeDeletedDays){
                System.out.println(contentName + "\t" + contentSize + " MB \t" + contentModDate.toString() + "\t" + secondDelta + " uploaded day(s) ago\t-> Deleted.");
                // Delete files
                (new BucketOperation()).deleteBucketContent(bucketName, contentName);
                totalFileCount++;
                totalReleasedSize += contentSize;
            }
        }
        System.out.println("\n" + totalReleasedSize + " MB released.");
        System.out.println(totalFileCount + " files deleted.");

    } // main
} // class
