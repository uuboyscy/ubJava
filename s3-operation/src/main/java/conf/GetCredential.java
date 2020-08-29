package conf;

import com.amazonaws.regions.Regions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.util.StringUtils;

import java.util.List;

public class GetCredential {

    public static String access = "access"; //ex: AKIA3PGEPUAAAAAABBBB
    public static String secret = "secret"; //ex: W3pIE45y64DAjeRXyBUQAAAAAAAAAABBBBBBBBBB
    public static Regions regions = Regions.AP_NORTHEAST_1;

    // Create connection
    AWSCredentials creds = new BasicAWSCredentials(access, secret);
    public AmazonS3 conn = new AmazonS3Client(creds);

    public static void main(String[] args){

        // Create connection
        AmazonS3 conn = (new GetCredential()).conn;

        List<Bucket> buckets = conn.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }
    }
}
