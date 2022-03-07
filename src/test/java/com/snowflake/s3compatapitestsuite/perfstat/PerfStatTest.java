package com.snowflake.s3compatapitestsuite.perfstat;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.snowflake.s3compatapitestsuite.compatapi.S3CompatStorageClient;
import com.snowflake.s3compatapitestsuite.util.TestConstants;
import com.snowflake.s3compatapitestsuite.util.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;


public class PerfStatTest {
    /** A client created with region TestConstants.region1 provided. */
    private static S3CompatStorageClient clientWithRegion1;
    /** A prefix for running tests. */
    private static String prefix = TestConstants.PERFSTAT_PREFIX;
    private static String bucketName = TestConstants.BUCKET_AT_REGION_1;
    private static String filePath1 = prefix + "/" + TestConstants.LOCAL_FILE_PATH_1;
    private static AWSCredentialsProvider credentialsProvider = null;
    @BeforeAll
    public static void setup() {
        credentialsProvider = new EnvironmentVariableCredentialsProvider();
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, TestConstants.REGION_1, TestConstants.ENDPOINT);
        File file = new File(TestConstants.PERF_REPORT);
        // delete the previous report
        file.delete();
    }
    @AfterAll
    public static void tearDown() throws UnsupportedEncodingException {
        // Delete files that we uploaded to each prefix for testing.
        clientWithRegion1.deleteObjects(TestConstants.BUCKET_AT_REGION_1, prefix);
        System.out.println("PerfStat report generated!");
    }
    @Test
    public void collectPerfStat() throws IOException {
        simpleApiCalls();
        putObjectWithLargeSize();
        listLargeNumOfObjects();
        listLargeNumOfVersions();
        deleteLargeNumOfObjects();
    }
    private void simpleApiCalls() {
        // put a file in order for testing
        PutObjectResult putObjectResult1 = clientWithRegion1.putObject(bucketName, prefix, TestConstants.LOCAL_FILE_PATH_1);
        PutObjectResult putObjectResult2 = clientWithRegion1.putObject(bucketName, prefix, TestConstants.LOCAL_FILE_PATH_2);
        // getBucketLocation
        clientWithRegion1.getBucketLocation(bucketName);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // getObjectMetadata
        clientWithRegion1.getObjectMetadata(bucketName, filePath1, putObjectResult1.getVersionId());
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // getObject
        clientWithRegion1.getObject(bucketName, filePath1);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // putObject
        clientWithRegion1.putObject(bucketName, prefix, TestConstants.LOCAL_FILE_PATH_1);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // copyObjects
        clientWithRegion1.copyObject(bucketName, filePath1, putObjectResult1.getVersionId(), bucketName, "dst_" + filePath1);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // listObjects
        clientWithRegion1.listObjectsV2(bucketName, prefix, null /* maxKeys */);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // listVersions
        clientWithRegion1.listVersions(bucketName, filePath1, false);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // deleteObject
        clientWithRegion1.deleteObject(bucketName, filePath1);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
        // deleteObjects
        clientWithRegion1.deleteObjects(bucketName, prefix);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
    }
    private void putObjectWithLargeSize() {
        // pubObject -- size of 5GB
        File file = TestUtils.generateFileWithSize(TestConstants.LARGE_FILE_NAME, 5368709120L); // 5GB
        clientWithRegion1.putObject(bucketName, prefix, TestConstants.LARGE_FILE_NAME);
        file.delete();
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
    }
    private void listLargeNumOfObjects() {
        clientWithRegion1.listObjectsV2(bucketName, TestConstants.PREFIX_FOR_PAGE_LISTING_AT_REG_1, null /* maxKeys*/);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
    }
    private void listLargeNumOfVersions() {
        int numFiles = TestUtils.getRandomInt(550, 750);
        String testPrefix = prefix + "/versions";
        for (int i = 0; i < numFiles; i++) {
            clientWithRegion1.putObject(bucketName, testPrefix, TestConstants.LOCAL_FILE_PATH_1);
        }
        clientWithRegion1.listVersions(bucketName, testPrefix + "/" + TestConstants.LOCAL_FILE_PATH_1, false);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
    }
    @Test
    public void deleteLargeNumOfObjects() {
        int numFiles = TestUtils.getRandomInt(500, 750);
        String testPrefix = prefix + "/deleteFiles";
        String fileNamePrefix = "src/resources/";
        for (int i = 0; i < numFiles; i++) {
            String fileName = fileNamePrefix + "tempfile_" + i;
            File file = TestUtils.generateFileWithSize(fileName, 10);
            clientWithRegion1.putObject(bucketName, testPrefix, fileName);
            file.delete();
        }
        clientWithRegion1.deleteObjects(bucketName, testPrefix);
        clientWithRegion1.flushStat(TestConstants.PERF_REPORT);
    }
}
