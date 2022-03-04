package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


class S3CompatApiTest {

    private static S3CompatStorageClient clientWithNoRegionSpecified;
    private static S3CompatStorageClient clientWithRegion1;
    private static S3CompatStorageClient clientWithRegion2;
    private static S3CompatStorageClient clientWithInvalidKeyId;
    private static S3CompatStorageClient clientWithInvalidSecret;
    private static S3CompatStorageClient clientWithNoCredentials;
    private static AWSCredentialsProvider credentialsProvider;
    private static final String endPoint = "s3.amazonaws.com";
    private static final String region1 = "us-east-1";
    private static final String region2 = "us-west-2";
    private static final String nullRegion = null;
    private static final String publicBucket = "scedc-pds";
    private static final String bucketAtRegion1 = "sfc-dev1";
    private static final String bucketAtRegion2 ="sfc-dev1-data";
    private static final String notExistingBucket = "sf-not-existing-bucket";
    private static final String prefixForBucketAtReg2 = "s3compatapi/tests";
    private static String prefixForPageListingAtReg2 = "s3compatapi/tests/data";
    private static final String localFilePath1 = "src/resources/test1.txt";
    private static final String localFilePath2 = "src/resources/test2.json";
    private static final String largeFile = "src/resources/test3.txt";
    private static String prefix = "";


    @BeforeAll
    public static void setupBase() {
        credentialsProvider = new EnvironmentVariableCredentialsProvider();
        BasicAWSCredentials wrongKeyId = new BasicAWSCredentials("invalid_access_key_id", credentialsProvider.getCredentials().getAWSSecretKey());
        BasicAWSCredentials wrongSecret = new BasicAWSCredentials(credentialsProvider.getCredentials().getAWSAccessKeyId(), "invalid_key_id");
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, region1, endPoint);
        clientWithRegion2 = new S3CompatStorageClient(credentialsProvider, region2, endPoint);
        clientWithNoRegionSpecified = new S3CompatStorageClient(credentialsProvider, nullRegion, endPoint);
        clientWithInvalidKeyId = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongKeyId), region2, endPoint);
        clientWithInvalidSecret = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongSecret), region2, endPoint);
        clientWithNoCredentials = new S3CompatStorageClient(null, region2, endPoint);
    }

    @AfterAll
    public static void tearDown() throws UnsupportedEncodingException {
        for (TestUtils.OPERATIONS op: TestUtils.OPERATIONS.values()) {
            prefix = prefixForBucketAtReg2 + "/" + op;
            List<S3ObjectSummary> result = clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix);
            clientWithRegion2.deleteObjects(bucketAtRegion2, fromS3ObjectSummaryToDeleteSpec(result));
            Assertions.assertTrue(clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).isEmpty());
        }
        System.out.println("ALL Tests Pass!");
    }

    private static List<DeleteRemoteObjectSpec> fromS3ObjectSummaryToDeleteSpec(List<S3ObjectSummary> summaries) {
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        summaries.forEach(s3Object -> toDelete.add(new DeleteRemoteObjectSpec(s3Object.getKey())));
        return toDelete;
    }

    @Test
    void getBucketLocation() throws Exception {
        startTest(TestUtils.OPERATIONS.GET_BUCKET_LOCATION);
        // positive tests:
        Assertions.assertEquals(region2, clientWithRegion2.getBucketLocation(bucketAtRegion2));
        Assertions.assertEquals(region2, clientWithNoRegionSpecified.getBucketLocation(bucketAtRegion2));
        Assertions.assertEquals(region2, clientWithRegion1.getBucketLocation(bucketAtRegion2));
        // AWS returns "US" for the standard region in us-east-1
        Assertions.assertEquals("US", clientWithRegion2.getBucketLocation(bucketAtRegion1));
        Assertions.assertEquals("US", clientWithRegion1.getBucketLocation(bucketAtRegion1));
        Assertions.assertEquals("US", clientWithNoRegionSpecified.getBucketLocation(bucketAtRegion1));
        Assertions.assertEquals(region2, clientWithNoCredentials.getBucketLocation(publicBucket));

        // negative tests
        TestUtils.functionCallThrowsException(() -> clientWithNoRegionSpecified.getBucketLocation(notExistingBucket),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */,
                "The specified bucket does not exist" /* expectedErrorMsg */);
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.getBucketLocation(bucketAtRegion2),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.getBucketLocation(bucketAtRegion2),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getBucketLocation(bucketAtRegion2),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }

    @Test
    void getObject() throws Exception {
        startTest(TestUtils.OPERATIONS.GET_OBJECT);

        String filePath = prefix + "/" + localFilePath1;
        File file = new File(localFilePath1);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        S3Object object = clientWithRegion2.getObject(bucketAtRegion2, filePath);
        Assertions.assertEquals(file.length(), object.getObjectMetadata().getContentLength());
        Assertions.assertEquals(bucketAtRegion2, object.getBucketName());
        Assertions.assertEquals(filePath, object.getKey());

        // Negative test: get a file does not exist
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.getObject(bucketAtRegion2, "notExisting" + filePath),
                404 /* expectedStatusCode */,
                "NoSuchKey" /* expectedErrorCode */,
                "The specified key does not exist." /* expectedErrorMsg */);

        // Negative test: get a file at a wrong region
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObject(bucketAtRegion2, filePath),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */,
                "The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-west-2'" /* expectedRegionFromExceptionMsg */);

        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getBucketLocation(bucketAtRegion2),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }

    @Test
    void getObjectMetadata() throws Exception {
        startTest(TestUtils.OPERATIONS.GET_OBJECT_METADATA);

        File file = new File(localFilePath1);
        String filePath = prefix + "/" + localFilePath1;
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        RemoteObjectMetadata mt = clientWithRegion2.getObjectMetadata(bucketAtRegion2, filePath);
        Assertions.assertEquals(file.length(), mt.getObjectContentLength());

        // Negative test: provide wrong version id
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.getObjectMetadata(bucketAtRegion2, filePath, "NonExistingVersion"),
                400 /* expectedStatusCode */,
                "400 Bad Request" /* expectedErrorCode */,
                "Bad Request" /* expectedRegionFromExceptionMsg */);
        // Negative test: get metadata for not existing file
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.getObjectMetadata(bucketAtRegion2, "not-existing" + filePath),
                404 /* expectedStatusCode */,
                "404 Not Found" /* expectedErrorCode */,
                "Not Found" /* expectedRegionFromExceptionMsg */);
    }

    @Test
    void putObject() throws Exception {
        startTest(TestUtils.OPERATIONS.PUT_OBJECT);
        // Positive test: put a file successfully
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        // Positive test: put a file with user metadata
        testPutObjectWithUserMetadata();
        // Positive test: put a file with up to size of 5GB
        testPutLargeObjectUpTo5GB();

        // Negative test: put object on a wrong region bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.putObject(bucketAtRegion2, prefix, localFilePath1),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */ ,
                "The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-west-2'" /* expectedErrorMsg */);
        // Negative test: put object on a not existing bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.putObject(notExistingBucket, prefix, localFilePath1),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */ ,
                "The specified bucket does not exist" /* expectedRegionFromExceptionMsg */);
        // Negative test: put object providing invalid access key id
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.putObject(bucketAtRegion2, prefix, localFilePath1),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);

        // Negative test: put object providing invalid secret key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.putObject(bucketAtRegion2, prefix, localFilePath1),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
    }

    private void testPutObjectWithUserMetadata() throws IOException {

        Map<String, String> addiontalMetadata = new TreeMap<>();
        addiontalMetadata.putIfAbsent("user", "sf");
        File file = new File(localFilePath2);
        String filePath = prefix + "/" + localFilePath2;
        WriteObjectSpec writeObjectSpec = new WriteObjectSpec(
                bucketAtRegion2 /* bucketName*/,
                filePath /* filePath */,
                () -> new FileInputStream(file) /* contentsSupplier */,
                file.length() /* contentLength */,
                false /* bucketOwnerFullControl */,
                null /* clientTimeoutInMs */,
                addiontalMetadata);
        clientWithRegion2.putObject(writeObjectSpec);
        // verify tha the object is successfully put
        RemoteObjectMetadata metadata = clientWithRegion2.getObjectMetadata(bucketAtRegion2, filePath);
        Assertions.assertNotNull(metadata.getObjectUserMetadata());
        Assertions.assertEquals(addiontalMetadata.get("user"), metadata.getObjectUserMetadata().get("user"));
        Assertions.assertEquals(file.length(), metadata.getObjectContentLength());

    }

    private void testPutLargeObjectUpTo5GB() throws IOException {
        long size_5GB = 5368709120L;
        File file = new File(largeFile);
        file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(size_5GB);
        raf.close();

        clientWithRegion2.putObject(bucketAtRegion2, prefix, S3CompatApiTest.largeFile);
        // verify that the object is successfully put
        String filePath = prefix + "/" + largeFile;
        RemoteObjectMetadata metadata = clientWithRegion2.getObjectMetadata(bucketAtRegion2, filePath);
        Assertions.assertEquals(size_5GB, metadata.getObjectContentLength());
        file.delete();
    }

    @Test
    void listObjects() throws IOException {
        startTest(TestUtils.OPERATIONS.LIST_OBJECTS);

        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath2);
        Assertions.assertEquals(2, clientWithRegion2.listObjects(bucketAtRegion2, prefix).size());
        // page listing, should have more than 1000 files on the location to trigger page listing
        Assertions.assertEquals(4000, clientWithRegion2.listObjects(bucketAtRegion2, prefixForPageListingAtReg2).size());
    }


    @Test
    void listObjectsV2() throws IOException {
        startTest(TestUtils.OPERATIONS.LIST_OBJECTS_V2);

        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath2);
        Assertions.assertEquals(clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).size(), 2);
        testPageListing();
    }

    void testPageListing() throws UnsupportedEncodingException {
        // page listing, should have more than 1000 files on the location to trigger page listing
        Assertions.assertEquals(4000, clientWithRegion2.listObjectsV2(bucketAtRegion2, prefixForPageListingAtReg2).size());
    }

    @Test
    void listVersions() throws IOException {
        startTest(TestUtils.OPERATIONS.LIST_VERSIONS);

        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath2);
        List<S3VersionSummary> summaries1 =  clientWithRegion2.listVersions(bucketAtRegion2, prefix, true);
        List<S3VersionSummary> summaries2 = clientWithRegion2.listVersions(bucketAtRegion2, prefix, false);
        for (int i = 0; i < summaries1.size(); i++) {
            S3VersionSummary v1 = summaries1.get(i);
            S3VersionSummary v2 = summaries2.get(i);
            Assertions.assertEquals(v1.getKey(), v2.getKey());
            Assertions.assertEquals(v1.getVersionId(), v2.getVersionId());
            Assertions.assertEquals(v1.getETag(), v2.getETag());
            Assertions.assertEquals(v1.getSize(), v2.getSize());
        }
    }

    @Test
    void deleteObject() throws IOException {
        startTest(TestUtils.OPERATIONS.DELETE_OBJECT);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        Assertions.assertEquals(1, clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).size());
        clientWithRegion2.deleteObject(bucketAtRegion2, prefix + "/" + localFilePath1);
        Assertions.assertTrue(clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).isEmpty());
    }

    @Test
    void deleteObjects() throws Exception {
        startTest(TestUtils.OPERATIONS.DELETE_OBJECTS);
        // Put files into the location for testing delete
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath2);
        Assertions.assertEquals(2, clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).size());

        DeleteRemoteObjectSpec objectWithWrongVersion = new DeleteRemoteObjectSpec(prefix + "/" + localFilePath1, "versionId");
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        toDelete.add(objectWithWrongVersion);
        // Delete fails as object is provided with a wrong version id, will receive MultiObjectDeleteException
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.deleteObjects(bucketAtRegion2, toDelete),
                200 /* expectedStatusCode */,
                null /* expectedErrorCode */,
                null /* expectedRegionFromExceptionMsg */);
        DeleteRemoteObjectSpec spec1 = new DeleteRemoteObjectSpec(prefix + "/" + localFilePath1);
        DeleteRemoteObjectSpec spec2 = new DeleteRemoteObjectSpec(prefix + "/"  + localFilePath2);
        // TODO: should add one more case: provide a version id for the object to delete
        toDelete.remove(0);// remove the one with wrong version id
        toDelete.addAll(Arrays.asList(spec1, spec2));
        Assertions.assertEquals(toDelete.size(), clientWithRegion2.deleteObjects(bucketAtRegion2, toDelete));

        // Listing should return 0 at the prefix since files have been deleted.
        Assertions.assertEquals(0, clientWithRegion2.listObjectsV2(bucketAtRegion2, prefix).size());
    }

    @Test
    void copyObject() throws IOException {
        startTest(TestUtils.OPERATIONS.COPY_OBJECT);
        // put a file at the location for copy
        clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);

        String sourceFileName = prefix + "/" + localFilePath1;
        String dstFileName = "dst_" + sourceFileName;

        clientWithRegion2.copyObject(bucketAtRegion2, sourceFileName, null, bucketAtRegion2, dstFileName );
        RemoteObjectMetadata dst = clientWithRegion2.getObjectMetadata(bucketAtRegion2, dstFileName);
        RemoteObjectMetadata source = clientWithRegion2.getObjectMetadata(bucketAtRegion2, sourceFileName);
        Assertions.assertEquals(source.getObjectContentLength(), dst.getObjectContentLength());
        Assertions.assertEquals(source.getObjectETag(), dst.getObjectETag());

    }

    @Test
    void setRegion() {
        startTest(TestUtils.OPERATIONS.SET_REGION);

        S3CompatStorageClient temClient = new S3CompatStorageClient(credentialsProvider, region1, endPoint);
        Assertions.assertEquals(region1, temClient.getRegionName());
        temClient.setRegion(Region.getRegion(Regions.EU_WEST_1));
        Assertions.assertEquals(Regions.EU_WEST_1.getName(), temClient.getRegionName());
    }

    @Test
    void generatePresignedUrl() {
        startTest(TestUtils.OPERATIONS.GENERATE_PRESIGNED_URL);

        String filePath = prefix + "/" + localFilePath1;
        try {
            clientWithRegion2.putObject(bucketAtRegion2, prefix, localFilePath1);
            // Test valid presigned url
            String validPresignedUrl1 = clientWithRegion2.generatePresignedUrl(bucketAtRegion2, filePath);
            URL presignedUrl1 = new URL(validPresignedUrl1);
            HttpURLConnection c = (HttpURLConnection) presignedUrl1.openConnection();
            Assertions.assertEquals(200, c.getResponseCode());

        } catch (IOException e) {
            Assertions.fail("Unexpected failure: " + e);
        }

        // Expired presigned url
        testForbiddenPresignedUrl(clientWithRegion2, bucketAtRegion2, filePath, 0 /* expiryTime */);
        // Url presigned with invalid key id
        testForbiddenPresignedUrl(clientWithInvalidKeyId, bucketAtRegion2, filePath, null /* expiryTime */);
        // Url presigned with invalid secret key
        testForbiddenPresignedUrl(clientWithInvalidSecret, bucketAtRegion2, filePath, null /* expiryTime */);

    }

    private void testForbiddenPresignedUrl(S3CompatStorageClient client, String bucketName, String filePath, Integer expiryTime) {
        String forbiddenUrlStr;
        try {
            if (expiryTime == null) {
                forbiddenUrlStr = client.generatePresignedUrl(bucketName, filePath);
            } else {
                forbiddenUrlStr = client.generatePresignedUrl(bucketName, filePath, expiryTime, null /* contentType*/);
            }
            URL expiredUrl = new URL(forbiddenUrlStr);
            HttpURLConnection c = (HttpURLConnection) expiredUrl.openConnection();
            Assertions.assertEquals(403, c.getResponseCode());
            Assertions.assertEquals("Forbidden", c.getResponseMessage());
        } catch (IOException e) {
            Assertions.fail("Unexpected failure: " + e);
        }
    }

    private void startTest(TestUtils.OPERATIONS operation) {
        prefix = prefixForBucketAtReg2 + "/" + operation;
    }
}
