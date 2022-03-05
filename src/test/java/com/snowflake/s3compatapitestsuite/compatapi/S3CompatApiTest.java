/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3VersionSummary;
import org.junit.jupiter.api.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
/**
 * Test s3compat api calls needed for Snowflake.
 * This test will require filling in some static values for testing purpose.
 */
class S3CompatApiTest {
    /** A client created without providing specific region. */
    private static StorageClient clientWithNoRegionSpecified;
    /** A client created with region {@value region1} provided. */
    private static StorageClient clientWithRegion1;
    /** A client created with region {@value region2} provided. */
    private static StorageClient clientWithRegion2;
    /** A client created with an invalid access key id. */
    private static StorageClient clientWithInvalidKeyId;
    /** A client created with an invalid secret key id. */
    private static StorageClient clientWithInvalidSecret;
    /** A client created without providing any credentials. */
    private static StorageClient clientWithNoCredentials;
    /** AWS credentials. The credentials should be set following aws guide here
     * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
     * */
    private static AWSCredentialsProvider credentialsProvider;
    /** A non-existing bucket. */
    private static final String notExistingBucket = "sf-non-existing-bucket";
    /** A prefix under region {@value  bucketAtRegion1} */
    private static final String prefixForBucketAtReg1 = "s3compatapi/tests";
    /** A local file for testing.*/
    private static final String localFilePath1 = "src/resources/test1.txt";
    /** A second local file for testing. */
    private static final String localFilePath2 = "src/resources/test2.json";
    /** A third file name for testing. */
    private static final String largeFile = "src/resources/largeFile.txt";
    /** A prefix will be updated for each test. */
    private static String prefix = "";
    /**
     * Please fill in below values for testing.
     */
    /** An endpoint the client will make requests to. eg: "s3.amazonaws.com" */
    private static final String endPoint = "";
    /** A region that a bucket locates, and it should correspond to s3 region. eg: "us-east-1" */
    private static final String region1 = "";
    /** Another region that is different from region {@value  region1}. eg: "us-west-2" */
    private static final String region2 = "";
    /** A bucket locates at region {@value  region1}. */
    private static final String bucketAtRegion1 = "";
    /** A bucket locates at region {@value  region2}. The bucket corresponds to S3 bucket. */
    private static final String bucketAtRegion2 = "";
    /** A bucket locates at region {@value  region1} but the provided credentials do not have access to. */
    private static final String bucketExistsButNotAccessible = "";
    /** A public bucket, which means without providing credentials, the client can have access to it. */
    private static final String publicBucket = "";
    /** A prefix under bucket {@value  bucketAtRegion1} which should contain over 1000 files, this is for page listing tests. */
    private static final String prefixForPageListingAtReg1 = "";
    /** The total number of files under prefix {@value  prefixForPageListingAtReg1}, it must > 1000 */
    private static final int pageListingTotalSize = 0;
    @BeforeAll
    public static void setupBase() {
        credentialsProvider = new EnvironmentVariableCredentialsProvider();
        BasicAWSCredentials wrongKeyId = new BasicAWSCredentials("invalid_access_key_id", credentialsProvider.getCredentials().getAWSSecretKey());
        BasicAWSCredentials wrongSecret = new BasicAWSCredentials(credentialsProvider.getCredentials().getAWSAccessKeyId(), "invalid_key_id");
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, region1, endPoint);
        clientWithRegion2 = new S3CompatStorageClient(credentialsProvider, region2, endPoint);
        clientWithNoRegionSpecified = new S3CompatStorageClient(credentialsProvider, null, endPoint);
        clientWithInvalidKeyId = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongKeyId), region1, endPoint);
        clientWithInvalidSecret = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongSecret), region1, endPoint);
        clientWithNoCredentials = new S3CompatStorageClient(null, region1, endPoint);
    }
    @AfterAll
    public static void tearDown() throws UnsupportedEncodingException {
        // Delete files that we uploaded to each prefix during each test.
        for (TestUtils.OPERATIONS op: TestUtils.OPERATIONS.values()) {
            updatePrefixForTestCase(op);
            clientWithRegion1.deleteObjects(bucketAtRegion1, prefix);
        }
        System.out.println("ALL Tests Pass!");
    }
    @Test
    void setRegion() {
        updatePrefixForTestCase(TestUtils.OPERATIONS.SET_REGION);
        S3CompatStorageClient temClient = new S3CompatStorageClient(credentialsProvider, region2, endPoint);
        Assertions.assertEquals(region2, temClient.getRegionName());
        temClient.setRegion(Regions.EU_WEST_1.getName());
        Assertions.assertEquals(Regions.EU_WEST_1.getName(), temClient.getRegionName());
    }
    @Test
    void getBucketLocation() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_BUCKET_LOCATION);
        // positive tests:
        Assertions.assertEquals(region1, clientWithRegion1.getBucketLocation(bucketAtRegion1));
        Assertions.assertEquals(region1, clientWithNoRegionSpecified.getBucketLocation(bucketAtRegion1));
        Assertions.assertEquals(region1, clientWithRegion2.getBucketLocation(bucketAtRegion1));
        // AWS returns "US" for the standard region in us-east-1
        Assertions.assertEquals("US", clientWithRegion1.getBucketLocation(bucketAtRegion2));
        Assertions.assertEquals("US", clientWithRegion2.getBucketLocation(bucketAtRegion2));
        Assertions.assertEquals("US", clientWithNoRegionSpecified.getBucketLocation(bucketAtRegion2));
        // Test for public bucket
        Assertions.assertEquals(region1, clientWithNoCredentials.getBucketLocation(publicBucket));
        // Negative test: bucket does not exist
        TestUtils.functionCallThrowsException(() -> clientWithNoRegionSpecified.getBucketLocation(notExistingBucket),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */,
                "The specified bucket does not exist" /* expectedErrorMsg */);
        // Negative test: invalid access key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.getBucketLocation(bucketAtRegion1),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);
        // Negative test: invalid secret key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.getBucketLocation(bucketAtRegion1),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
        // Negative test: no credentials provided, access denied.
        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getBucketLocation(bucketAtRegion1),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Negative test: get bucket location for an existing but not accessible bucket name.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getBucketLocation(bucketExistsButNotAccessible),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Set the region back for later testing
        clientWithRegion1.setRegion(region1);
        clientWithRegion2.setRegion(region2);
        clientWithNoCredentials.setRegion(region1);
        clientWithInvalidKeyId.setRegion(region1);
        clientWithInvalidSecret.setRegion(region1);
        clientWithNoRegionSpecified = new S3CompatStorageClient(credentialsProvider, null, endPoint);
    }
    @Test
    void putObject() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.PUT_OBJECT);
        // Positive test: put a file successfully
        clientWithRegion1.putObject(bucketAtRegion1, prefix, localFilePath1);
        // Positive test: put a file with user metadata
        testPutObjectWithUserMetadata();
        // Positive test: put a file with up to size of 5GB
        testPutLargeObjectUpTo5GB();
        // Negative test: put object on a wrong region bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.putObject(bucketAtRegion1, prefix, localFilePath1),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */ ,
                "The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-west-2'" /* expectedErrorMsg */);
        // Negative test: put object on a non-existing bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.putObject(notExistingBucket, prefix, localFilePath1),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */ ,
                "The specified bucket does not exist" /* expectedRegionFromExceptionMsg */);
        // Negative test: put object by providing invalid access key id
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.putObject(bucketAtRegion1, prefix, localFilePath1),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);
        // Negative test: put object by providing invalid secret key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.putObject(bucketAtRegion1, prefix, localFilePath1),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
        // Negative test: put object to an existing but not accessible bucket.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.putObject(bucketExistsButNotAccessible, prefix, localFilePath1),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }
    private void testPutObjectWithUserMetadata() throws IOException {
        Map<String, String> addiontalMetadata = new TreeMap<>();
        addiontalMetadata.putIfAbsent("user", "sf");
        File file = new File(localFilePath2);
        String filePath = prefix + "/" + localFilePath2;
        WriteObjectSpec writeObjectSpec = new WriteObjectSpec(
                bucketAtRegion1 /* bucketName*/,
                filePath /* filePath */,
                () -> new FileInputStream(file) /* contentsSupplier */,
                file.length() /* contentLength */,
                null /* clientTimeoutInMs */,
                addiontalMetadata);
        clientWithRegion1.putObject(writeObjectSpec);
        // verify tha the object is successfully put
        RemoteObjectMetadata metadata = getObjectMetadata(clientWithRegion1, bucketAtRegion1, filePath);
        Assertions.assertNotNull(metadata.getObjectUserMetadata());
        Assertions.assertEquals(addiontalMetadata.get("user"), metadata.getObjectUserMetadata().get("user"));
        Assertions.assertEquals(file.length(), metadata.getObjectContentLength());
    }
    private void testPutLargeObjectUpTo5GB() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.PUT_OBJECT);
        long size_5GB = 5368709120L;
        File file = new File(largeFile);
        file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(size_5GB);
        raf.close();
        // Put the object, should success without error.
        clientWithRegion1.putObject(bucketAtRegion1, prefix, largeFile);
        // Verify that the object is successfully put
        String filePath = prefix + "/" + largeFile;
        RemoteObjectMetadata metadata = getObjectMetadata(clientWithRegion1, bucketAtRegion1, filePath);
        Assertions.assertEquals(size_5GB, metadata.getObjectContentLength());
        file.delete();
    }
    @Test
    void getObject() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_OBJECT);
        // Upload a file to ensure we can get it later
        uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, localFilePath1);
        String filePath = prefix + "/" + localFilePath1;
        File file = new File(localFilePath1);
        S3Object object = clientWithRegion1.getObject(bucketAtRegion1, filePath);
        Assertions.assertEquals(file.length(), object.getObjectMetadata().getContentLength());
        Assertions.assertEquals(bucketAtRegion1, object.getBucketName());
        Assertions.assertEquals(filePath, object.getKey());
        // Negative test: get a file that does not exist
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObject(bucketAtRegion1, "notExisting" + filePath),
                404 /* expectedStatusCode */,
                "NoSuchKey" /* expectedErrorCode */,
                "The specified key does not exist." /* expectedErrorMsg */);
        // Negative test: get a file at a wrong region
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.getObject(bucketAtRegion1, filePath),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */,
                "The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-west-2'" /* expectedRegionFromExceptionMsg */);
        // Negative test: get a file without providing credential.
        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getObject(bucketAtRegion1, filePath),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Negative test: get a file an existing but not accessible bucket.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObject(bucketExistsButNotAccessible, filePath),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }
    @Test
    void getObjectMetadata() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_OBJECT_METADATA);
        uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, localFilePath1);
        File file = new File(localFilePath1);
        String filePath = prefix + "/" + localFilePath1;
        RemoteObjectMetadata mt = getObjectMetadata(clientWithRegion1, bucketAtRegion1, filePath);
        Assertions.assertEquals(file.length(), mt.getObjectContentLength());
        // Negative test: provide wrong version id
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObjectMetadata(bucketAtRegion1, filePath, "NonExistingVersion"),
                400 /* expectedStatusCode */,
                "400 Bad Request" /* expectedErrorCode */,
                "Bad Request" /* expectedRegionFromExceptionMsg */);
        // Negative test: get metadata for a non-existing file
        TestUtils.functionCallThrowsException(() -> getObjectMetadata(clientWithRegion1, bucketAtRegion1, "not-existing" + filePath),
                404 /* expectedStatusCode */,
                "404 Not Found" /* expectedErrorCode */,
                "Not Found" /* expectedRegionFromExceptionMsg */);
        // TODO: add a test for getting with version id
    }
    private RemoteObjectMetadata getObjectMetadata(StorageClient client, String bucketName, String key) {
        return client.getObjectMetadata(bucketName, key, null /* versionId*/);
    }
    @Test
    void listObjects() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_OBJECTS);
        // Upload files to make sure later listing as expected.
        List<String> filesToUpload = Arrays.asList(localFilePath1, localFilePath2);
        for (String file: filesToUpload) {
            uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, file);
        }
        Assertions.assertEquals(filesToUpload.size(), clientWithRegion1.listObjects(bucketAtRegion1, prefix).size());
        // page listing, should have more than 1000 files on the location to trigger page listing
        Assertions.assertEquals(pageListingTotalSize, clientWithRegion1.listObjects(bucketAtRegion1, prefixForPageListingAtReg1).size());
    }
    @Test
    void listObjectsV2() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_OBJECTS_V2);
        // Upload files to make sure listing as expected.
        List<String> filesToUpload = Arrays.asList(localFilePath1, localFilePath2);
        for (String file: filesToUpload) {
            uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, file);
        }
        Assertions.assertEquals(filesToUpload.size(), clientWithRegion1.listObjectsV2(bucketAtRegion1, prefix).size());
        testPageListing();
    }
    void testPageListing() {
        // page listing, should have more than 1000 files on the location to trigger page listing
        if (pageListingTotalSize <= 1000) {
            Assertions.fail("Expect to list over 1000 files.");
        }
        System.out.println(bucketAtRegion1 + "/" + prefixForPageListingAtReg1);
        Assertions.assertEquals(pageListingTotalSize, clientWithRegion1.listObjectsV2(bucketAtRegion1, prefixForPageListingAtReg1).size());
    }
    @Test
    void listVersions() {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_VERSIONS);
        // Upload files to make sure listing as expected.
        List<String> filesToUpload = Arrays.asList(localFilePath1, localFilePath2);
        for (String file: filesToUpload) {
            uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, file);
        }
        List<S3VersionSummary> summaries1 =  clientWithRegion1.listVersions(bucketAtRegion1, prefix, true /* useUrlEncoding */);
        List<S3VersionSummary> summaries2 = clientWithRegion1.listVersions(bucketAtRegion1, prefix, false /* useUrlEncoding */);
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
        updatePrefixForTestCase(TestUtils.OPERATIONS.DELETE_OBJECT);
        // Upload a file to make sure to delete the target.
        uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, localFilePath1);
        Assertions.assertEquals(1, clientWithRegion1.listObjectsV2(bucketAtRegion1, prefix).size());
        clientWithRegion1.deleteObject(bucketAtRegion1, prefix + "/" + localFilePath1);
        Assertions.assertTrue(clientWithRegion1.listObjectsV2(bucketAtRegion1, prefix).isEmpty());
    }
    @Test
    void deleteObjects() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.DELETE_OBJECTS);
        // Put files into the location for testing delete
        // Upload files to make sure listing as expected.
        List<String> filesToUpload = Arrays.asList(localFilePath1, localFilePath2);
        for (String file: filesToUpload) {
            uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, file);
        }
        // ensure files are at the location
        Assertions.assertEquals(filesToUpload.size(), clientWithRegion1.listObjectsV2(bucketAtRegion1, prefix).size());
        DeleteRemoteObjectSpec objectWithWrongVersion = new DeleteRemoteObjectSpec(prefix + "/" + localFilePath1, "versionId");
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        toDelete.add(objectWithWrongVersion);
        // Delete fails as object is provided with a wrong version id, will receive MultiObjectDeleteException
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.deleteObjects(bucketAtRegion1, toDelete),
                200 /* expectedStatusCode */,
                null /* expectedErrorCode */,
                null /* expectedRegionFromExceptionMsg */);
        // TODO: should add one more case: provide a version id for the object to delete
        toDelete.remove(0);// remove the one with wrong version id
        for (String file: filesToUpload) {
            toDelete.add(new DeleteRemoteObjectSpec(prefix + "/" + file));
        }
        Assertions.assertEquals(toDelete.size(), clientWithRegion1.deleteObjects(bucketAtRegion1, toDelete));
        // Listing should return 0 at the prefix since files have been deleted.
        Assertions.assertEquals(0, clientWithRegion1.listObjectsV2(bucketAtRegion1, prefix).size());
    }
    @Test
    void copyObject() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.COPY_OBJECT);
        // put a file at the location for copy
        uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, localFilePath1);
        String sourceFileName = prefix + "/" + localFilePath1;
        String dstFileName = "dst_" + sourceFileName;
        // TODO: should update a test to copy with version id
        clientWithRegion1.copyObject(bucketAtRegion1, sourceFileName, null, bucketAtRegion1, dstFileName );
        RemoteObjectMetadata dst = getObjectMetadata(clientWithRegion1, bucketAtRegion1, dstFileName);
        RemoteObjectMetadata source = getObjectMetadata(clientWithRegion1, bucketAtRegion1, sourceFileName);
        Assertions.assertEquals(source.getObjectContentLength(), dst.getObjectContentLength());
        Assertions.assertEquals(source.getObjectETag(), dst.getObjectETag());
    }
    @Test
    void generatePresignedUrl() {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GENERATE_PRESIGNED_URL);
        // Upload a file to ensure a file to get pre-signed url
        uploadAnObjectToTestingLocation(clientWithRegion1, bucketAtRegion1, localFilePath1);
        String filePath = prefix + "/" + localFilePath1;
        try {
            // Test valid presigned url
            String validPresignedUrl1 = generatePresignedUrl(clientWithRegion1, bucketAtRegion1, filePath);
            URL presignedUrl1 = new URL(validPresignedUrl1);
            HttpURLConnection c = (HttpURLConnection) presignedUrl1.openConnection();
            Assertions.assertEquals(200, c.getResponseCode());
        } catch (IOException e) {
            Assertions.fail("Unexpected failure: " + e);
        }
        // Expired presigned url
        testForbiddenPresignedUrl(clientWithRegion1, bucketAtRegion1, filePath, 0 /* expiryTime */);
        // Url presigned with invalid key id
        testForbiddenPresignedUrl(clientWithInvalidKeyId, bucketAtRegion1, filePath, null /* expiryTime */);
        // Url presigned with invalid secret key
        testForbiddenPresignedUrl(clientWithInvalidSecret, bucketAtRegion1, filePath, null /* expiryTime */);
    }
    private void testForbiddenPresignedUrl(StorageClient client, String bucketName, String filePath, Integer expiryTime) {
        String forbiddenUrlStr;
        try {
            if (expiryTime == null) {
                forbiddenUrlStr = generatePresignedUrl(client, bucketName, filePath);
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
    /**
     * Generate a pre-signed url using default expiry time, and without providing content type.
     */
    private String generatePresignedUrl(StorageClient client, String bucketName, String key) {
        return client.generatePresignedUrl(bucketName, key, -1 /* expiryTime */, null /* contentType */);
    }
    private static void updatePrefixForTestCase(TestUtils.OPERATIONS operation) {
        prefix = prefixForBucketAtReg1 + "/" + operation;
    }
    /**
     * Upload a file to a remote location with prefix {@value prefix} under a bucket, the prefix is updated for each test.
     */
    private void uploadAnObjectToTestingLocation(StorageClient client, String bucket, String fileName) {
        client.putObject(bucket, prefix, fileName);
    }
}
