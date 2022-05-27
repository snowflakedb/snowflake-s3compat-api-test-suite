/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.snowflake.s3compatapitestsuite.EnvConstants;
import com.snowflake.s3compatapitestsuite.util.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Test s3compat api calls needed for Snowflake.
 * This test will require filling in some static values for testing purpose.
 */
class S3CompatApiTest {
    /** A client created without providing specific region. */
    private static StorageClient clientWithNoRegionSpecified;
    /** A client created with region TestConstants.region1 provided. */
    private static S3CompatStorageClient clientWithRegion1;
    /** A client created with region TestConstants.region2 provided. */
    private static StorageClient clientWithRegion2;
    /** A client created with an invalid access key id. */
    private static StorageClient clientWithInvalidKeyId;
    /** A client created with an invalid secret key id. */
    private static StorageClient clientWithInvalidSecret;
    /** A client created without providing any credentials. */
    private static StorageClient clientWithNoCredentials;
    /** AWS credentials. */
    private static AWSCredentialsProvider credentialsProvider;
    /** A prefix will be updated for each test. */
    private static String prefix = "";
    @BeforeAll
    public static void setupBase() throws RuntimeException{
       // BasicConfigurator.configure();
        EnvConstants.setUpParameterValues();
        credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(EnvConstants.ACCESS_KEY, EnvConstants.SECRET_KEY));
        BasicAWSCredentials wrongKeyId = new BasicAWSCredentials("invalid_access_key_id", credentialsProvider.getCredentials().getAWSSecretKey());
        BasicAWSCredentials wrongSecret = new BasicAWSCredentials(credentialsProvider.getCredentials().getAWSAccessKeyId(), "invalid_key_id");
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, EnvConstants.REGION_1, EnvConstants.ENDPOINT);
        clientWithRegion2 = new S3CompatStorageClient(credentialsProvider, EnvConstants.REGION_2, EnvConstants.ENDPOINT);
        clientWithNoRegionSpecified = new S3CompatStorageClient(credentialsProvider, null, EnvConstants.ENDPOINT);
        clientWithInvalidKeyId = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongKeyId), EnvConstants.REGION_1, EnvConstants.ENDPOINT);
        clientWithInvalidSecret = new S3CompatStorageClient(new AWSStaticCredentialsProvider(wrongSecret), EnvConstants.REGION_1, EnvConstants.ENDPOINT);
        clientWithNoCredentials = new S3CompatStorageClient(null, EnvConstants.REGION_1, EnvConstants.ENDPOINT);
    }
    @AfterAll
    public static void tearDown() throws UnsupportedEncodingException {
        // Delete all versions of files that we uploaded to each prefix during each test.
        try {
            for (TestUtils.OPERATIONS op : TestUtils.OPERATIONS.values()) {
                updatePrefixForTestCase(op);
                // cleanup all versions
                List<DeleteRemoteObjectSpec> toDeleteList = new ArrayList<>();
                try {
                    for (S3VersionSummary v : clientWithRegion1.listVersions(EnvConstants.BUCKET_AT_REGION_1, prefix, true /* useEncodeUrl*/, null /* maxKey */)) {
                        toDeleteList.add(new DeleteRemoteObjectSpec(v.getKey(), v.getVersionId()));
                        if (toDeleteList.size() == 1000) {
                            clientWithRegion1.deleteObjects(EnvConstants.BUCKET_AT_REGION_1, toDeleteList);
                            toDeleteList.clear();
                        }
                    }
                    clientWithRegion1.deleteObjects(EnvConstants.BUCKET_AT_REGION_1, toDeleteList);
                } catch (Exception e) {
                    // in case if listVersions is not supported
                    clientWithRegion1.deleteObject(EnvConstants.BUCKET_AT_REGION_1, prefix);
                }
            }
        } catch (Exception e) {
            System.out.println("S3Compat API Test Cleanup fail. " + e);
            e.printStackTrace();
        }

    }
    @Test
    void getBucketLocation() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_BUCKET_LOCATION);
        // positive tests:
        Assertions.assertEquals(EnvConstants.REGION_1, clientWithRegion1.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1));
        Assertions.assertEquals(EnvConstants.REGION_1, clientWithNoRegionSpecified.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1));
        Assertions.assertEquals(EnvConstants.REGION_1, clientWithRegion2.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1));
        // Negative test: bucket does not exist
        TestUtils.functionCallThrowsException(() -> clientWithNoRegionSpecified.getBucketLocation(EnvConstants.NOT_EXISTING_BUCKET),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */,
                "The specified bucket does not exist" /* expectedErrorMsg */);
        // Negative test: invalid access key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);
        // Negative test: invalid secret key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
        // Negative test: no credentials provided, access denied.
        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getBucketLocation(EnvConstants.BUCKET_AT_REGION_1),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Negative test: get bucket location for an existing but not accessible bucket name.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getBucketLocation(EnvConstants.BUCKET_EXISTS_BUT_NOT_ACCESSIBLE),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Negative test: invalid bucket name.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getBucketLocation("invalid bucket name"),
                400 /* expectedStatusCode */,
                "InvalidBucketName" /* expectedErrorCode */,
                "The specified bucket is not valid.");

        // Set the region back for later testing
        clientWithRegion1.setRegion(EnvConstants.REGION_1);
        clientWithRegion2.setRegion(EnvConstants.REGION_2);
        clientWithNoCredentials.setRegion(EnvConstants.REGION_1);
        clientWithInvalidKeyId.setRegion(EnvConstants.REGION_1);
        clientWithInvalidSecret.setRegion(EnvConstants.REGION_1);
        clientWithNoRegionSpecified = new S3CompatStorageClient(credentialsProvider, null, EnvConstants.ENDPOINT);
    }
    @Test
    void putObject() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.PUT_OBJECT);
        // Positive test: put a file successfully
        clientWithRegion1.putObject(EnvConstants.BUCKET_AT_REGION_1, prefix, EnvConstants.LOCAL_FILE_PATH_1);
        // Positive test: put a file with user metadata
        testPutObjectWithUserMetadata();
        // Positive test: put a file with up to size of 5GB
        testPutLargeObjectUpTo5GB();
        // Negative test: put object on a wrong region bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.putObject(EnvConstants.BUCKET_AT_REGION_1, prefix, EnvConstants.LOCAL_FILE_PATH_1),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */ ,
                String.format("The authorization header is malformed; the region '%s' is wrong; expecting '%s'" /* expectedErrorMsg */,EnvConstants.REGION_2, EnvConstants.REGION_1 ));
        // Negative test: put object on a non-existing bucket
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.putObject(EnvConstants.NOT_EXISTING_BUCKET, prefix, EnvConstants.LOCAL_FILE_PATH_1),
                404 /* expectedStatusCode */,
                "NoSuchBucket" /* expectedErrorCode */ ,
                "The specified bucket does not exist" /* expectedRegionFromExceptionMsg */);
        // Negative test: put object by providing invalid access key id
        TestUtils.functionCallThrowsException(() -> clientWithInvalidKeyId.putObject(EnvConstants.BUCKET_AT_REGION_1, prefix, EnvConstants.LOCAL_FILE_PATH_1),
                403 /* expectedStatusCode */,
                "InvalidAccessKeyId" /* expectedErrorCode */,
                "The AWS Access Key Id you provided does not exist in our records." /* expectedErrorMsg */);
        // Negative test: put object by providing invalid secret key
        TestUtils.functionCallThrowsException(() -> clientWithInvalidSecret.putObject(EnvConstants.BUCKET_AT_REGION_1, prefix, EnvConstants.LOCAL_FILE_PATH_1),
                403 /* expectedStatusCode */,
                "SignatureDoesNotMatch" /* expectedErrorCode */,
                "The request signature we calculated does not match the signature you provided. Check your key and signing method." /* expectedErrorMsg */);
        // Negative test: put object to an existing but not accessible bucket.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.putObject(EnvConstants.BUCKET_EXISTS_BUT_NOT_ACCESSIBLE, prefix, EnvConstants.LOCAL_FILE_PATH_1),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }
    private void testPutObjectWithUserMetadata() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.PUT_OBJECT);
        Map<String, String> addiontalMetadata = new TreeMap<>();
        addiontalMetadata.putIfAbsent("user", "sf");
        File file = new File(EnvConstants.LOCAL_FILE_PATH_2);
        String filePath = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_2;
        WriteObjectSpec writeObjectSpec = new WriteObjectSpec(
                EnvConstants.BUCKET_AT_REGION_1 /* bucketName*/,
                filePath /* filePath */,
                () -> new FileInputStream(file) /* contentsSupplier */,
                file.length() /* contentLength */,
                null /* clientTimeoutInMs */,
                addiontalMetadata);
        PutObjectResult res = clientWithRegion1.putObject(writeObjectSpec);
        // verify tha the object is successfully put
        RemoteObjectMetadata metadata = clientWithRegion1.getObjectMetadata(EnvConstants.BUCKET_AT_REGION_1, filePath, res.getVersionId());
        Assertions.assertNotNull(metadata.getObjectUserMetadata());
        Assertions.assertEquals(addiontalMetadata.get("user"), metadata.getObjectUserMetadata().get("user"));
        Assertions.assertEquals(file.length(), metadata.getObjectContentLength());
    }
    private void testPutLargeObjectUpTo5GB() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.PUT_OBJECT);
        long size_5GB = 5368709120L;
        File file = new File(EnvConstants.LARGE_FILE_NAME);
        file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(size_5GB);
        raf.close();
        String filePath = prefix + "/" + EnvConstants.LARGE_FILE_NAME;
        // Put the object, should success without error.
        clientWithRegion1.putObject(EnvConstants.BUCKET_AT_REGION_1, filePath, EnvConstants.LARGE_FILE_NAME);
        // Verify that the object is successfully put
        RemoteObjectMetadata metadata = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, filePath);
        Assertions.assertEquals(size_5GB, metadata.getObjectContentLength());
        file.delete();
    }
    @Test
    void getObject() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_OBJECT);
        // Upload a file to ensure we can get it later
        uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        String filePath = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
        File file = new File(EnvConstants.LOCAL_FILE_PATH_1);
        S3CompatObject object = clientWithRegion1.getObject(EnvConstants.BUCKET_AT_REGION_1, filePath);
        Assertions.assertEquals(file.length(), object.getContentLength());
        Assertions.assertEquals(EnvConstants.BUCKET_AT_REGION_1, object.getBucketName());
        Assertions.assertEquals(filePath, object.getKey());
        // Test getObject range
        S3CompatObject object1 = clientWithRegion1.getObject(EnvConstants.BUCKET_AT_REGION_1, filePath, 0L, 8L);
        Assertions.assertEquals(object1.getContentLength(), 9);
        // Negative test: get a file that does not exist
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObject(EnvConstants.BUCKET_AT_REGION_1, "notExisting" + filePath),
                404 /* expectedStatusCode */,
                "NoSuchKey" /* expectedErrorCode */,
                "The specified key does not exist." /* expectedErrorMsg */);
        // Negative test: get a file at a wrong region
        TestUtils.functionCallThrowsException(() -> clientWithRegion2.getObject(EnvConstants.BUCKET_AT_REGION_1, filePath),
                400 /* expectedStatusCode */,
                "AuthorizationHeaderMalformed" /* expectedErrorCode */,
                String.format("The authorization header is malformed; the region '%s' is wrong; expecting '%s'" /* expectedRegionFromExceptionMsg */, EnvConstants.REGION_2, EnvConstants.BUCKET_AT_REGION_1));
        // Negative test: get a file without providing credential.
        TestUtils.functionCallThrowsException(() -> clientWithNoCredentials.getObject(EnvConstants.BUCKET_AT_REGION_1, filePath),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
        // Negative test: get a file an existing but not accessible bucket.
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObject(EnvConstants.BUCKET_EXISTS_BUT_NOT_ACCESSIBLE, filePath),
                403 /* expectedStatusCode */,
                "AccessDenied" /* expectedErrorCode */,
                "Access Denied");
    }
    @Test
    void getObjectMetadata() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GET_OBJECT_METADATA);
        // Put an object to remote location for testing
        PutObjectResult putObjectResult1 = uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        File file = new File(EnvConstants.LOCAL_FILE_PATH_1);
        String filePath = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
        // Test getObjectMetadata without provide a version id
        RemoteObjectMetadata mt1 = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, filePath);
        Assertions.assertEquals(file.length(), mt1.getObjectContentLength());
        Assertions.assertNotNull(mt1.getObjectLastModified());
        Assertions.assertEquals(putObjectResult1.getVersionId(), mt1.getObjectVersionId());
        // Test getObjectMetadata by provide a version id
        RemoteObjectMetadata mt2 = clientWithRegion1.getObjectMetadata(EnvConstants.BUCKET_AT_REGION_1, filePath, putObjectResult1.getVersionId());
        Assertions.assertEquals(putObjectResult1.getVersionId(), mt2.getObjectVersionId());
        TimeUnit.MILLISECONDS.sleep(500);
        // Put the file to the remote location will generate a new version id
        PutObjectResult putObjectResult2 = uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        RemoteObjectMetadata mt3 = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, filePath);
        Assertions.assertEquals(putObjectResult2.getVersionId(), mt3.getObjectVersionId());
        // The current version id should not be equals to the previous one
        Assertions.assertNotEquals(mt3.getObjectVersionId(), mt2.getObjectVersionId());
        // Etag the same
        Assertions.assertEquals(mt3.getObjectETag(), mt2.getObjectETag());
        Assertions.assertTrue(mt1.getObjectLastModified().compareTo(mt3.getObjectLastModified()) < 0 );
        // Negative test: provide wrong version id
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.getObjectMetadata(EnvConstants.BUCKET_AT_REGION_1, filePath, "NonExistingVersion"),
                400 /* expectedStatusCode */,
                "400 Bad Request" /* expectedErrorCode */,
                "Bad Request" /* expectedRegionFromExceptionMsg */);
        // Negative test: get metadata for a non-existing file
        TestUtils.functionCallThrowsException(() -> getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, "not-existing" + filePath),
                404 /* expectedStatusCode */,
                "404 Not Found" /* expectedErrorCode */,
                "Not Found" /* expectedRegionFromExceptionMsg */);
    }
    private RemoteObjectMetadata getObjectMetadata(StorageClient client, String bucketName, String key) {
        return client.getObjectMetadata(bucketName, key, null /* versionId*/);
    }
    @Test
    void listObjectsV2() {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_OBJECTS_V2);
        // Upload files to make sure listing as expected.
        List<String> filesToUpload = Arrays.asList(EnvConstants.LOCAL_FILE_PATH_1, EnvConstants.LOCAL_FILE_PATH_2);
        for (String file: filesToUpload) {
            uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, file);
        }
        Assertions.assertEquals(filesToUpload.size(), clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, prefix, null /* maxKeys */).size());
        testPageListing();
    }
    void testPageListing() {
        // page listing, should have more than 1000 files on the location to trigger default page listing
        if (EnvConstants.PAGE_LISTING_TOTAL_SIZE <= 1000) {
            Assertions.fail("Expect to list over 1000 files.");
        }
        Assertions.assertEquals(EnvConstants.PAGE_LISTING_TOTAL_SIZE, clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, EnvConstants.PREFIX_FOR_PAGE_LISTING_AT_REG_1, null /* maxKeys */).size());
        // Test list with max keys specified
        int maxKeys = 300;
        Assertions.assertEquals(EnvConstants.PAGE_LISTING_TOTAL_SIZE, clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, EnvConstants.PREFIX_FOR_PAGE_LISTING_AT_REG_1, maxKeys).size());
    }
    @Test
    void listVersions() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_VERSIONS);
        // Upload files to make sure listing as expected.
        uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        List<S3VersionSummary> summaries1 =  clientWithRegion1.listVersions(EnvConstants.BUCKET_AT_REGION_1, prefix + '/' + EnvConstants.LOCAL_FILE_PATH_1, true /* useUrlEncoding */, /* maxKey */ null);
        List<S3VersionSummary> summaries2 = clientWithRegion1.listVersions(EnvConstants.BUCKET_AT_REGION_1, prefix + '/' + EnvConstants.LOCAL_FILE_PATH_1, false /* useUrlEncoding */, /* maxKey */ null);
        for (int i = 0; i < summaries1.size(); i++) {
            S3VersionSummary v1 = summaries1.get(i);
            S3VersionSummary v2 = summaries2.get(i);
            Assertions.assertEquals(v1.getKey(), v2.getKey());
            Assertions.assertEquals(v1.getVersionId(), v2.getVersionId());
            Assertions.assertEquals(v1.getETag(), v2.getETag());
            Assertions.assertEquals(v1.getSize(), v2.getSize());
        }
        listNextBatchOfVersions();
    }

    void listNextBatchOfVersions() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.LIST_VERSIONS);
        // upload same file multiple times, to generate different versions
        File file = new File(EnvConstants.LOCAL_FILE_PATH_2);
        String filePath = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_2;
        WriteObjectSpec writeObjectSpec = new WriteObjectSpec(
                EnvConstants.BUCKET_AT_REGION_1 /* bucketName*/,
                filePath /* filePath */,
                () -> new FileInputStream(file) /* contentsSupplier */,
                file.length() /* contentLength */,
                null /* clientTimeoutInMs */,
                null /* additional metadata*/);
        int numVersions = 10;
        for (int i = 0; i < numVersions; i++) {
            clientWithRegion1.putObject(writeObjectSpec);
        }
        // set max result of list request to 2
        List<S3VersionSummary> summaries =  clientWithRegion1.listVersions(EnvConstants.BUCKET_AT_REGION_1, prefix + '/' + EnvConstants.LOCAL_FILE_PATH_2, true /* useUrlEncoding */, /* maxKey */ 2);
        Assertions.assertEquals(summaries.size(), numVersions);
    }
    @Test
    void deleteObject() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.DELETE_OBJECT);
        // Upload a file to make sure to delete the target.
        uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        Assertions.assertEquals(1, clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, prefix, null /* maxKeys */).size());
        clientWithRegion1.deleteObject(EnvConstants.BUCKET_AT_REGION_1, prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1);
        Assertions.assertTrue(clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, prefix, null /* maxKeys */).isEmpty());
    }
    @Test
    void deleteObjects() throws Exception {
        updatePrefixForTestCase(TestUtils.OPERATIONS.DELETE_OBJECTS);
        // Put files into the location for testing delete
        List<String> files = Arrays.asList(EnvConstants.LOCAL_FILE_PATH_1, EnvConstants.LOCAL_FILE_PATH_2);
        for (String file: files) {
            uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, file);
        }
        String key1 = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
        String key2 = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_2;
        // Ensure files uploaded to  the location for testing.
        Assertions.assertEquals(files.size(), clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, prefix, null /* maxKeys */).size());
        DeleteRemoteObjectSpec objectWithWrongVersion = new DeleteRemoteObjectSpec(key1, "versionId");
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        toDelete.add(objectWithWrongVersion);
        // Delete fails as object is provided with a wrong version id, will receive MultiObjectDeleteException
        TestUtils.functionCallThrowsException(() -> clientWithRegion1.deleteObjects(EnvConstants.BUCKET_AT_REGION_1, toDelete),
                200 /* expectedStatusCode */,
                null /* expectedErrorCode */,
                null /* expectedRegionFromExceptionMsg */);
        toDelete.remove(0);// remove the one with wrong version id
        // To delete an object without providing version id
        toDelete.add(new DeleteRemoteObjectSpec(key1));
        // To delete an object by providing version id
        toDelete.add(new DeleteRemoteObjectSpec(key2, getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, key2 ).getObjectVersionId()));
        Assertions.assertEquals(toDelete.size(), clientWithRegion1.deleteObjects(EnvConstants.BUCKET_AT_REGION_1, toDelete));
        // Listing should return 0 at the prefix since files have been deleted.
        Assertions.assertEquals(0, clientWithRegion1.listObjectsV2(EnvConstants.BUCKET_AT_REGION_1, prefix, null /* maxKeys */).size());
    }
    @Test
    void copyObject() throws IOException {
        updatePrefixForTestCase(TestUtils.OPERATIONS.COPY_OBJECT);
        // put a file at the location for copy
        uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        String sourceFileName = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
        String dstFileName = "dst1_" + sourceFileName;
        // Copy without providing version id
        clientWithRegion1.copyObject(EnvConstants.BUCKET_AT_REGION_1, sourceFileName, null, EnvConstants.BUCKET_AT_REGION_1, dstFileName );
        RemoteObjectMetadata dstMt1 = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, dstFileName);
        RemoteObjectMetadata source = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, sourceFileName);
        Assertions.assertEquals(source.getObjectContentLength(), dstMt1.getObjectContentLength());
        Assertions.assertEquals(source.getObjectETag(), dstMt1.getObjectETag());
        // Copy providing version id
        String dstFileName2 = "dst2_" + sourceFileName;
        clientWithRegion1.copyObject(EnvConstants.BUCKET_AT_REGION_1, sourceFileName, source.getObjectVersionId(), EnvConstants.BUCKET_AT_REGION_1, dstFileName2 );
        RemoteObjectMetadata dstMt2 = getObjectMetadata(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, dstFileName2);
        Assertions.assertNotEquals(dstMt1.getObjectVersionId(), dstMt2.getObjectVersionId());
        Assertions.assertEquals(dstMt1.getObjectETag(), dstMt2.getObjectETag());
    }
    @Test
    void generatePresignedUrl() {
        updatePrefixForTestCase(TestUtils.OPERATIONS.GENERATE_PRESIGNED_URL);
        // Upload a file to ensure a file to get pre-signed url
        uploadAnObjectToTestingLocation(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, EnvConstants.LOCAL_FILE_PATH_1);
        String filePath = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
        try {
            // Test valid presigned url
            String validPresignedUrl1 = generatePresignedUrl(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, filePath);
            URL presignedUrl1 = new URL(validPresignedUrl1);
            HttpURLConnection c = (HttpURLConnection) presignedUrl1.openConnection();
            Assertions.assertEquals(200, c.getResponseCode());
        } catch (IOException e) {
            Assertions.fail("Unexpected failure for using presigned url: " + e);
        }
        // Expired presigned url
        testForbiddenPresignedUrl(clientWithRegion1, EnvConstants.BUCKET_AT_REGION_1, filePath, 1 /* expiryTime */);
        // Url presigned with invalid key id
        testForbiddenPresignedUrl(clientWithInvalidKeyId, EnvConstants.BUCKET_AT_REGION_1, filePath, null /* expiryTime */);
        // Url presigned with invalid secret key
        testForbiddenPresignedUrl(clientWithInvalidSecret, EnvConstants.BUCKET_AT_REGION_1, filePath, null /* expiryTime */);
    }
    private void testForbiddenPresignedUrl(StorageClient client, String bucketName, String filePath, Integer expiryTime) {
        String forbiddenUrlStr;
        try {
            if (expiryTime == null) {
                forbiddenUrlStr = generatePresignedUrl(client, bucketName, filePath);
            } else {
                forbiddenUrlStr = client.generatePresignedUrl(bucketName, filePath, expiryTime, null /* contentType*/);
                TimeUnit.SECONDS.sleep(expiryTime + 2);
            }
            URL expiredUrl = new URL(forbiddenUrlStr);
            HttpURLConnection c = (HttpURLConnection) expiredUrl.openConnection();
            Assertions.assertEquals(403, c.getResponseCode());
            Assertions.assertEquals("Forbidden", c.getResponseMessage());
        } catch (IOException e) {
            Assertions.fail("Unexpected failure: " + e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * Generate a pre-signed url using default expiry time, and without providing content type.
     */
    private String generatePresignedUrl(StorageClient client, String bucketName, String key) {
        return client.generatePresignedUrl(bucketName, key, -1 /* expiryTime */, null /* contentType */);
    }
    private static void updatePrefixForTestCase(TestUtils.OPERATIONS operation) {
        prefix = EnvConstants.PREFIX_FOR_BUCKET_AT_REG_1 + "/" + operation;
    }
    /**
     * Upload a file to a remote location with prefix {@value prefix} under a bucket, the prefix is updated for each test.
     */
    private PutObjectResult uploadAnObjectToTestingLocation(StorageClient client, String bucket, String fileName) {
        return client.putObject(bucket, prefix + '/' + fileName, fileName);
    }
}
