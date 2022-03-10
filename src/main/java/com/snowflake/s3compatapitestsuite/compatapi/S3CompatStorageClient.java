/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Wrapper for a S3Compat storage client.
 */
public class S3CompatStorageClient implements StorageClient {
    private static final Logger logger = LogManager.getLogger(S3CompatStorageClient.class);
    final AmazonS3 s3Client;
    private static final int CLIENT_TIMEOUT_FOR_READ = 20_000; // in MS
    private static final String ENCODING = "UTF-8";
    /** Configurable value to use for the max error retry configuration when creating an S3 client. */
    private static final int MAX_ERROR_RETRY = 5;
    /**
     * Constructor for a s3 compat storage client.
     * @param awsCredentialsProvider Wrapper for aws credential.
     * @param region The region the client targeting.
     * @param endpoint Endpoint the client would make requests to.
     */
    public S3CompatStorageClient(
            @Nullable AWSCredentialsProvider awsCredentialsProvider,
            @Nullable String region,
            String endpoint) {
        this.s3Client = createS3Client(region, awsCredentialsProvider, endpoint);
    }
    private AmazonS3 createS3Client(
            final @Nullable String region,
            final @Nullable AWSCredentialsProvider awsCredentialsProvide,
            final String endpoint) {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.withSignerOverride("AWSS3V4SignerType");
        clientCfg.setMaxErrorRetry(MAX_ERROR_RETRY);
        clientCfg.withSocketTimeout(10_000);
        clientCfg.withTcpKeepAlive(true);
        AmazonS3 s3Client;
        s3Client = new AmazonS3Client(awsCredentialsProvide, clientCfg);
        s3Client.setEndpoint(endpoint);
        if (region != null) {
            s3Client.setRegion(Region.getRegion(Regions.fromName(region)));
        }
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(false).build());
        return s3Client;
    }
    @Override
    public String getBucketLocation(String bucketName) {
        String regionRes;
        try {
            regionRes = this.s3Client.getBucketLocation(bucketName);
        } catch (AmazonS3Exception ex) {
            if (ex.getAdditionalDetails() != null) {
                String correctRegion = ex.getAdditionalDetails().get("Region");
                if (correctRegion != null) {
                    this.s3Client.setRegion(Region.getRegion(Regions.fromName(correctRegion)));
                    return this.s3Client.getBucketLocation(bucketName);
                }
            }
            throw ex;
        }
        return regionRes;
    }
    @Override
    public S3Object getObject(String bucketName, String key) {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            if (CLIENT_TIMEOUT_FOR_READ > 0) {
                request.setSdkClientExecutionTimeout(CLIENT_TIMEOUT_FOR_READ);
            }
            return this.s3Client.getObject(request);
        } catch (AmazonS3Exception | IllegalArgumentException ex) {
            throw ex;
        }
    }
    @Override
    public RemoteObjectMetadata getObjectMetadata(String bucketName, String key, @Nullable String versionId) throws AmazonS3Exception{
        GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
        if (versionId != null ) {
            objectMetadataRequest.setVersionId(versionId);
        }
        return RemoteObjectMetadata.fromS3ObjectMetadata(this.s3Client.getObjectMetadata(objectMetadataRequest));
    }
    /**
     * Get metadata of an object.
     * @param bucketName Bucket name where the object locates.
     * @param filePath path of a file .
     * @return Metadata of the object.
     */
    public RemoteObjectMetadata getObjectMetadata(String bucketName, String filePath) {
        return getObjectMetadata(bucketName, filePath, null);
    }
    @Override
    public PutObjectResult putObject(String bucketName, String key, String fileName) {
        final File file = new File(fileName);
        PutObjectResult res;
        try {
            WriteObjectSpec writeObjectSpec = new WriteObjectSpec(bucketName, key + "/" + fileName, () -> new FileInputStream(file), file.length(), null /* timeoutInMs */, null);
            res = putObject(writeObjectSpec);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }
    @Override
    public PutObjectResult putObject(WriteObjectSpec writeObjectSpec) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(writeObjectSpec.getContentLengthToWrite());
        if (writeObjectSpec.getAdditionalBlobMetadata() != null) {
            if (meta.getUserMetadata() == null) {
                meta.setUserMetadata(new TreeMap<>());
            }
            writeObjectSpec.getAdditionalBlobMetadata().forEach((key, value) -> meta.getUserMetadata().put(key, value));
        }
        try {
            PutObjectRequest request =
                    new PutObjectRequest(writeObjectSpec.getBucketName(), writeObjectSpec.getFilePath(), writeObjectSpec.getInputStream(), meta);
            if (writeObjectSpec.getClientTimeoutInMs() != null && writeObjectSpec.getClientTimeoutInMs() > 0) {
                request.setSdkClientExecutionTimeout(writeObjectSpec.getClientTimeoutInMs());
            }
            PutObjectResult putResult = this.s3Client.putObject(request);
            RemoteObjectMetadata objectMetadata = getObjectMetadata(writeObjectSpec.getBucketName(), writeObjectSpec.getFilePath(), putResult.getVersionId());
            if (putResult == null) {
                throw new RuntimeException("Put Object result should not be null! ");
            }
            if (putResult.getVersionId() != null && !putResult.getVersionId().equalsIgnoreCase(objectMetadata.getObjectVersionId())) {
                throw new RuntimeException("Version id not match a read after write.");
            } else {
                logger.log(Level.INFO, "Put " + objectMetadata.getObjectContentLength() + " bytes to remote location " + writeObjectSpec.getBucketName() + "/" + writeObjectSpec.getFilePath());
            }
            return putResult;
        } catch (AmazonS3Exception ex) {
            throw ex;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Fail to putObject:" + writeObjectSpec.getFilePath());
    }
    @Override
    public List<S3ObjectSummary> listObjects(String bucketName, String prefix) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.withBucketName(bucketName);
        listObjectsRequest.withPrefix(prefix);
        try {
            ObjectListing listResult = this.s3Client.listObjects(listObjectsRequest);
            String encodingType = listResult.getEncodingType();
            boolean truncated = listResult.isTruncated();
            int pageSize = listResult.getMaxKeys();
            List<S3ObjectSummary> s3Summaries = fromObjectListing(listResult);
            while (listResult.getNextMarker() != null && listResult.isTruncated()) {
                // call listNextBatchOfObjects
                listResult = this.s3Client.listNextBatchOfObjects(listResult);
                s3Summaries.addAll(fromObjectListing(listResult));
            }
            return s3Summaries;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    private @NotNull List<S3ObjectSummary> fromObjectListing(@NotNull ObjectListing ol) throws UnsupportedEncodingException {
        String encodingType = ol.getEncodingType();
        String bucketName = ol.getBucketName();
        String prefix = ol.getPrefix();
        boolean truncated = ol.isTruncated();
        int pageSize = ol.getMaxKeys();
        List<S3ObjectSummary> s3Summaries = ol.getObjectSummaries();
        if (encodingType != null) {
            // Some special logic if we are URL Encoded.
            // First, validate that it is actually url encoding.
            if (!"url".equals(encodingType)) {
                throw new IllegalArgumentException("Unexpected encoding type: " + encodingType);
            }
            for (S3ObjectSummary s3Obj : s3Summaries) {
                s3Obj.setKey(URLDecoder.decode(s3Obj.getKey(), ENCODING));
            }
            // The server expects it to be non-url encoded.  Reverse the encoding here in order
            // to enable proper object listing with url encoding on.
            String urlEncodedNextMarker = ol.getNextMarker();
            if (null != urlEncodedNextMarker) {
                String decodedMarker = URLDecoder.decode(urlEncodedNextMarker, ENCODING);
                ol.setNextMarker(decodedMarker);
            }
        }
        return s3Summaries;
    }
    @Override
    public List<S3ObjectSummary> listObjectsV2(String bucketName, String prefix, @Nullable Integer maxKeys) {
        ListObjectsV2Request listV2Req = new ListObjectsV2Request();
        listV2Req = listV2Req.withBucketName(bucketName);
        listV2Req = listV2Req.withPrefix(prefix);
        if (maxKeys != null) {
            listV2Req.withMaxKeys(maxKeys);
        }
        try {
            ListObjectsV2Result listV2Res = this.s3Client.listObjectsV2(listV2Req);
            List<S3ObjectSummary> s3ObjectSummaries = fromV2ObjectListing(listV2Res, listV2Req);
            while (listV2Res.getNextContinuationToken() != null && listV2Res.isTruncated()) {
                listV2Res = this.s3Client.listObjectsV2(listV2Req);
                s3ObjectSummaries.addAll(fromV2ObjectListing(listV2Res, listV2Req));
            }
            return s3ObjectSummaries;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
    private @NotNull List<S3ObjectSummary> fromV2ObjectListing(@NotNull ListObjectsV2Result listRes,
                                                               @NotNull ListObjectsV2Request listReq)
            throws UnsupportedEncodingException {
        String encodingType = listRes.getEncodingType();
        String bucketName = listRes.getBucketName();
        String prefix = listRes.getPrefix();
        boolean truncated = listRes.isTruncated();
        int pageSize = listRes.getMaxKeys();
        List<S3ObjectSummary> s3Summaries = listRes.getObjectSummaries();
        String continuationToken = listRes.getNextContinuationToken();
        if (null != continuationToken) {
            listReq.setContinuationToken(continuationToken);
        }
        if (encodingType != null) {
            // Some special logic if we are URL Encoded.
            // First, validate that it is actually url encoding.
            if (!"url".equals(encodingType)) {
                throw new IllegalArgumentException("Unexpected encoding type: " + encodingType);
            }
            for (S3ObjectSummary s3Obj : s3Summaries) {
                s3Obj.setKey(URLDecoder.decode(s3Obj.getKey(), ENCODING));
            }
        }
        return s3Summaries;
    }
    @Override
    public List<S3VersionSummary> listVersions(String bucketName, String key, boolean useUrlEncoding) {
        List<S3VersionSummary> versionSum = null;
        try {
            ListVersionsRequest listVersionsReq = new ListVersionsRequest();
            listVersionsReq.withBucketName(bucketName);
            listVersionsReq.withPrefix(key);
            if (useUrlEncoding) {
                listVersionsReq.withEncodingType("url");
            }
            VersionListing lsVersions = this.s3Client.listVersions(listVersionsReq);
            versionSum = fromListVersions(lsVersions);
            if (lsVersions.isTruncated()) {
                lsVersions = this.s3Client.listNextBatchOfVersions(lsVersions);
                versionSum.addAll(fromListVersions(lsVersions));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return versionSum;
    }
    private @NotNull List<S3VersionSummary> fromListVersions(@NotNull VersionListing vl) throws UnsupportedEncodingException {
        String encodingType = vl.getEncodingType();

        boolean truncated = vl.isTruncated();
        int pageSize = vl.getMaxKeys();
        String bucketName = vl.getBucketName();
        String prefix = vl.getPrefix();
        List<S3VersionSummary> s3Summaries = vl.getVersionSummaries();
        if (encodingType != null) {
            // Some special logic if we are URL Encoded.
            // First, validate that it is actually url encoding.
            if (!"url".equals(encodingType)) {
                throw new IllegalArgumentException("Unexpected encoding type: " + encodingType);
            }
            for (S3VersionSummary version : s3Summaries) {
                version.setKey(URLDecoder.decode(version.getKey(), ENCODING));
            }
            String urlEncodedNextKeyMarker = vl.getNextKeyMarker();
            if (null != urlEncodedNextKeyMarker) {
                String decodedMarker = URLDecoder.decode(urlEncodedNextKeyMarker, ENCODING);
                vl.setNextKeyMarker(decodedMarker);
            }
        }
        return s3Summaries;
    }
    @Override
    public void deleteObject(String bucketName, String fileKey) {
        try {
            this.s3Client.deleteObject(bucketName, fileKey);
        } catch (AmazonS3Exception ex) {
            throw ex;
        }
    }
    @Override
    public int deleteObjects(String bucketName, String prefixPath) {
        int numDeleted = 0;
        try {
            for (S3ObjectSummary file: this.s3Client.listObjects(bucketName, prefixPath).getObjectSummaries()) {
                this.s3Client.deleteObject(bucketName, file.getKey());
                numDeleted++;
            }
        } catch (AmazonS3Exception ex) {
            throw ex;
        }
        return numDeleted;
    }
    @Override
    public int deleteObjects(String bucketName, List<DeleteRemoteObjectSpec> toDeleteList){
        List<DeleteObjectsRequest.KeyVersion> kvList = new ArrayList<>(toDeleteList.size());
        if (toDeleteList.isEmpty()) {
            return 0;
        }
        for (DeleteRemoteObjectSpec objectSpec: toDeleteList) {
            kvList.add(objectSpec.toS3KeyVersion());
        }
        try {
            DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName);
            dor.setKeys(kvList);
            DeleteObjectsResult result = this.s3Client.deleteObjects(dor);
            return result.getDeletedObjects().size();
        } catch (AmazonClientException ex) {
            throw ex;
        }
    }
    @Override
    public void copyObject(String sourceBucket, String sourceKey, @Nullable String sourceFileVersionId, String dstBucket, String destKey) {
        CopyObjectRequest cpReq;
        if (sourceFileVersionId != null) {
            cpReq = new CopyObjectRequest(sourceBucket, sourceKey, sourceFileVersionId, dstBucket, destKey);
        } else {
            cpReq = new CopyObjectRequest(sourceBucket, sourceKey, dstBucket, destKey);
        }
        try {
            this.s3Client.copyObject(cpReq);
        } catch (AmazonS3Exception ex) {
            throw ex;
        }
    }
    @Override
    public void setRegion(@Nullable String region) {
         try {
             this.s3Client.setRegion(Region.getRegion(Regions.fromName(region)));
         } catch (AmazonS3Exception ex) {
             throw ex;
         }
    }
    @Override
    public String generatePresignedUrl(String bucketName, String key, int expiryTimeInSec, String contentType) {
        PreSignedUrlGenerator pg =  new PreSignedUrlGenerator()
                .withBucketName(bucketName)
                .withClient(this)
                .withKey(key)
                .withLifetimeInSeconds(expiryTimeInSec);
        if (contentType != null) {
            pg = pg.withContentType(contentType);
        }
        return pg.generate();
    }
    /**
     * Get the region name for the client.
     * @return The name of the region.
     */
    public String getRegionName() {
        return this.s3Client.getRegionName();
    }
}
