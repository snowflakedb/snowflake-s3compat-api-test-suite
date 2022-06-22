/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.snowflake.s3compatapitestsuite.perf.PerfMeasurement;
import com.snowflake.s3compatapitestsuite.perf.PerfMeasurementRecorder;
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
    final InstrumentedAmazonS3Client s3Client;
    private static final int TIME_OUT = 300_000; // in MS
    private static final String ENCODING = "UTF-8";
    /** Configurable value to use for the max error retry configuration when creating an S3 client. */
    private static final int MAX_ERROR_RETRY = 5;

    private boolean measurementPerformance = false;

    private PerfMeasurementRecorder perfMeasurement;
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
    private InstrumentedAmazonS3Client createS3Client(
            final @Nullable String region,
            final @Nullable AWSCredentialsProvider awsCredentialsProvide,
            final String endpoint) {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.withSignerOverride("AWSS3V4SignerType");
        clientCfg.setMaxErrorRetry(MAX_ERROR_RETRY);
        clientCfg.withSocketTimeout(TIME_OUT);
        clientCfg.withTcpKeepAlive(true);
        InstrumentedAmazonS3Client s3Client;
        if (awsCredentialsProvide == null) {
            s3Client = new InstrumentedAmazonS3Client(null, clientCfg);
        } else {
            s3Client = new InstrumentedAmazonS3Client(awsCredentialsProvide.getCredentials(), clientCfg);
        }
        if (region != null) {
            s3Client.setSignerRegionOverride(region);
            s3Client.withRegion(RegionUtils.getRegion(region));
        }
        s3Client.setEndpoint(endpoint);
        // explicitly force using virtual style access for S3
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(false).build());
        return s3Client;
    }
    @Override
    public String getBucketLocation(String bucketName) {
        String regionRes;
        try {
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.GET_BUCKET_LOCATION);
            }
            regionRes = this.s3Client.getBucketLocation(bucketName);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.GET_BUCKET_LOCATION);
            }
            if ("US".equals(regionRes)) {
                // For backward compatibility reasons, AWS returns "US" for the standard region in
                // us-east-1.
                regionRes = "us-east-1";
            }
        } catch (AmazonS3Exception ex) {
            if (ex.getAdditionalDetails() != null) {
                String correctRegion = ex.getAdditionalDetails().get("Region");
                if (correctRegion != null) {
                    this.s3Client.setSignerRegionOverride(correctRegion);
                    this.s3Client.setRegion(RegionUtils.getRegion(correctRegion));
                    return this.s3Client.getBucketLocation(bucketName);
                }
            }
            throw ex;
        }
        return regionRes;
    }
    @Override
    public S3CompatObject getObject(String bucketName, String key) {
        return getObject(bucketName, key, null, null);
    }

    public S3CompatObject getObject(String bucketName, String key, @Nullable Long start, @Nullable Long end) {
        S3Object res = null;
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            if (start != null) {
                request.setRange(start);
            }
            if (start != null && end != null) {
                request.setRange(start, end);
            }
            if (TIME_OUT > 0) {
                request.setSdkClientExecutionTimeout(TIME_OUT);
            }
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.GET_OBJECT);
            }
            res = this.s3Client.getObject(request);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.GET_OBJECT);
            }
            return new S3CompatObject(res);
        } catch (AmazonS3Exception | IllegalArgumentException ex) {
            throw ex;
        } finally {
            if (res != null) {
                try {
                    res.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Override
    public RemoteObjectMetadata getObjectMetadata(String bucketName, String key, @Nullable String versionId) throws AmazonS3Exception{
        GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
        if (versionId != null ) {
            objectMetadataRequest.setVersionId(versionId);
        }
        if (measurementPerformance && perfMeasurement != null) {
            perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.GET_OBJECT_METADATA);
        }
        RemoteObjectMetadata res = RemoteObjectMetadata.fromS3ObjectMetadata(this.s3Client.getObjectMetadata(objectMetadataRequest));
        if (measurementPerformance && perfMeasurement != null) {
            perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.GET_OBJECT_METADATA);
        }
        return res;
    }
    @Override
    public PutObjectResult putObject(String bucketName, String key, String fileName) {
        final File file = new File(fileName);
        PutObjectResult res;
        try {
            WriteObjectSpec writeObjectSpec = new WriteObjectSpec(bucketName, key, () -> new FileInputStream(file), file.length(), null /* timeoutInMs */, null);
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
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.PUT_OBJECT);
            }
            PutObjectResult putResult = this.s3Client.putObject(request);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.PUT_OBJECT);
            }
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
    public List<S3ObjectSummary> listObjectsV2(String bucketName, String prefix, @Nullable Integer maxKeys, PerfMeasurement.FUNC_NAME funcName) {
        if (!(funcName == PerfMeasurement.FUNC_NAME.LIST_OBJECTS_V2 || funcName == PerfMeasurement.FUNC_NAME.LIST_LARGE_NUM_OBJECTS)) {
            throw new IllegalArgumentException("Only LIST_OBJECT_V2 or LIST_LARGE_NUM_OBJECTS allowed, illegal param: " + funcName.name());
        }
        ListObjectsV2Request listV2Req = new ListObjectsV2Request();
        listV2Req = listV2Req.withBucketName(bucketName);
        listV2Req = listV2Req.withPrefix(prefix);
        if (maxKeys != null) {
            listV2Req.withMaxKeys(maxKeys);
        }
        try {
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(funcName);
            }
            ListObjectsV2Result listV2Res = this.s3Client.listObjectsV2(listV2Req);
            List<S3ObjectSummary> s3ObjectSummaries = fromV2ObjectListing(listV2Res, listV2Req);
            while (listV2Res.getNextContinuationToken() != null && listV2Res.isTruncated()) {
                listV2Res = this.s3Client.listObjectsV2(listV2Req);
                s3ObjectSummaries.addAll(fromV2ObjectListing(listV2Res, listV2Req));
            }
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(funcName);
            }
            return s3ObjectSummaries;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
    @Override
    public List<S3ObjectSummary> listObjectsV2(String bucketName, String prefix, @Nullable Integer maxKeys) {
        return listObjectsV2(bucketName, prefix, maxKeys, PerfMeasurement.FUNC_NAME.LIST_OBJECTS_V2);
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
    public List<S3VersionSummary> listVersions(String bucketName, String key, boolean useUrlEncoding, @Nullable Integer maxKey) {
        List<S3VersionSummary> versionSum = null;
        try {
            ListVersionsRequest listVersionsReq = new ListVersionsRequest();
            listVersionsReq.withBucketName(bucketName);
            listVersionsReq.withPrefix(key);
            if (useUrlEncoding) {
                listVersionsReq.withEncodingType("url");
            }
            if (maxKey != null) {
                listVersionsReq.withMaxResults(maxKey);
            }
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.LIST_VERSIONS);
            }
            VersionListing vl = this.s3Client.listVersions(listVersionsReq);
            versionSum = fromListVersions(vl);
            while (vl.isTruncated()) {
                vl = this.s3Client.listNextBatchOfVersions(vl);
                versionSum.addAll(fromListVersions(vl));
            }
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.LIST_VERSIONS);
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
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.DELETE_OBJECT);
            }
            this.s3Client.deleteObject(bucketName, fileKey);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.DELETE_OBJECT);
            }
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
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.DELETE_OBJECTS);
            }
            DeleteObjectsResult result = this.s3Client.deleteObjects(dor);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.DELETE_OBJECTS);
            }
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
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.startTiming(PerfMeasurement.FUNC_NAME.COPY_OBJECT);
            }
            this.s3Client.copyObject(cpReq);
            if (measurementPerformance && perfMeasurement != null) {
                perfMeasurement.recordElapsedTime(PerfMeasurement.FUNC_NAME.COPY_OBJECT);
            }
        } catch (AmazonS3Exception ex) {
            throw ex;
        }
    }
    @Override
    public void setRegion(@Nullable String region) {
         try {
             this.s3Client.setRegion(RegionUtils.getRegion(region));
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

    public void setMeasurementPerformance(boolean measurementPerformance) {
        this.measurementPerformance = measurementPerformance;
    }

    public void setPerfMeasurement(PerfMeasurement.FUNC_NAME funcName) {
        this.perfMeasurement = new PerfMeasurementRecorder(funcName);
    }
}
