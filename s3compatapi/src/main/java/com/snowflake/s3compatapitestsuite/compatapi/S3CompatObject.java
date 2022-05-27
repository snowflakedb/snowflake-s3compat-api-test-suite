/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;

public class S3CompatObject {

    private InputStream inputStream;
    private String eTag;
    private long contentLength;
    private String versionId;
    private String bucketName;
    private String key;

    /**
     * Constuctor for an S3CompatObject
     * @param s3Object s3Object
     */
    public S3CompatObject(S3Object s3Object) {
        this.inputStream = s3Object.getObjectContent();
        this.eTag = s3Object.getObjectMetadata().getETag();
        this.contentLength = s3Object.getObjectMetadata().getContentLength();
        this.versionId = s3Object.getObjectMetadata().getVersionId();
        this.bucketName = s3Object.getBucketName();
        this.key = s3Object.getKey();
    }

    public String getETag() {
        return eTag;
    }

    public long getContentLength() {
        return contentLength;
    }

    public String getVersionId() {
        return versionId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }
}
