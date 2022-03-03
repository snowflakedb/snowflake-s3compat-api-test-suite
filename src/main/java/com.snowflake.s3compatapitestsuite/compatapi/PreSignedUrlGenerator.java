package com.snowflake.s3compatapitestsuite.compatapi;

/**
 * Generate pre-signed URL for a file PUT, GET or DELETE operation.
 *
 * @author aravind.r
 */
import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.google.common.base.Strings;
import org.jetbrains.annotations.NotNull;

import java.net.URL;
import java.util.Date;

/** Generate pre-signed URL for a file. Default for GET operation */
public class PreSignedUrlGenerator {
    private @NotNull String bucketName;
    /** Validity period in seconds */
    private int lifetimeInSecs;
    /** The file  */
    private @NotNull String key;
    /** Content type (e.g. "image/jpeg") */
    private String contentType;
    /** Content encoding of the response */
    private String responseContentEncoding;

    private @NotNull S3CompatStorageClient client;

    /**
     * generate pre-signed url
     *
     * @return pre-signed url.
     */
    public String generate() {
        try {
            // compute expiration time
            long now = System.currentTimeMillis();
            if (lifetimeInSecs < 0) {
                lifetimeInSecs = 3600; // default value
            }
            long expiresAt = now + lifetimeInSecs * 1000;
            java.util.Date expiration = new Date(expiresAt);

            // create a new GeneratePresignedUrlRequest instance
            GeneratePresignedUrlRequest presignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, key);

            // update the request with the url expiration time and the http method
            presignedUrlRequest.withExpiration(expiration).withMethod(HttpMethod.GET);
            // if a valid content type has been specified
            if (!Strings.isNullOrEmpty(contentType)) {
                presignedUrlRequest.withContentType(contentType);
            }
            // if a content encoding has been specified for the response
            if (!Strings.isNullOrEmpty(responseContentEncoding)) {
                ResponseHeaderOverrides respHeaders = new ResponseHeaderOverrides();
                respHeaders.setContentEncoding(responseContentEncoding);
                // update the request with this information
                presignedUrlRequest.withResponseHeaders(respHeaders);
            }
            URL url = this.client.s3Client.generatePresignedUrl(presignedUrlRequest);
            return url.toString();
        } catch (AmazonClientException ex) {
            throw new RuntimeException(ex);
        }
    }

    public @NotNull PreSignedUrlGenerator withBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public @NotNull PreSignedUrlGenerator withLifetimeInSeconds(int lifetimeInSecs) {
        this.lifetimeInSecs = lifetimeInSecs;
        return this;
    }

    public @NotNull PreSignedUrlGenerator withKey(String key) {
        this.key = key;
        return this;
    }

    public @NotNull PreSignedUrlGenerator withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public @NotNull PreSignedUrlGenerator withResponseContentEncoding(
            String responseContentEncoding) {
        this.responseContentEncoding = responseContentEncoding;
        return this;
    }

    public @NotNull PreSignedUrlGenerator withClient(final S3CompatStorageClient client) {
        this.client = client;
        return this;
    }
}
