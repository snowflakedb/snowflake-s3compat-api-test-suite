/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.google.common.base.Strings;
import org.jetbrains.annotations.NotNull;
import java.net.URL;
import java.util.Date;

/** Generate pre-signed URL for a file. */
public class PreSignedUrlGenerator {
    /** Bucket name of the file where stores */
    private @NotNull String bucketName;
    /** Validity period in seconds. -1 means use default value */
    private int lifetimeInSecs;
    /**
     * The key for the file, which is the "prefix + file name".
     * eg: path1/path2/file1.txt
     */
    private @NotNull String key;
    /** Content type (e.g. "image/jpeg") */
    private String contentType;
    /** Content encoding of the response */
    private String responseContentEncoding;
    /** The client this generator will use */
    private @NotNull S3CompatStorageClient client;
    /**
     * Generate pre-signed url
     *
     * @return A pre-signed url.
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

    /**
     * Generate with bucket name provided.
     * @param bucketName Bucket name.
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }
    /**
     * Generate with a life time span.
     * @param lifetimeInSecs Time in seconds to expire.
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withLifetimeInSeconds(int lifetimeInSecs) {
        this.lifetimeInSecs = lifetimeInSecs;
        return this;
    }
    /**
     * Generate with a file key.
     * @param key The key for the file, which is the "prefix + file name".
     *         eg: path1/path2/file1.txt
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withKey(String key) {
        this.key = key;
        return this;
    }
    /**
     * Generate with content type provided.
     * @param contentType
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }
    /**
     * Generate with require of response content encoding.
     * @param responseContentEncoding Response content encoding.
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withResponseContentEncoding(
            String responseContentEncoding) {
        this.responseContentEncoding = responseContentEncoding;
        return this;
    }
    /**
     * Generate with a client provided.
     * @param client The client that the generator would use to make requests.
     * @return The presigned url generator.
     */
    public @NotNull PreSignedUrlGenerator withClient(final S3CompatStorageClient client) {
        this.client = client;
        return this;
    }
}
