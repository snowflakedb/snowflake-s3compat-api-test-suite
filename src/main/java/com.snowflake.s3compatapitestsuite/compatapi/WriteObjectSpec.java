/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Wrapper to write an object.
 */
public class WriteObjectSpec {
    private static final Logger logger = LogManager.getLogger(WriteObjectSpec.class);
    private final String bucketName;
    /** The file path which is relative to the bucket name. Correspond to S3 key.*/
    private final String filePath;
    /** Encapsulates the input stream to write to remote storage. */
    private final @NotNull InputStreamSupplier inputStreamSupplier;
    /** Additional blob metadata we may want to add */
    private Map<String, String> additionalBlobMetadata;

    /** Max execution time in ms. Null value implies default. */
    private final @Nullable Integer clientTimeoutInMs;

    /**
     * Initial input stream used to verify that subsequent calls to inputStreamSupplier.get() do not
     * return the same
     */
    private final @Nullable InputStream firstInputStream;

    /** Encapsulates the length of content to write to remote storage. */
    private final long contentLengthToWrite;

    /**
     * Constructor for a PutRemoteObjectSpec.
     * @param bucketName Bucket name of the location to store the object in the remote storage platform.
     * @param filePath File path of the object to store.
     * @param contentsSupplier Supplier of a stream of data to write to remote storage.
     * @param contentLength Size of the contents stream.
     * @param clientTimeoutInMs Timeout in ms for the client.
     * @throws IOException
     */
    public WriteObjectSpec(
            String bucketName,
            String filePath,
            InputStreamSupplier contentsSupplier,
            long contentLength,
            @Nullable Integer clientTimeoutInMs,
            Map<String, String> additionalBlobMetadata) throws IOException {
        validateString(filePath, "objectName");
        if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength must be non-negative.");
        }
        if (null == contentsSupplier) {
            throw new IllegalArgumentException("content supplier must not be null.");
        }
        if (clientTimeoutInMs != null && clientTimeoutInMs < 0) {
            throw new IllegalArgumentException("client timeout must not be > 0.");
        }
        InputStream inputStream = null;
        try {
            inputStream = getInputStreamHelper(contentsSupplier);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        this.bucketName = bucketName;
        this.filePath = filePath;
        this.inputStreamSupplier = contentsSupplier;
        this.firstInputStream = inputStream;
        this.contentLengthToWrite = contentLength;
        this.additionalBlobMetadata = additionalBlobMetadata;
        this.clientTimeoutInMs = clientTimeoutInMs;
    }

    /**
     * Get the bucket name.
     * @return Name of the bucket.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Get the file path.
     * @return The path of the file.
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * Get the input stream supplier.
     * @return The input stream supplier.
     */
    public InputStreamSupplier getInputStreamSupplier() {
        return inputStreamSupplier;
    }

    /**
     * Get the additional metadata for the object.
     * @return A map representing additional metadata.
     */
    public Map<String, String> getAdditionalBlobMetadata() {
        return additionalBlobMetadata;
    }

    /**
     * Get the timeout.
     * @return Timeout in millisecond.
     */
    public Integer getClientTimeoutInMs() {
        return clientTimeoutInMs;
    }

    /**
     * Get the first input stream.
     * @return The input stream.
     */
    public InputStream getFirstInputStream() {
        return firstInputStream;
    }

    /**
     * Get the content length to write.
     * @return The length of the content to write.
     */
    public long getContentLengthToWrite() {
        return contentLengthToWrite;
    }

    /**
     * Helper that validates that the given String parameter is not null, empty, or all whitespace.
     *
     * @param toValidate The string arg to validate.
     * @param argName The name of the arg to include in the error message.
     */
    private void validateString(String toValidate, String argName) {
        if (toValidate.isBlank()) {
            throw new IllegalArgumentException(argName + " may not be blank.");
        }
    }

    /**
     * Helper method to get and validate an inputstream from the stream supplier. Validates that the
     * returned stream is not null.
     *
     * @param streamSupplier - the stream supplier
     * @return - a 'valid' inputstream
     * @throws IOException - if the call to streamSupplier.get() throws one
     */
    private static @NotNull InputStream getInputStreamHelper(
            @NotNull InputStreamSupplier streamSupplier) throws IOException {
        InputStream inputStream = streamSupplier.get();
        if (inputStream == null) {
            throw new RuntimeException("Input stream for is null");
        }
        return inputStream;
    }

    /**
     * Get the input stream representing the contents to write. - Ensure that we're not getting the
     * same input stream as in an earlier call. That will cause a number of problems downstream. - If
     * the call to inputStreamSupplier.get() results in an IOException, the exception is bubbled back
     * to the caller.
     *
     * @return the inputstream
     * @throws IOException if the call to inputStreamSupplier.get() throws one
     */
    @NotNull
    InputStream getInputStream() throws IOException {
        InputStream is = getInputStreamHelper(this.inputStreamSupplier);
        // Ensure that we're not reusing a stream. We're comparing the stream
        // we got from the most recent call to the stream supplier with the stream
        // we got when we constructed this (WriteRemoteObjectSpec) object. And
        // we use reference equality for comparison.
        if (is == this.firstInputStream) {
            throw new RuntimeException("Input stream should not be re-used.");
        }
        return is;
    }

    /**
     * Functional interface: A Supplier of an InputStream.
     *
     * <p>IMPORTANT: Each call to get() should return a different InputStream.
     */
    @FunctionalInterface
    public interface InputStreamSupplier {
        public @NotNull InputStream get() throws IOException;
    }

}
