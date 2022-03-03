package com.snowflake.s3compatapitestsuite.compatapi;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class WriteObjectSpec {
    private static final Logger logger = LogManager.getLogger(WriteObjectSpec.class);
    private final String bucketName;
    /** The file path which is relative to the bucket name. */
    private final String filePath;
    /** Encapsulates the input stream to write to remote storage. */
    private final @NotNull InputStreamSupplier inputStreamSupplier;
    /** Additional blob metadata we may want to add */
    private Map<String, String> additionalBlobMetadata;
    /**
     * Whether to grant the bucket owner full control on the written object on put requests.
     * https://aws.amazon.com/premiumsupport/knowledge-center/s3-bucket-owner-access/
     */
    private final boolean bucketOwnerFullControl;
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
     * @throws IOException
     */
    public WriteObjectSpec(
            String bucketName,
            String filePath,
            InputStreamSupplier contentsSupplier,
            long contentLength,
            boolean bucketOwnerFullControl,
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
        this.bucketOwnerFullControl = bucketOwnerFullControl;
        this.additionalBlobMetadata = additionalBlobMetadata;
        this.clientTimeoutInMs = clientTimeoutInMs;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getFilePath() {
        return filePath;
    }

    public InputStreamSupplier getInputStreamSupplier() {
        return inputStreamSupplier;
    }

    public Map<String, String> getAdditionalBlobMetadata() {
        return additionalBlobMetadata;
    }

    public boolean isBucketOwnerFullControl() {
        return bucketOwnerFullControl;
    }

    public Integer getClientTimeoutInMs() {
        return clientTimeoutInMs;
    }

    public InputStream getFirstInputStream() {
        return firstInputStream;
    }

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
     * <p>IMPORTANT: Each call to get() should return a different InputStream. This is extremely
     * important in the context of retries,
     */
    @FunctionalInterface
    public interface InputStreamSupplier {
        public @NotNull InputStream get() throws IOException;
    }

    /**
     * Functional interface to allow definition of a Supplier that throws.
     *
     * @param <T> What the supplier supplies.
     */
    @FunctionalInterface
    public interface SupplierWithRSPE<T> {
        public @NotNull T getWithRSPE(final StorageClient remoteClient)
                throws RuntimeException;
    }

}
