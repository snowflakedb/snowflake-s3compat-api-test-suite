/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.perfStats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Stopwatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** A class encapsulates stats of an operation request. */
public class OperationStat {
    private OPERATIONS operation;
    private long timeTakenMs;
    private String bucketName;
    private @Nullable String fileKey;
    /** The size of the content. If it is a file, then it is the content length,
     * if it is a list operation, then it is the list size */
    private @Nullable Long contentLengthOrListSize;
    private ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Constructor to build an OperationStat.
     * @param builder Builder to build the stat.
     */
    public OperationStat(Builder builder) {
        this.bucketName = builder.bucketName;
        this.fileKey = builder.fileKey;
        this.contentLengthOrListSize = builder.contentLengthOrListSize;
        this.timeTakenMs = builder.timeTakenMs;
        this.operation = builder.operation;
    }
    /**
     * Get the request of the operation.
     * @return The operation for this request.
     */
    public OPERATIONS getOperation() {
        return operation;
    }
    /**
     * Get the time taken in ms for the request.
     * @return time in millisecond.
     */
    public long getTimeTakenMs() {
        return timeTakenMs;
    }
    /**
     * Get the bucket name.
     * @return name of the bucket.
     */
    public String getBucketName() {
        return bucketName;
    }
    /**
     * Get the file path.
     * @return The path for the file.
     */
    public String getFileKey() {
        return fileKey;
    }
    /**
     * Get the content length or list size.
     * @return Size of the file content or list.
     */
    public Long getContentLengthOrListSize() {
        return contentLengthOrListSize;
    }
    /**
     * Flush the operation stat, which is to write the stat to a target file.
     * @param fileName The name of the target file to write the stat.
     */
    public void flush(String fileName)  {
        try (FileWriter fw = new FileWriter(fileName, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            ObjectWriter ow = objectMapper.writer().withDefaultPrettyPrinter();
            String json = ow.writeValueAsString(this);
            bw.append(json).append("\n");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Flush failed with JsonProcessingException: " + e);
        } catch (IOException e) {
            throw new RuntimeException("Flush failed with IOException: " + e);
        }
    }
    /**
     * Builder for collecting stats for a request.
     */
    public static class Builder implements AutoCloseable {
        private String bucketName;
        private @Nullable String fileKey;
        private @Nullable Long contentLengthOrListSize;
        private OPERATIONS operation;
        /** Start time (ms) */
        private final @NotNull Stopwatch stopwatch;
        /** Total time taken (ms) */
        private long timeTakenMs;
        @Override
        public void close() {
            stopwatch.stop();
            timeTakenMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
        /**
         * Instance of the builder.
         * @return An instance for the builder.
         */
        public static Builder newInstance() {
            return new Builder();
        }
        /**
         * Constructor for the builder.
         */
        private Builder() {
            stopwatch = Stopwatch.createStarted();
        }
        public @NotNull Builder withBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }
        public @NotNull Builder withFileKey(String fileKey) {
            this.fileKey = fileKey;
            return this;
        }
        public @NotNull Builder withContentLengthOrListSize(long contentLength) {
            this.contentLengthOrListSize = contentLength;
            return this;
        }
        public @NotNull Builder withOperation(OPERATIONS op) {
            this.operation = op;
            return this;
        }
        public @NotNull OperationStat build() {
            return new OperationStat(this);
        }
    }
    /**
     * Operations run in this suite.
     */
    public enum OPERATIONS {
        GET_BUCKET_LOCATION,
        GET_OBJECT,
        GET_OBJECT_METADATA,
        PUT_OBJECT,
        LIST_OBJECTS,
        LIST_OBJECTS_V2,
        LIST_VERSIONS,
        DELETE_OBJECT,
        DELETE_OBJECTS,
        COPY_OBJECT,
        SET_REGION,
        GENERATE_PRESIGNED_URL;
    }
}
