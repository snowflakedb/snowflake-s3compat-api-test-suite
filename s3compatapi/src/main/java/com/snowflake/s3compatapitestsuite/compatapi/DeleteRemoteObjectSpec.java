/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class to encapsulate the parameters required to delete an object from a bucket in
 * remote storage.
 */
public class DeleteRemoteObjectSpec {
    /** Encapsulates the S3 key. */
    private final @NotNull String remoteFileName;
    /** Encapsulates the versionId. */
    private final @Nullable String remoteVersionId;
    /**
     * Constructor for a DeleteRemoteObjectSpec with no version id.
     *
     * @param fileName Name of the object to read. Corresponds to an S3 key.
     */
    DeleteRemoteObjectSpec(String fileName) {
        this(fileName, null);
    }
    /**
     * Constructor for a DeleteRemoteObjectSpec with a version id.
     *
     * @param fileName Name of the object to read. Corresponds to an S3 key.
     * @param remoteVersionId VersionId in order to request metadata about a specific version.
     */
    public DeleteRemoteObjectSpec(String fileName, @Nullable String remoteVersionId) {
        if (fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("fileName must not be null, empty, or all whitespace.");
        }
        this.remoteFileName = fileName;
        this.remoteVersionId = remoteVersionId;
    }
    /**
     * Returns the S3 key of the object of interest.
     *
     * @return The S3 key of the object of interest.
     */
    @NotNull
    String getFileName() {
        return this.remoteFileName;
    }
    /**
     * Returns the S3 versionId of the object of interest.
     *
     * @return The S3 versionId of the object of interest.
     */
    String getObjectVersionId() {
        return this.remoteVersionId;
    }
    /**
     * Converts this object into an S3 KeyVersion object.
     *
     * @return The newly constructed KeyVersion object.
     */
    @NotNull
    DeleteObjectsRequest.KeyVersion toS3KeyVersion() {
        return new DeleteObjectsRequest.KeyVersion(this.getFileName(), this.getObjectVersionId());
    }
    @Override
    public @NotNull String toString() {
        return "DeleteRemoteObjectSpec{"
                + "remoteFileName='"
                + remoteFileName
                + '\''
                + ", remoteVersionId='"
                + remoteVersionId
                + '\''
                + '}';
    }
}
