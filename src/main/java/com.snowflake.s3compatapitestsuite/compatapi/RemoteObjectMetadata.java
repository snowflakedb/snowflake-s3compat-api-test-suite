package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Class to encapsulate the metadata about an object stored in remote storage.
 */

public class RemoteObjectMetadata {

    /** Encapsulates the length of the object content. */
    private final long objectContentLength;

    /** Encapsulates the ETag assigned to the object by the storage provider. */
    private final @Nullable String objectETag;

    /** Encapsulates the S3 versionId of the object. */
    private final @Nullable String objectVersionId;

    /** Encapsulates the time of the last modification of the remote object. */
    private final @Nullable Date objectLastModified;

    /**
     * Encapsulates the S3-specific rich metadata object Needed because the ObjectMetadata object can be used as an argument for S3 copy commands, and it is
     * impractical to extract *ALL* information and reconstruct it for copies.
     */
    private final @Nullable ObjectMetadata s3FullMetadata;

    /**
     * Encapsulates the information returned from the getUserMetadata (S3). Like "x-amz-matdesc"
     */
    private final @Nullable Map<String, String> objectUserMetadata;

    /**
     /**
     * Constructor for a RemoteObjectMetadata with no S3-specific metadata.
     *
     * @param contentLength Size of the object contents.
     * @param eTag ETag assigned to the object by the storage provider.
     * @param versionId VersionId (S3) of the object.
     * @param lastModified Time of last object modification in remote storage.
     */
    RemoteObjectMetadata(long contentLength, String eTag, String versionId, Date lastModified) {
        this(contentLength, eTag, versionId, lastModified, null, null);
    }

    /**
     * Constructor for a RemoteObjectMetadata with S3-specific metadata.
     *
     * @param contentLength Size of the object contents.
     * @param eTag ETag assigned to the object by the storage provider.
     * @param versionId VersionId (S3) / SnapshotId (Azure) of the object.
     * @param lastModified Time of last object modification in remote storage.
     * @param s3SpecificMetadata S3-specific metadata used for S3 copy commands.
     */
    private RemoteObjectMetadata(
            long contentLength,
            String eTag,
            String versionId,
            Date lastModified,
            Map<String, String> objectUserMetadata,
            ObjectMetadata s3SpecificMetadata) {
        this.objectContentLength = contentLength;
        this.objectETag = eTag;
        this.objectVersionId = versionId;
        this.objectLastModified = lastModified;
        if (objectUserMetadata != null) {
            this.objectUserMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.objectUserMetadata.putAll(objectUserMetadata);
        } else {
            this.objectUserMetadata = null;
        }
        this.s3FullMetadata = s3SpecificMetadata;
    }

    /**
     * Constructs a RemoteObjectMetadata from an S3 ObjectMetadata.
     *
     * @param om The S3 ObjectMetadata to pull the metadata information from.
     * @return The newly constructed RemoteObjectMetadata.
     */
    static @NotNull RemoteObjectMetadata fromS3ObjectMetadata(@NotNull ObjectMetadata om) {
        long contentLength = om.getContentLength();
        String eTag = om.getETag();
        String versionId = om.getVersionId();
        Date lastModified = om.getLastModified();
        Map<String, String> objectUserMetadata = om.getUserMetadata();

        return new RemoteObjectMetadata(contentLength, eTag, versionId, lastModified, objectUserMetadata, om);
    }


    @Override
    public String toString() {
        return String.format("ObjectMetadata: {ContentLength: %s, eTag: %s, versionId: %s, lastModified: %s}", objectContentLength, objectETag,objectVersionId, objectLastModified );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteObjectMetadata)) return false;
        RemoteObjectMetadata that = (RemoteObjectMetadata) o;
        return getObjectContentLength() == that.getObjectContentLength() && Objects.equals(getObjectETag(), that.getObjectETag()) && Objects.equals(getObjectVersionId(), that.getObjectVersionId()) && Objects.equals(getObjectLastModified(), that.getObjectLastModified()) && Objects.equals(getS3FullMetadata(), that.getS3FullMetadata()) && Objects.equals(getObjectUserMetadata(), that.getObjectUserMetadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getObjectContentLength(), getObjectETag(), getObjectVersionId(), getObjectLastModified(), getS3FullMetadata(), getObjectUserMetadata());
    }

    public long getObjectContentLength() {
        return objectContentLength;
    }

    public String getObjectETag() {
        return objectETag;
    }

    public String getObjectVersionId() {
        return objectVersionId;
    }

    public Date getObjectLastModified() {
        return objectLastModified;
    }

    public ObjectMetadata getS3FullMetadata() {
        return s3FullMetadata;
    }

    public Map<String, String> getObjectUserMetadata() {
        return objectUserMetadata;
    }
}
