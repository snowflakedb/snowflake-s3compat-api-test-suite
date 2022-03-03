package com.snowflake.s3compatapitestsuite.compatapi;


import com.amazonaws.services.s3.model.*;
import org.jetbrains.annotations.Nullable;
import com.amazonaws.regions.Region;
import java.util.List;

/** Interface representing StorageClient. */
public interface StorageClient {

    /**
     * Gets the geographical location where stored the specified bucket.
     * @param bucketName The name of the bucket to lookup.
     * @return The location of the bucket.
     */
    String getBucketLocation(String bucketName);

    /**
     * Get the object by providing bucket name and key.
     * @param bucketName The name of the bucket containing the desired object.
     * @param key The key in the specified bucket under which the object is stored.
     * @return An S3 object.
     */
    S3Object getObject(String bucketName, String key);

    /**
     * Get the object by providing bucket name and key.
     * @param bucketName The name of the bucket containing the desired object
     * @param key The key in the specified bucket under which the object is stored.
     * @param versionId VersionId to request metadata about a specific version
     * @return The object metadata.
     */
    RemoteObjectMetadata getObjectMetadata(String bucketName, String key, @Nullable String versionId);

    /**
     * Write an object to a remote storage location.
     * @param bucketName The name of an existing bucket, to which the new object will be uploaded.
     * @param key The key under which to store the new object.
     * @param fileName The path of the file to upload to Amazon S3.
     * @return result of the writing operation.
     */
    PutObjectResult putObject(String bucketName, String key, String fileName) ;

    /**
     * List all objects with given prefix.
     * @param bucketName Name of the bucket
     * @param prefix An optional parameter restricting the response to keys beginning with the specified prefix.
     * @return a list of summary information about the objects in the specified bucket.
     */
    List<S3ObjectSummary> listObjects(String bucketName, String prefix);

    /**
     * List objects V2 by providing bucket name and prefix of the object.
     * @param bucketName Name of the bucket
     * @param prefix An optional parameter restricting the response to keys beginning with the specified prefix.
     * @return list of objects summaries.
     */
    List<S3ObjectSummary> listObjectsV2(String bucketName, String prefix) ;

    /**
     * List all versions in a location per the list versions request
     *
     * @param bucketName Bucket name to read the object in the remote storage platform from.
     *     Corresponds to an S3 bucket.
     * @param prefix Name of the object to read. Corresponds to an S3 key or an Azure blobName.
     * @param useUrlEncoding If true, set URL encoding for S3 requests.
     * @return list of versions
     */
    List<S3VersionSummary> listVersions(String bucketName, String prefix, boolean useUrlEncoding) ;

    /**
     * Deletes the specified object in the specified bucket.
     * @param bucketName Name of the bucket that contains the object to delete.
     * @param fileKey The key of the object to delete.
     */
    void deleteObject(String bucketName, String fileKey);

    /**
     * Delete all the objects per the delete object request
     * @param bucketName the bucket name where the objects locate
     * @param toDeleteList a list of object spec (filename, versionId) to delete
     * @return The number of deleted objects.
     */
    int deleteObjects(String bucketName, List<DeleteRemoteObjectSpec> toDeleteList);

    /**
     * <p>Copy all objects from srcPath to dstPath. It will copy all objects with srcPath as prefix in
     * their key, and replace srcPath in their full path with dstPath as the destination.
     *
     * @param srcPath The key prefix of source objects to copy
     * @param dstPath The key prefix of destination objects
     */
    /**
     * Copy all objects from srcPath to dstPath. It will copy all objects with srcPath as prefix in their key,
     * and replace srcPath in their full path with dstPath as the destination.
     * @param sourceBucket Bucket name of the source object of the copy.
     * @param sourceFileName Name of the source object of the copy.
     * @param sourceFileVersionId Version id of the source object of the copy.
     * @param dstBucket Bucket name of the target of the copy.
     * @param dstFileName Name of the target of the copy.
     */
    void copyObject(String sourceBucket, String sourceFileName, @Nullable String sourceFileVersionId, String dstBucket, String dstFileName);

    /**
     * Sets the regional endpoint for this client's service calls.
     * @param region The region this client will communicate with.
     */
    void setRegion(Region region);

    /**
     * Generate a pre-signed URL
     * @param bucketName bucket name
     * @param key the file path
     * @param expiryTimeInSec expiry time in seconds
     * @param contentType Content type (e.g. "image/jpeg")
     * @return a string of presigne url.
     */
    String generatePresignedUrl(String bucketName, String key, int expiryTimeInSec, String contentType);
}