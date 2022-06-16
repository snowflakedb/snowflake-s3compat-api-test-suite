package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * This is a wrapper around AmazonS3Client.
 */
public class InstrumentedAmazonS3Client extends AmazonS3Client {
    public InstrumentedAmazonS3Client(
            AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        super(awsCredentials, clientConfiguration);
    }
}
