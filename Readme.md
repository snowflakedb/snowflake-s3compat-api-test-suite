Snowflake S3Compat API Test Suite.
**********************************

This is a test suite for S3Compat API used in Snowflake.

For any issue please contact shannon.chen@snowflake.com

Prerequisites
=============
This test suite requires Java 1.8 or higher, and is built using Maven.

Installation
============
Build from Source Code 
----------------------
1. Checkout source code from Github by running:

.. code-block:: bash

    git clone https://github.com/snowflakedb/snowflake-s3compat-api-test-suite.git

2. Build the test suite by running:

.. code-block:: bash

    mvn clean install -DskipTests

Vriables needed for running tests
=================================
```
         [variables]                    [description]
       REGION_1                   region for the first bucket, us-east-1
       REGION_2                   region for the second bucket, us-west-2
       BUCKET_NAME_1              the first bucket name for testing, locate at REGION_1
       BUCKET_NAME_2              the second bucket name for testing, locate at REGION_2
       S3COMPAT_ACCESS_KEY        Access key to used to authenticate the request to above both buckets.
       S3COMPAT_SECRET_KEY        Secret key to used to authenticate the request to above both buckets.
       END_POINT                  end point for the test suite
       NOT_ACCESSIBLE_BUCKET      a bucket that is not accessible by the provided keys, in REGION_1
       PAGE_LISTING_TOTAL_SIZE    page listing total size
       PREFIX_FOR_PAGE_LISTING    the prefix for testing page listing
```
The test suite accept envrionment variables or cli arguments.

Usage
=====
Test all Apis using already setup evnrionment variables
------------------------------------------------------
mvn test -Dtest=S3CompatApiTest

Test all Apis using cli variables
---------------------------------
mvn test -Dtest=S3CompatApiTest -DREGION_1=us-east-1 -DREGION_1=us-west-2

Test a specific api
-------------------
mvn test -Dtest=S3CompatApiTest#getBucketLocation

Collect performance stats
--------------------------
mvn exec:java -Dexec.mainClass=com.snowflake.s3compatapitestsuite.perf.PerfStatsApp

Visualize the performance stats
-------------------------------
java -jar target/dependency-jar/spf4j-ui-8.9.5.jar 
(use above ui to open the generated .tsdb2 file)

## Private repositories
We will make it public after implementation, security, legal review is done.

## List of S3Compat APIs
Below is the list of APIs called in this repo:
```
getBucketLocation
getObject
getObjectMetadata
putObject
listObjectsV2
listVersions
deleteObject
deleteObjects
copyObject
setRegion
generatePresignedUrl
```


