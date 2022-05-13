## **Snowflake S3Compat API Test Suite.**

This test suite tests necessary s3compat API's and measure simple performance stats.

For any issue please contact @github/sfc-gh-schen or shannon.chen@snowflake.com

Prerequisites
=============
This test suite requires Java 1.8 or higher, and is built using Maven.

Installation
============
Build from Source Code 
----------------------
1. Checkout source code from Github by running:
```bash
git clone git@github.com:snowflakedb/snowflake-s3compat-api-test-suite.git
```

2. Build the test suite by running:
```bash
cd snowflake-s3compat-api-test-suite && mvn clean install -DskipTests
```

Variables needed for running tests
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
example to set  environment variables:
```bash
export REGION_1=<regtion_1_for_bucket_1>

```

The test suite accept environment variables or CLI arguments.

Usage
=====
navigate to target folder
-------------------------
```bash
cd s3compatapi
``` 

Test a specific API
-------------------
```bash
mvn test -Dtest=S3CompatApiTest#getBucketLocation
```

Test all APIs using already setup evnvironment variables
------------------------------------------------------
```bash
mvn test -Dtest=S3CompatApiTest
```
Note that run all tests may take more than 2 min as one putOjbect test is testing uploading file upto 5GB.

Test all APIs using CLI variables (if evnvironment variables set setup yet)
---------------------------------------------------------------------------
```bash
mvn test -Dtest=S3CompatApiTest -DREGION_1=us-east-1 -DREGION_1=us-west-2
```

Collect performance stats
--------------------------
collect perf stats by default: all API's run 20 times
```bash
java -jar target/java -jar target/snowflake-s3compat-api-tests-1.0-SNAPSHOT.jar
```
collect perf stats by passing arguments: arg1= a list of APIs separated by comma, arg2=times to run the API's.
```bash
java -jar target/snowflake-s3compat-api-tests-1.0-SNAPSHOT.jar -a getObject,putOjbect -b 10
```
Above command indicates to collect perf stats for 10 times of getObject and putOjbect.

Visualize the performance stats
-------------------------------
(use below ui to open the generated .tsdb2 file)
```bash
java -jar ../spf4jui/target/dependency-jars/spf4j-ui-8.9.5.jar
```
 
The generated perf data
-------------------
Perf data is generated and stored in .tsdb2 as binary;

perf data is also stored in .txt file for other processing if necessary.

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


