## **Snowflake S3Compat API Test Suite**

This test suite tests necessary s3compat APIs and measure simple performance stats.

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

2. Build the test suite

   The dependency spf4j-ui used in this repo has a fork build, GitHub Package requires authorized access for it. See [GitHub Maven registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry).

   Add below block to your ~/.m2/settings.xml file, A read-only token is sufficient.

```
<server>
  <id>github</id>
  <username>your github username</username>
  <password>your github access token</password>
</server>
```
   Build the test suite by running:
```bash
cd snowflake-s3compat-api-test-suite && mvn clean install -DskipTests
```

Variables needed for running tests
=================================
```
         [variables]                    [description]
       BUCKET_NAME_1              The bucket name for testing, locate at REGION_1, expect versioning enabled.
       REGION_1                   Region for the above bucket, like us-east-1.
       REGION_2                   Region that is different than REGION_1, like us-west-2.
       S3COMPAT_ACCESS_KEY        Access key to used to acess to the above bucket.
       S3COMPAT_SECRET_KEY        Secret key to used to access to the above bucket.
       END_POINT                  End point that can route operations to the provided bucket.
       NOT_ACCESSIBLE_BUCKET      A bucket that is not accessible by the provided keys, at REGION_1.
       PREFIX_FOR_PAGE_LISTING    The prefix for testing listing large num of objects, at BUCKET_NAME_1, it needs to have over 1000 objects. 
       PAGE_LISTING_TOTAL_SIZE    The total size of objects on the above prefix: PREFIX_FOR_PAGE_LISTING.
```
example to set environment variables:
```bash
export REGION_1=<region_1_for_bucket_1>

```

The test suite accept environment variables or CLI arguments.

Usage
=====
navigate to target folder
```bash
cd s3compatapi
``` 

Test APIs
-------------------
Test a specific API, eg: test getBucketLocation
```bash
mvn test -Dtest=S3CompatApiTest#getBucketLocation
```
Test all APIs using already setup environment variables
```bash
mvn test -Dtest=S3CompatApiTest
```
Note that run all tests may take more than 2 min as one putObject test is testing uploading file upto 5GB.

Test using CLI variables (if environment variables not set setup yet)
```bash
mvn test -Dtest=S3CompatApiTest -DREGION_1=us-east-1 -DREGION_2=us-west-2 -D...
mvn test -Dtest=S3CompatApiTest#getObject -DREGION_1=us-east-1 -DREGION_2=us-west-2 -D...
```

Collect performance stats
--------------------------
Collect perf stats by default: all APIs run 20 times
```bash
java -jar target/snowflake-s3compat-api-tests-1.0-SNAPSHOT.jar
```
Collect perf stats by passing arguments: 

-a: list of APIs separated by comma; -t: how times to run the APIs.
```bash
java -jar target/snowflake-s3compat-api-tests-1.0-SNAPSHOT.jar -a getObject,putObject -t 10
```
Above command indicates to collect perf stats for 10 times of getObject and putObject.

Visualize the performance stats
-------------------------------
(use below ui to open the generated .tsdb2 file)
```bash
java -jar ../spf4jui/target/dependency-jars/spf4j-ui-8.9.5.jar
```
Open the .tsdb2 file, choose one of the API data generated, click Plot to see the charts.

Perf data is generated and stored in .tsdb2 as binary;

Perf data is also stored in .txt file for other processing if necessary.

## List of S3Compat APIs
Below is the list of APIs called in this repo:
```
getBucketLocation
getObject
getObjectMetadata
putObject
listObjectsV2
listVersions
listNextBatchOfVersions
deleteObject
deleteObjects
copyObject
generatePresignedUrl
```

## Public documentation
If all of your APIs pass the tests in this repo, please refer to our public documentations about using this feature on Snowflake deployments.

[Working With Amazon S3-compatible Storage](https://docs.snowflake.com/en/LIMITEDACCESS/tables-external-s3-compatible.html)

[Using On-Premises Data in Place with Snowflake](https://www.snowflake.com/blog/external-tables-on-prem/)

## Troubleshooting
We expect the storage vendors provide S3-compatible APIs, which should work like S3 APIs. We have observed there are still differences between storage vendors.
Below are some troubleshooting cases. When your tests fail, please refer to the source code to see what are the test cases.
1. getBucketLocation tests fail
   We call getBucketLocation() to retrieve the region for a bucket, because the region is required for SigV4. If you confirm that your service ignore the bucket location (region) in the SigV4 from the request, then you can ignore the test failures for getBucketLocation.
2. negative tests fail
   For some negative test cases, AWS S3 returns 404 or 403, while your APIs return 400 or other error code, so your tests fails as the error code is different from what the tests expect.
   It should be fine if your APIs return reasonable error codes and messages for those negative cases.

## Support
Feel free to file an issue or submit a PR here for general cases. For official support, contact Snowflake support at: https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge


