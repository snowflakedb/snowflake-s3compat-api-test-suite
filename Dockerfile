FROM maven:3.9

ENV BUCKET_NAME_1 test-accessible-bucket
ENV REGION_1 us-west-2
ENV REGION_2 us-east-1
ENV S3COMPAT_ACCESS_KEY AKIA...
ENV S3COMPAT_SECRET_KEY SECRET...
ENV END_POINT my-s3-endpoint.company.com
ENV NOT_ACCESSIBLE_BUCKET test-not-accessible-bucket
ENV PREFIX_FOR_PAGE_LISTING test/prefix/
ENV PAGE_LISTING_TOTAL_SIZE 1000

COPY . /snowflake-s3compat-api-test-suite/
WORKDIR /snowflake-s3compat-api-test-suite/
RUN --mount=type=secret,id=m2settings,dst=/root/.m2/settings.xml \ 
    mvn clean install -DskipTests

ENTRYPOINT ["mvn", "test"]
CMD ["-Dtest=S3CompatApiTest"]
