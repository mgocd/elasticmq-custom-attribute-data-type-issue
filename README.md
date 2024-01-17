1. Run `./gradlew test -i`.
2. Observe the error message (`com.amazonaws.AmazonClientException: MD5 returned by SQS does not match the calculation on the original request`).
3. Either downgrade AWS Java SDK version to 1.12.583 or change `.withDataType("Number.java.lang.Long")` line in `AmazonSqsAsyncTest` to `.withDataType("Number")`, and run the test again to see it succeeds.