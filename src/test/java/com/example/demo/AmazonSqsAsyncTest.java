package com.example.demo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.with;

public class AmazonSqsAsyncTest {

    private static final Logger log = LoggerFactory.getLogger(AmazonSqsAsyncTest.class);

    private static final String ACCOUNT_ID = "000000000000";
    private static final String QUEUE_NAME = "test-queue";

    @Test
    void amazonSqsAsyncTest() {
        var sqsRestServer = SQSRestServerBuilder.withDynamicPort().start();
        var address = sqsRestServer.waitUntilStarted().localAddress();
        var endpoint = String.format("http://%s:%d", address.getHostName(), address.getPort());
        var amazonSqs = getAmazonSqsClient(endpoint);

        amazonSqs.createQueue(QUEUE_NAME);
        var sendMessageRequest = getSendMessageRequest(endpoint);
        amazonSqs.sendMessage(sendMessageRequest);

        with()
                .pollInterval(250, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .await()
                .until(() -> validMessageReceived(amazonSqs, endpoint));
    }

    private AmazonSQSAsync getAmazonSqsClient(String endpoint) {
        return AmazonSQSAsyncClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .build();
    }

    private SendMessageRequest getSendMessageRequest(String endpoint) {
        var request = new SendMessageRequest(
                getQueueUrl(endpoint),
                getMessageBody()
        );
        request.setMessageAttributes(Map.of(
                "timestamp", new MessageAttributeValue()
                        .withStringValue("1")
                        .withDataType("Number.java.lang.Long")
//                "binary", new MessageAttributeValue()
//                        .withBinaryValue(ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8)))
//                        .withDataType("Binary")
        ));
        return request;
    }

    private String getQueueUrl(String endpoint) {
        return String.format("%s/%s/%s", endpoint, ACCOUNT_ID, QUEUE_NAME);
    }

    private String getMessageBody() {
        return "{\"message\": \"TestMessage\"}";
    }

    private boolean validMessageReceived(AmazonSQS amazonSqs, String endpoint) {
        var receiveMessageRequest = new ReceiveMessageRequest(getQueueUrl(endpoint));
        receiveMessageRequest.setAttributeNames(List.of("All"));
        receiveMessageRequest.setMessageAttributeNames(List.of("All"));
        var received = amazonSqs.receiveMessage(receiveMessageRequest);
        log.info("Received: {}", received);
        var messages = received.getMessages();
        if (messages.isEmpty()) {
            return false;
        }
        if (messages.size() > 1) {
            throw new IllegalStateException("More than 1 message available: " + messages);
        }
        var message = messages.get(0);
        var expectedBody = getMessageBody();
        return message.getBody().equals(expectedBody);
    }


}
