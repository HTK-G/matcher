package com.example.matcher;

import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class Matcher {

    // Define queue URLs and SNS topic ARNs
    private static final String BUY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/474668409406/NvidiaBuy";
    private static final String SELL_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/474668409406/NvidiaSell";
    private static final String FULLY_MATCHED_TOPIC_ARN = "arn:aws:sns:us-east-1:474668409406:FullyMatched";
    private static final String PARTIALLY_MATCHED_TOPIC_ARN = "arn:aws:sns:us-east-1:474668409406:PartiallyMatched";

    private final SqsClient sqsClient;
    private final SnsClient snsClient;

    public Matcher() {
        this.sqsClient = SqsClient.create();
        this.snsClient = SnsClient.create();
    }

    public void matchOrders() {
        while (true) {
            System.out.println("Polling for messages...");
            
            // Receive BUY and SELL orders
            List<Message> buyOrders = receiveMessages(BUY_QUEUE_URL);
            List<Message> sellOrders = receiveMessages(SELL_QUEUE_URL);
    
            if (buyOrders.isEmpty() || sellOrders.isEmpty()) {
                System.out.println("No orders found. Retrying...");
                try {
                    Thread.sleep(5000); // Avoid aggressive polling
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
    
            System.out.println("Found BUY orders: " + buyOrders.size());
            System.out.println("Found SELL orders: " + sellOrders.size());
    
            for (Message buyOrder : buyOrders) {
                for (Message sellOrder : sellOrders) {
                    try {
                        // Parse the Message field
                        JsonObject buyOrderJson = parseMessage(buyOrder.body());
                        JsonObject sellOrderJson = parseMessage(sellOrder.body());
    
                        // Extract NumOfShares
                        int buyShares = buyOrderJson.get("NumOfShares").getAsInt();
                        int sellShares = sellOrderJson.get("NumOfShares").getAsInt();
    
                        if (buyShares == sellShares) {
                            System.out.println("Fully matched: BUY " + buyOrderJson + " with SELL " + sellOrderJson);
    
                            buyOrderJson.addProperty("Status", "MATCHED");
                            sellOrderJson.addProperty("Status", "MATCHED");
    
                            sendToSns(FULLY_MATCHED_TOPIC_ARN, buyOrderJson.toString());
                            sendToSns(FULLY_MATCHED_TOPIC_ARN, sellOrderJson.toString());
                            
                            System.out.printf("Deleting message from %s: %s%n", BUY_QUEUE_URL, buyOrder.messageId());
                            System.out.printf("Deleting message from %s: %s%n", SELL_QUEUE_URL, sellOrder.messageId());

                            deleteMessage(BUY_QUEUE_URL, buyOrder);
                            deleteMessage(SELL_QUEUE_URL, sellOrder);
    
                        } else if (buyShares < sellShares) {
                            System.out.println("Partially matched: BUY " + buyOrderJson + " fully matched, SELL partially matched.");
    
                            buyOrderJson.addProperty("Status", "MATCHED");
                            sellOrderJson.addProperty("NumOfShares", sellShares - buyShares);
                            sellOrderJson.addProperty("Status", "PROCESSING");
    
                            sendToSns(FULLY_MATCHED_TOPIC_ARN, buyOrderJson.toString());
                            sendToSns(PARTIALLY_MATCHED_TOPIC_ARN, sellOrderJson.toString());
                            
                            System.out.printf("Deleting message from %s: %s%n", BUY_QUEUE_URL, buyOrder.messageId());
                            System.out.printf("Deleting message from %s: %s%n", SELL_QUEUE_URL, sellOrder.messageId());

                            deleteMessage(BUY_QUEUE_URL, buyOrder);
                            deleteMessage(SELL_QUEUE_URL, sellOrder);
    
                        } else {
                            System.out.println("Partially matched: SELL " + sellOrderJson + " fully matched, BUY partially matched.");
    
                            sellOrderJson.addProperty("Status", "MATCHED");
                            buyOrderJson.addProperty("NumOfShares", buyShares - sellShares);
                            buyOrderJson.addProperty("Status", "PROCESSING");
    
                            sendToSns(FULLY_MATCHED_TOPIC_ARN, sellOrderJson.toString());
                            sendToSns(PARTIALLY_MATCHED_TOPIC_ARN, buyOrderJson.toString());
                            
                            System.out.printf("Deleting message from %s: %s%n", BUY_QUEUE_URL, buyOrder.messageId());
                            System.out.printf("Deleting message from %s: %s%n", SELL_QUEUE_URL, sellOrder.messageId());

                            deleteMessage(BUY_QUEUE_URL, buyOrder);
                            deleteMessage(SELL_QUEUE_URL, sellOrder);
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing orders: " + e.getMessage());
                    }
                }
            }
        }
    }
    

    private List<Message> receiveMessages(String queueUrl) {
        System.out.println("Polling messages from: " + queueUrl);
    
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributeNames("All")
                .maxNumberOfMessages(10)
                .waitTimeSeconds(5)
                .build();
    
        List<Message> messages = sqsClient.receiveMessage(request).messages();
        // System.out.println("Received messages: " + messages);
        return messages;
    }
    

    private void sendToSns(String topicArn, String message) {
        PublishRequest publishRequest = PublishRequest.builder()
                .topicArn(topicArn)
                .message(message)
                .build();

        snsClient.publish(publishRequest);
    }

    private void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();

        sqsClient.deleteMessage(deleteRequest);
    }

    private JsonObject parseMessage(String messageBody) {
        JsonObject outerJson = JsonParser.parseString(messageBody).getAsJsonObject();
        String message = outerJson.get("Message").getAsString(); // Extract the inner JSON string
        return JsonParser.parseString(message).getAsJsonObject(); // Parse the inner JSON string
    }

    public static void main(String[] args) {
        Matcher matcher = new Matcher();
        matcher.matchOrders();
    }
}
