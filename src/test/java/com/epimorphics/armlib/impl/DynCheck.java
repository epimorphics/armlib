/******************************************************************
 * File:        DynCheck.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.epimorphics.armlib.impl.DynQueueManager.*;

/**
 * Scratch pad for experimenting with DynamoDB
 */
public class DynCheck {
    DynQueueManager queue;
    
    public DynCheck() {
        queue = new DynQueueManager();
        queue.setCheckInterval(100);
        queue.setLocalTestEndpoint("http://localhost:8000");
        queue.startup(null);
    }
    
    
    public void run() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            addCompletedRequest(i);
            Thread.sleep(250);
        }
        long cutoff = System.currentTimeMillis() - 2000;
        System.out.println("Scan completed");
        scanCompleted();
        
        List<String> toDelete = null;
        do {
            toDelete = listCompletedOlderThan(cutoff);
            System.out.println("Deleting " + toDelete);
            deleteAll(toDelete);
        } while ( ! toDelete.isEmpty() );
        
        System.out.println("Final scan");
        scanCompleted();
    }
    
    private void scanCompleted() {
        ScanResponse result = queue.client.scan(ScanRequest.builder()
                .tableName(queue.getQueueTableName())
                .indexName(COMPLETED_TIME_INDEX)
                .build());
        System.out.println( String.format("Scanned %d returning %d", result.scannedCount(), result.count()) );
        for (Map<String,AttributeValue> item : result.items()) {
            System.out.println(" - " + item);
        }
    }
    
    protected final int BATCH_SIZE = 5;  // Can't be more than 25


    private List<String> listCompletedOlderThan(long cutoff) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(queue.getCompletedTableName())
                .keyConditionExpression("#s = :v_status and Finished < :v_cutoff")
                .expressionAttributeNames(new HashMap<String, String>() {{
                    put("#s", STATUS_ATTRIBUTE);
                }})
                .expressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":v_status", AttributeValue.builder().s(StatusFlag.Completed.name()).build());
                    put(":v_cutoff", AttributeValue.builder().n(String.valueOf(cutoff)).build());
                }})
                .limit(BATCH_SIZE)
                .build();

        QueryResponse queryResponse = queue.getDynamoClient().query(queryRequest);

        return queryResponse.items().stream()
                .map(item -> item.get(KEY_ATTRIBUTE).s())
                .collect(Collectors.toList());
    }

    private void deleteAll(List<String> deleteKeys) {
        List<WriteRequest> writeRequests = deleteKeys.stream()
                .map(key -> WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder()
                                .key(new HashMap<String, AttributeValue>() {{
                                    put(KEY_ATTRIBUTE, AttributeValue.builder().s(key).build());
                                }})
                                .build())
                        .build())
                .collect(Collectors.toList());

        BatchWriteItemRequest batchWriteRequest = BatchWriteItemRequest.builder()
                .requestItems(new HashMap<String, List<WriteRequest>>() {{
                    put(queue.getCompletedTableName(), writeRequests);
                }})
                .build();

        try {
            queue.getDynamoClient().batchWriteItem(batchWriteRequest);
        } catch (Exception e) {
            // TODO logging
        }
    }

    private void addCompletedRequest(int i) {
        BatchRequest request = new BatchRequest("http://localhost", "p=foo&q=bar" + i);
        String key = "request" + i;
        request.setKey(key);
        queue.submit(request);
        queue.finishRequest(key);
    }
    
    public static void main(String[] args) throws Exception {
        new DynCheck().run();
    }
}
