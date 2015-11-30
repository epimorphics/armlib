/******************************************************************
 * File:        DynCheck.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import static com.epimorphics.armlib.impl.DynQueueManager.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus.StatusFlag;

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
        ScanResult result = queue.client.scan(new ScanRequest()
                .withTableName(COMPLETED_TABLE)
                .withIndexName(COMPLETED_TIME_INDEX));
        System.out.println( String.format("Scanned %d returning %d", result.getScannedCount(), result.getCount()) );
        for (Map<String,AttributeValue> item : result.getItems()) {
            System.out.println(" - " + item);
        }
    }
    
    protected final int BATCH_SIZE = 5;  // Can't be more than 25
    
    private List<String> listCompletedOlderThan(long cutoff) {
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#s", STATUS_ATTRIBUTE);
        QuerySpec spec = new QuerySpec()
                .withMaxResultSize(BATCH_SIZE)
                .withKeyConditionExpression("#s = :v_status and Finished < :v_cutoff")
                        .withValueMap(new ValueMap()
                                .withString(":v_status", StatusFlag.Completed.name())
                                .withNumber(":v_cutoff", cutoff))
                        .withNameMap(nameMap);
        Index index = queue.dynamoDB.getTable(COMPLETED_TABLE).getIndex(COMPLETED_TIME_INDEX);
        List<String> results = new ArrayList<>();
        for (Iterator<Item> i = index.query(spec).iterator(); i.hasNext();) {
            Item item = i.next();
            results.add( item.getString(KEY_ATTRIBUTE) );
        }
        return results;
    }
    
    private void deleteAll(List<String> deleteKeys) {
        TableWriteItems items = new TableWriteItems(COMPLETED_TABLE);
        for (String key : deleteKeys) {
            items.addPrimaryKeyToDelete( new PrimaryKey(KEY_ATTRIBUTE, key) );
        }
        try {
            queue.dynamoDB.batchWriteItem( new BatchWriteItemSpec().withTableWriteItems(items) );
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
