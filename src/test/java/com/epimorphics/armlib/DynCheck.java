/******************************************************************
 * File:        DynCheck.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.impl.DynCompletedEntry;
import com.epimorphics.armlib.impl.DynQueueEntry;

/**
 * Scratch pad for experimenting with DynamoDB
 */
public class DynCheck {
    public static final String QUEUE_TABLE = "Queue";
    public static final String COMPLETED_TABLE = "Completed";
    
    static Logger log = LoggerFactory.getLogger( DynCheck.class );
    
    AmazonDynamoDBClient client;
    DynamoDB dynamoDB;
    DynamoDBMapper mapper;

    public DynCheck() {
        client = new AmazonDynamoDBClient();
        client.setEndpoint("http://localhost:8000");
        dynamoDB = new DynamoDB(client);
        mapper = new DynamoDBMapper(client);
        try {
            initDB();
        } catch (InterruptedException e) {
            log.error("DB initialization interrupted");
        }
    }
    
    public void initDB() throws InterruptedException {
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        boolean hasInQueue = false;
        boolean hasProcessingQueue = false;
        for (Iterator<Table> i = tables.iterator(); i.hasNext();) {
            String tablename = i.next().getTableName();
            if (tablename.equals(QUEUE_TABLE)) hasInQueue = true;
            if (tablename.equals(COMPLETED_TABLE)) hasProcessingQueue = true;
        }
        
        if (!hasInQueue) {
            log.info("Creating " + QUEUE_TABLE + " table");
            CreateTableRequest req = mapper.generateCreateTableRequest(DynQueueEntry.class);
            req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
            Table table = dynamoDB.createTable(req);
            table.waitForActive();            
        }
        
        if (!hasProcessingQueue) {
            log.info("Creating " + COMPLETED_TABLE + " table");
            CreateTableRequest req = mapper.generateCreateTableRequest(DynCompletedEntry.class);
            req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
            Table table = dynamoDB.createTable(req);
            table.waitForActive();            
        }
    }
    
    public BatchStatus submit(BatchRequest request) {
        DynQueueEntry entry = new DynQueueEntry(request);        
        DynCompletedEntry procEnt = mapper.load(DynCompletedEntry.class, request.getKey());
        if (procEnt == null || procEnt.getStatus() == StatusFlag.Failed) {
            if (procEnt != null) {
                mapper.delete(procEnt);
            }
            try {
                mapper.save(entry);
            } catch (ConditionalCheckFailedException e) {
                // Request already submitted between earlier check and now, so don't duplicate
                log.warn("Suppressing duplicate submit of request: " + request.getKey());
            }
            return new BatchStatus(request.getKey(), StatusFlag.Pending);
        } else {
            return procEnt.getBatchStatus();
        }
    }
    
    public List<DynQueueEntry> getRawQueue() {
        List<DynQueueEntry> entries = new ArrayList<>( mapper.scan(DynQueueEntry.class, new DynamoDBScanExpression()) );
        Collections.sort(entries);
        return entries;
    }    
    
    public List<BatchStatus> getQueue() {
        List<DynQueueEntry> entries = getRawQueue();
        List<BatchStatus> state = new ArrayList<>( entries.size() );
        for (DynQueueEntry entry : entries) {
            state.add( entry.getBatchStatus() );
        }
        return state;
    }    
    
    public BatchRequest nextRequest() {
        for (DynQueueEntry entry : getRawQueue()) {
            if (entry.getStatus() == StatusFlag.Pending) {
                entry.setStarted();
                try {
                    mapper.save(entry);
                    return entry.getBatchRequest();
                } catch (ConditionalCheckFailedException e) {
                    // This entry was started by someone else after all, skip it
                }
            }
        }
        return null;
    }
    
    public void run() {
        BatchRequest request = new BatchRequest("http://localhost", "p=foo&q=bar");
        request.setKey("Request1");
        submit(request);
        
        request = new BatchRequest("http://localhost", "p=foo2&q=bar2");
        request.setKey("Request2");
        submit(request);

        System.out.println("Queue = ");
        for (BatchStatus status : getQueue()) {
            System.out.println("  " + status.getKey() + " - " + status.getStatus());
        }
    
        showNextRequest();
        showNextRequest();

        System.out.println("Queue = ");
        for (BatchStatus status : getQueue()) {
            System.out.println("  " + status.getKey() + " - " + status.getStatus());
        }
    }
    
    private void showNextRequest() {
        BatchRequest request = nextRequest();
        System.out.println( request == null ? "No more requests" : "Next request is " + request.getKey());
    }
    
    public static void main(String[] args) {
        new DynCheck().run();
    }
}
