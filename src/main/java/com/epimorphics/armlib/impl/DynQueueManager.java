/******************************************************************
 * File:        MemQueueManager.java
 * Created by:  Dave Reynolds
 * Created on:  15 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

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
import com.epimorphics.appbase.core.App;
import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.appbase.core.Startup;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.QueueManager;

/**
 * Non-persistent, non-distributed, implementation of queue manager. Only 
 * useful for test/development.
 */
public class DynQueueManager extends ComponentBase implements QueueManager, Startup {
    public static final String QUEUE_TABLE = "Queue";
    public static final String COMPLETED_TABLE = "Completed";
    
    static Logger log = LoggerFactory.getLogger( DynQueueManager.class );
    
    protected long checkInterval = 1000;
    protected String localTestEndpoint;
    
    protected AmazonDynamoDBClient client;
    protected DynamoDB dynamoDB;
    protected DynamoDBMapper mapper;
    
    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }
    
    public void setLocalTestEndpoint(String endpoint) {
        this.localTestEndpoint = endpoint;
    }
    
    @Override
    public void startup(App app) {
        super.startup(app);
        client = new AmazonDynamoDBClient();
        if (localTestEndpoint != null) {
            client.setEndpoint("http://localhost:8000");
        }
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
    
    @Override
    public BatchStatus submit(BatchRequest request) {
        DynQueueEntry entry = find(request.getKey());
        if (entry == null || entry.getStatus() == StatusFlag.Failed) {
            if (entry != null) {
                try {
                    mapper.delete(entry);
                } catch (ConditionalCheckFailedException e) {
                    log.warn("Suppressing duplicate submit of request: " + request.getKey());
                }
            }
            entry = new DynQueueEntry(request);  
            try {
                mapper.save(entry);
            } catch (ConditionalCheckFailedException e) {
                log.warn("Suppressing duplicate submit of request: " + request.getKey());
            }
            return new BatchStatus(request.getKey(), StatusFlag.Pending);
        } else {
            return entry.getBatchStatus();
        }
    }
    
    public List<DynQueueEntry> getRawQueue() {
        List<DynQueueEntry> entries = new ArrayList<>( mapper.scan(DynQueueEntry.class, new DynamoDBScanExpression()) );
        Collections.sort(entries);
        return entries;
    }    
    
    @Override
    public List<BatchStatus> getQueue() {
        List<DynQueueEntry> entries = getRawQueue();
        List<BatchStatus> state = new ArrayList<>( entries.size() );
        for (DynQueueEntry entry : entries) {
            state.add( entry.getBatchStatus() );
        }
        return state;
    }    
    
    @Override
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

    @Override
    public BatchRequest nextRequest(long timeout) throws InterruptedException {
        long count = 0;
        BatchRequest next = nextRequest();
        if (next != null) return next;
        
        while (count < timeout) {
            long sleep = Math.min(checkInterval, timeout - count);
            Thread.sleep( sleep );
            count += sleep;
            
            next = nextRequest();
            if (next != null) return next;
        }
        return null;
    }
    
    @Override
    public BatchStatus getStatus(String requestKey) {
        DynQueueEntry entry = find(requestKey);
        if (entry != null) {
            return entry.getBatchStatus();
        } else {
            return new BatchStatus(requestKey, StatusFlag.Unknown);
        }
    }

    private DynQueueEntry find(String requestKey) {
        DynQueueEntry entry = mapper.load(DynQueueEntry.class, requestKey);
        if (entry == null){
            entry = mapper.load(DynCompletedEntry.class, requestKey);
        }
        return entry;
    }
    
    @Override
    public BatchRequest findRequest(String key) {
        DynQueueEntry entry = find(key);
        if (entry == null) {
            return null;
        } else {
            return entry.getBatchRequest();
        }
    }
    
    @Override
    public void finishRequest(String key) {
        DynQueueEntry entry = mapper.load(DynQueueEntry.class, key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as finished: " + key);
        } else {
            mapper.delete(entry);
            DynCompletedEntry completed = new DynCompletedEntry(entry);
            mapper.save(completed);
        }
    }

    @Override
    public void abortRequest(String key) {
        DynQueueEntry entry = mapper.load(DynQueueEntry.class, key);
        if (entry == null) {
            log.error("Request has been lost, can't abort: " + key);
        } else {
            entry.setStatusStr( StatusFlag.Pending.name() );
            entry.setStarted(null);
            mapper.save(entry);
        }
    }

    @Override
    public void failRequest(String key) {
        DynQueueEntry entry = mapper.load(DynQueueEntry.class, key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as failed: " + key);
        } else {
            mapper.delete(entry);
            DynCompletedEntry completed = new DynCompletedEntry(entry);
            completed.setStatusStr(StatusFlag.Failed.name());
            mapper.save(completed);
        }
    }
    
    @Override
    public BatchStatus resubmit(BatchRequest request) {
        DynCompletedEntry entry = mapper.load(DynCompletedEntry.class, request.getKey());
        if (entry != null) {
            // Clear out any old copy
            mapper.delete(entry);
        }
        return submit(request);
    }

}
    