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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
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
    public static final String QUEUE_TABLE_BASE = "Queue";
    public static final String COMPLETED_TABLE_BASE = "Completed";
    public static final String COMPLETED_TIME_INDEX = "CompletedIndexByTime";

    public static final String KEY_ATTRIBUTE = "Key";
    public static final String STATUS_ATTRIBUTE = "Status";
    public static final String FINISHED_ATTRIBUTE = "Finished";
    
    static Logger log = LoggerFactory.getLogger( DynQueueManager.class );
    
    protected long checkInterval = 1000;
    protected String localTestEndpoint;
    
    public String tablePrefix = "";
    
    protected AmazonDynamoDB client;
    protected DynamoDB dynamoDB;
    protected DynamoDBMapper mapper;
    protected Regions region = Regions.EU_WEST_1;
    
    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }
    
    public void setLocalTestEndpoint(String endpoint) {
        this.localTestEndpoint = endpoint;
    }
    
    public void setTablePrefix(String prefix) {
        tablePrefix = prefix;
    }
    
    public String getQueueTableName() {
        return tablePrefix + QUEUE_TABLE_BASE;
    }
    
    public String getCompletedTableName() {
        return tablePrefix + COMPLETED_TABLE_BASE;
    }
    
    @Override
    public void startup(App app) {
        super.startup(app);
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        if (localTestEndpoint != null) {
            builder.setEndpointConfiguration(
                    new EndpointConfiguration(localTestEndpoint, region.getName()) );
        } else {
            builder.setRegion( region.getName() );
        }
        client = builder.build();
        dynamoDB = new DynamoDB(client);
        DynamoDBMapperConfig.Builder configBuilder =
                new DynamoDBMapperConfig.Builder()
                    .withConsistentReads( ConsistentReads.CONSISTENT );
        if ( ! tablePrefix.isEmpty() ) {
            configBuilder.setTableNameOverride( TableNameOverride.withTableNamePrefix(tablePrefix) );
        }
        mapper = new DynamoDBMapper(client, configBuilder.build());
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
            if (tablename.equals( getQueueTableName() )) hasInQueue = true;
            if (tablename.equals( getCompletedTableName() )) hasProcessingQueue = true;
        }
        
        List<Table> newTables = new ArrayList<>();
        if (!hasInQueue) {
            log.info("Creating " + getQueueTableName() + " table");
            CreateTableRequest req = mapper.generateCreateTableRequest(DynQueueEntry.class);
            req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
            newTables.add( dynamoDB.createTable(req) );
        }
        
        if (!hasProcessingQueue) {
            log.info("Creating " + getCompletedTableName() + " table");
            GlobalSecondaryIndex index = new GlobalSecondaryIndex()
                    .withIndexName(COMPLETED_TIME_INDEX)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(3L)
                            .withWriteCapacityUnits(1L))
                    .withKeySchema(Arrays.asList( 
                                    new KeySchemaElement(STATUS_ATTRIBUTE, KeyType.HASH),
                                    new KeySchemaElement(FINISHED_ATTRIBUTE, KeyType.RANGE)
                            ) )
                    .withProjection(new Projection().withProjectionType(ProjectionType.INCLUDE).withNonKeyAttributes(KEY_ATTRIBUTE));
            CreateTableRequest req = mapper.generateCreateTableRequest(DynCompletedEntry.class)
                    .withAttributeDefinitions(Arrays.asList(
                            new AttributeDefinition(STATUS_ATTRIBUTE, ScalarAttributeType.S),
                            new AttributeDefinition(FINISHED_ATTRIBUTE, ScalarAttributeType.N),
                            new AttributeDefinition(KEY_ATTRIBUTE, ScalarAttributeType.S)
                            ))
                    .withGlobalSecondaryIndexes(index)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
            newTables.add( dynamoDB.createTable(req) );
        }
        for (Table table : newTables) {
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
        List<DynQueueEntry> entries = new ArrayList<>( 
                mapper.scan(DynQueueEntry.class, 
                        new DynamoDBScanExpression() ) );
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
                BatchRequest request = entry.getBatchRequest(); 
                try {
                    mapper.save(entry);
                } catch (ConditionalCheckFailedException e) {
                    // This entry was started by someone else after all, skip it
                }
                return request;
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

    @Override
    public void removeOldCompletedRequests(long cutoff) {
        long count = 0;
        
        while(true) {
            List<String> toDelete = listCompletedOlderThan(cutoff);
            if (toDelete.isEmpty()) break;
            count += toDelete.size();
            deleteAll(toDelete);
        }
        log.info("Cleanup deleted " + count + " old records of completed requests");
    }
    
    protected final int BATCH_SIZE = 20;  // Can't be more than 25
    
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
        Index index = dynamoDB.getTable( getCompletedTableName() ).getIndex(COMPLETED_TIME_INDEX);
        List<String> results = new ArrayList<>();
        for (Iterator<Item> i = index.query(spec).iterator(); i.hasNext();) {
            Item item = i.next();
            results.add( item.getString(KEY_ATTRIBUTE) );
        }
        return results;
    }
    
    private void deleteAll(List<String> deleteKeys) {
        TableWriteItems items = new TableWriteItems( getCompletedTableName() );
        for (String key : deleteKeys) {
            items.addPrimaryKeyToDelete( new PrimaryKey(KEY_ATTRIBUTE, key) );
        }
        try {
            dynamoDB.batchWriteItem( new BatchWriteItemSpec().withTableWriteItems(items) );
        } catch (Exception e) {
            log.error("Problem deleting old completed records", e);
        }
    }
    
    /**
     * Exposed only for testing purposes
     * @return
     */
    public AmazonDynamoDB getDynamoClient() {
        return client;
    }
    
}
    