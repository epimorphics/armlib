/******************************************************************
 * File:        MemQueueManager.java
 * Created by:  Dave Reynolds
 * Created on:  15 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import com.epimorphics.appbase.core.App;
import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.appbase.core.Startup;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

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
    
    protected DynamoDbClient client;
    protected DynamoDbEnhancedClient enhancedClient;
//    protected DynamoDb dynamoDB;
//    protected DynamoDBMapper mapper;
    protected Region region = Region.EU_WEST_1;
    
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
        DynamoDbClientBuilder builder = DynamoDbClient.builder();
        if (localTestEndpoint != null) {
            builder.endpointOverride(URI.create(localTestEndpoint));
        }
        builder.region(region);
        client = builder.build();
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(client)
                .build();
        initDB();
    }

    private void initDB() {
        ListTablesResponse tables = client.listTables();

        Set<String> existingTables = new HashSet<>(tables.tableNames());

        if (!existingTables.contains(getQueueTableName())) {
            createQueueTable();
        }

        if (!existingTables.contains(getCompletedTableName())) {
            createCompletedTable();
        }
    }

    private void createQueueTable() {
        log.info("Creating table: {}", getQueueTableName());
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(getQueueTableName())
                .keySchema(KeySchemaElement.builder().attributeName(KEY_ATTRIBUTE).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(KEY_ATTRIBUTE)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();
        client.createTable(request);
    }

    private void createCompletedTable() {
        log.info("Creating table: {}", getCompletedTableName());
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(getCompletedTableName())
                .keySchema(KeySchemaElement.builder().attributeName(KEY_ATTRIBUTE).keyType(KeyType.HASH).build())
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName(KEY_ATTRIBUTE)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(STATUS_ATTRIBUTE)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(FINISHED_ATTRIBUTE)
                                .attributeType(ScalarAttributeType.N)
                                .build()
                )
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(COMPLETED_TIME_INDEX)
                        .keySchema(
                                KeySchemaElement.builder().attributeName(STATUS_ATTRIBUTE).keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName(FINISHED_ATTRIBUTE).keyType(KeyType.RANGE).build()
                        )
                        .projection(
                                Projection.builder().projectionType(ProjectionType.INCLUDE).nonKeyAttributes(KEY_ATTRIBUTE).build()
                        )
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(3L)
                                .writeCapacityUnits(1L)
                                .build())
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();
        client.createTable(request);
    }

    @Override
    public BatchStatus submit(BatchRequest request) {
        DynQueueEntry entry = find(request.getKey());
        if (entry == null || entry.getStatus() == StatusFlag.Failed) {
            if (entry != null) delete(entry.key);
            entry = new DynQueueEntry(request);
            saveEntry(entry, getQueueTableName());
            return new BatchStatus(request.getKey(), StatusFlag.Pending);
        } else {
            return entry.getBatchStatus();
        }
    }
    public List<DynQueueEntry> getRawQueue() {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(getQueueTableName())
                .build();
        return client.scan(scanRequest)
                .items()
                .stream()
                .map(DynQueueEntry::fromItemMap)
                .collect(Collectors.toList());
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
                    saveEntry(entry, getQueueTableName());
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
        DynQueueEntry entry = findEntry(requestKey);
        if (entry == null){
            entry = findCompletedEntry(requestKey);
        }
        return entry;
    }

    private DynQueueEntry findEntry(String key) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(getQueueTableName())
                .key(Collections.singletonMap(KEY_ATTRIBUTE, AttributeValue.builder().s(key).build()))
                .build();

        Map<String, AttributeValue> item = client.getItem(request).item();
        return !item.isEmpty() ? DynQueueEntry.fromItemMap(item) : null;
    }

    private DynCompletedEntry findCompletedEntry(String key) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(getCompletedTableName())
                .key(Collections.singletonMap(KEY_ATTRIBUTE, AttributeValue.builder().s(key).build()))
                .build();

        Map<String, AttributeValue> item = client.getItem(request).item();
        return !item.isEmpty() ? DynCompletedEntry.fromItemMap(item) : null;
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
        DynQueueEntry entry = findEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as finished: " + key);
        } else {
            deleteEntry(key, getQueueTableName());
            DynCompletedEntry completed = new DynCompletedEntry(entry);
            completed.setStatusStr(StatusFlag.Completed.name());
            saveEntry(completed, getCompletedTableName());
        }
    }

    @Override
    public void abortRequest(String key) {
        DynQueueEntry entry = findEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't abort: " + key);
        } else {
            entry.setStatusStr( StatusFlag.Pending.name() );
            entry.setStarted(null);
            saveEntry(entry, getQueueTableName());
        }
    }

    @Override
    public void failRequest(String key) {
        DynQueueEntry entry = findEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as failed: {}", key);
        } else {
            deleteEntry(entry.key, getQueueTableName());
            DynCompletedEntry completed = new DynCompletedEntry(entry);
            completed.setStatusStr(StatusFlag.Failed.name());
            saveEntry(completed, getCompletedTableName());
        }
    }

    @Override
    public BatchStatus resubmit(BatchRequest request) {
        DynCompletedEntry entry = findCompletedEntry(request.getKey());
        if (entry != null) {
            // Clear out any old copy
            deleteEntry(entry.key, getCompletedTableName());
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
            for (String key : toDelete) {
                deleteEntry(key, getCompletedTableName());
            }
        }
        log.info("Cleanup deleted {} old records of completed requests", count);
    }
    
    protected final int BATCH_SIZE = 20;  // Can't be more than 25

    private List<String> listCompletedOlderThan(long cutoff) {
        QueryRequest request = QueryRequest.builder()
                .tableName(getCompletedTableName())
                .limit(BATCH_SIZE)
                .indexName(COMPLETED_TIME_INDEX)
                .expressionAttributeNames(new HashMap<String, String>() {{
                    put("#status", STATUS_ATTRIBUTE);
                    put("#finished", FINISHED_ATTRIBUTE);
                }})
                .expressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":v_status", AttributeValue.builder().s(StatusFlag.Completed.name()).build());
                    put(":v_cutoff", AttributeValue.builder().n(Long.toString(cutoff)).build());
                }})
                .keyConditionExpression("#status = :v_status AND #finished < :v_cutoff")
                .build();
        return client.query(request)
                .items()
                .stream()
                .map(item -> item.get(KEY_ATTRIBUTE).s())
                .collect(Collectors.toList());
    }

    private void saveEntry(DynQueueEntry entry, String tableName) {
        Map<String, AttributeValue> itemMap = entry.toItemMap();
        client.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(itemMap)
                .build());
    }

    private void delete(String key) {
        try {
            deleteEntry(key, getQueueTableName());
        } catch (Exception ex1) {
            try {
                deleteEntry(key, getQueueTableName());
            } catch (Exception ex2) {
                log.error("Failed to delete key {} from both {} and {} tables", key, getQueueTableName(), getCompletedTableName());
            }
        }
    }

    private void deleteEntry(String key, String tableName) {
        client.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap(KEY_ATTRIBUTE, AttributeValue.builder().s(key).build()))
                .build());
    }
    /**
     * Exposed only for testing purposes
     * @return
     */
    public DynamoDbClient getDynamoClient() {
        return client;
    }
    
}
    