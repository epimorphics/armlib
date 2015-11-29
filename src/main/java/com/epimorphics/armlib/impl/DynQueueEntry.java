/******************************************************************
 * File:        DynQueueEntry.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;

/**
 * Represents an in-progress or completed queue entry, including enough information to reconstruct
 * the original request, in a form suitable for use with AWS DynamoDB
 */
@DynamoDBTable(tableName="Queue")
public class DynQueueEntry implements Comparable<DynQueueEntry> {
    protected String requestURI;
    protected String parameters;
    protected String key;
    protected Long estimatedTime;
    protected Boolean sticky;
    protected Long created;
    protected String statusStr;
    protected Long started;
    protected Integer version;
    
    public DynQueueEntry() {
    }
    
    public DynQueueEntry(BatchRequest request) {
        this.requestURI = request.getRequestURI();
        this.parameters = request.getParameterString();
        this.key = request.getKey();
        this.estimatedTime = request.getEstimatedTime();
        this.sticky = request.isSticky();
        this.created = System.currentTimeMillis();
    }
    
    @DynamoDBIgnore
    public BatchStatus.StatusFlag getStatus() {
        return statusStr == null ? StatusFlag.Pending : StatusFlag.valueOf(statusStr);
    }
    
    @DynamoDBIgnore
    public BatchStatus getBatchStatus() {
        BatchStatus s = new BatchStatus(key, getStatus());
        if (started != null) {
            s.setStarted(started);
        }
        if (estimatedTime != null) {
            s.setEstimatedTime(estimatedTime);
        }
        return s;
    }    
    
    @DynamoDBIgnore
    public BatchRequest getBatchRequest() {
        BatchRequest req = new BatchRequest(requestURI, parameters, sticky);
        req.setKey( key );
        req.setEstimatedTime(estimatedTime);
        return req;
    }
    
    @DynamoDBIgnore
    public void setStarted() {
        started = System.currentTimeMillis();
        statusStr = StatusFlag.InProgress.name();
    }

    @DynamoDBHashKey(attributeName="Key")  
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }
    
    @DynamoDBAttribute(attributeName="RequestURI") 
    public String getRequestURI() {
        return requestURI;
    }
    public void setRequestURI(String requestURI) {
        this.requestURI = requestURI;
    }
    
    @DynamoDBAttribute(attributeName="Parameters") 
    public String getParameters() {
        return parameters;
    }
    public void setParameters(String parameters) {
        this.parameters = parameters;
    }
    
    @DynamoDBAttribute(attributeName="EstimatedTime") 
    public Long getEstimatedTime() {
        return estimatedTime;
    }
    public void setEstimatedTime(Long estimatedTime) {
        this.estimatedTime = estimatedTime;
    }
    
    @DynamoDBAttribute(attributeName="Sticky")     
    public Boolean isSticky() {
        return sticky;
    }
    public void setSticky(Boolean sticky) {
        this.sticky = sticky;
    }

    @DynamoDBAttribute(attributeName="Created") 
    public Long getCreated() {
        return created;
    }
    public void setCreated(Long created) {
        this.created = created;
    }
    
    @DynamoDBAttribute(attributeName="Status") 
    public String getStatusStr() {
        return statusStr;
    }
    public void setStatusStr(String status) {
        this.statusStr = status;
    }
    
    @DynamoDBAttribute(attributeName="Started") 
    public Long getStarted() {
        return started;
    }
    public void setStarted(Long started) {
        this.started = started;
    }

    @DynamoDBVersionAttribute
    public Integer getVersion() {
        return version;
    }
    public void setVersion(Integer version) {
        this.version = version;
    }    
    
    @Override
    public int compareTo(DynQueueEntry other) {
        return created.compareTo(other.created);
    }
}
