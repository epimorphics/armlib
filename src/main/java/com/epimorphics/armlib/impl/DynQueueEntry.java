/******************************************************************
 * File:        DynQueueEntry.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an in-progress or completed queue entry, including enough information to reconstruct
 * the original request, in a form suitable for use with AWS DynamoDB.
 */
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

    public StatusFlag getStatus() {
        return statusStr == null ? StatusFlag.Pending : StatusFlag.valueOf(statusStr);
    }

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

    public BatchRequest getBatchRequest() {
        BatchRequest req = new BatchRequest(requestURI, parameters, sticky);
        req.setKey(key);
        req.setEstimatedTime(estimatedTime);
        return req;
    }

    public void setStarted() {
        this.started = System.currentTimeMillis();
        this.statusStr = StatusFlag.InProgress.name();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRequestURI() {
        return requestURI;
    }

    public void setRequestURI(String requestURI) {
        this.requestURI = requestURI;
    }

    public String getParameters() {
        return parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public Long getEstimatedTime() {
        return estimatedTime;
    }

    public void setEstimatedTime(Long estimatedTime) {
        this.estimatedTime = estimatedTime;
    }

    public Boolean isSticky() {
        return sticky;
    }

    public void setSticky(Boolean sticky) {
        this.sticky = sticky;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public String getStatusStr() {
        return statusStr;
    }

    public void setStatusStr(String statusStr) {
        this.statusStr = statusStr;
    }

    public Long getStarted() {
        return started;
    }

    public void setStarted(Long started) {
        this.started = started;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Map<String, AttributeValue> toItemMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Key", AttributeValue.builder().s(key).build());
        item.put("RequestURI", AttributeValue.builder().s(requestURI).build());
        item.put("Parameters", AttributeValue.builder().s(parameters).build());
        if (estimatedTime != null) item.put("EstimatedTime", AttributeValue.builder().n(estimatedTime.toString()).build());
        if (sticky != null) item.put("Sticky", AttributeValue.builder().bool(sticky).build());
        if (created != null) item.put("Created", AttributeValue.builder().n(created.toString()).build());
        if (statusStr != null) item.put("Status", AttributeValue.builder().s(statusStr).build());
        if (started != null) item.put("Started", AttributeValue.builder().n(started.toString()).build());
        if (version != null) item.put("Version", AttributeValue.builder().n(version.toString()).build());
        return item;
    }

    public static DynQueueEntry fromItemMap(Map<String, AttributeValue> item) {
        DynQueueEntry entry = new DynQueueEntry();
        entry.setKey(item.get("Key").s());
        entry.setRequestURI(item.get("RequestURI").s());
        entry.setParameters(item.get("Parameters").s());
        if (item.containsKey("EstimatedTime")) entry.setEstimatedTime(Long.valueOf(item.get("EstimatedTime").n()));
        if (item.containsKey("Sticky")) entry.setSticky(item.get("Sticky").bool());
        if (item.containsKey("Created")) entry.setCreated(Long.valueOf(item.get("Created").n()));
        if (item.containsKey("Status")) entry.setStatusStr(item.get("Status").s());
        if (item.containsKey("Started")) entry.setStarted(Long.valueOf(item.get("Started").n()));
        if (item.containsKey("Version")) entry.setVersion(Integer.valueOf(item.get("Version").n()));
        return entry;
    }

    @Override
    public int compareTo(DynQueueEntry other) {
        return created.compareTo(other.created);
    }
}
