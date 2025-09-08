/******************************************************************
 * File:        DynQueueEntry.java
 * Created by:  Dave Reynolds
 * Created on:  29 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import com.epimorphics.armlib.BatchStatus.StatusFlag;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

/**
 * Represents an in-progress or completed queue entry, including enough information to reconstruct
 * the original request, in a form suitable for use with AWS DynamoDB
 */
public class DynCompletedEntry extends DynQueueEntry {
    protected Long finished;

    public DynCompletedEntry() {
    }

    public DynCompletedEntry(DynQueueEntry entry) {
        super();
        this.setRequestURI(entry.getRequestURI());
        this.setParameters(entry.getParameters());
        this.setKey(entry.getKey());
        this.setEstimatedTime(entry.getEstimatedTime());
        this.setSticky(entry.isSticky());
        this.setCreated(entry.getCreated());
        this.setStatusStr(entry.getStatusStr());
        this.setStarted(entry.getStarted());
        this.finished = System.currentTimeMillis();
    }

    public Long getFinished() {
        return finished;
    }

    public void setFinished(Long finished) {
        this.finished = finished;
    }

    @Override
    public Map<String, AttributeValue> toItemMap() {
        Map<String, AttributeValue> item = super.toItemMap();
        if (finished != null) {
            item.put("Finished", AttributeValue.builder().n(finished.toString()).build());
        }
        return item;
    }

    public static DynCompletedEntry fromItemMap(Map<String, AttributeValue> item) {
        DynCompletedEntry entry = new DynCompletedEntry(DynQueueEntry.fromItemMap(item));
        if (item.containsKey("Finished")) {
            entry.setFinished(Long.valueOf(item.get("Finished").n()));
        }
        return entry;
    }
}