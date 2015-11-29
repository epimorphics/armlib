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
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.epimorphics.armlib.BatchStatus.StatusFlag;

/**
 * Represents an in-progress or completed queue entry, including enough information to reconstruct
 * the original request, in a form suitable for use with AWS DynamoDB
 */
@DynamoDBTable(tableName="Completed")
public class DynCompletedEntry extends DynQueueEntry {
    protected Long finished;
    
    public DynCompletedEntry() {
    }
    
    public DynCompletedEntry(DynQueueEntry entry) {
        requestURI    = entry.requestURI;
        parameters    = entry.parameters;
        key           = entry.key;
        estimatedTime = entry.estimatedTime;
        sticky        = entry.sticky;
        created       = entry.created;
        statusStr     = StatusFlag.Completed.name();
        started       = entry.started;
        finished      = System.currentTimeMillis();
    }
    
    @DynamoDBAttribute(attributeName="Finished") 
    public Long getFinished() {
        return finished;
    }

    public void setFinished(Long finished) {
        this.finished = finished;
    }

    
}
