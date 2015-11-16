/******************************************************************
 * File:        QueueEntry.java
 * Created by:  Dave Reynolds
 * Created on:  15 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import java.util.Optional;

import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;

/**
 * Represents a queue batch request in simple implementations
 */
public class QueueEntry {
    protected BatchRequest request;
    protected String requestKey;
    protected BatchStatus.StatusFlag status;
    protected Optional<Long> started = Optional.empty();
    
    public QueueEntry(BatchRequest request) {
        this.request = request;
        this.requestKey = request.getKey();
        this.status = StatusFlag.Pending;
    }
    
    public void setStarted() {
        status = StatusFlag.InProgress;
        started = Optional.of( System.currentTimeMillis() );
    }
    
    public void setStatus(StatusFlag status) {
        this.status = status;
    }

    public BatchRequest getRequest() {
        return request;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public BatchStatus getStatus() {
        BatchStatus s = new BatchStatus(requestKey, status);
        if (started.isPresent()) {
            s.setStarted( started.get() );
        }
        return s;
    }
    
    public StatusFlag getStatusFlag() {
        return status;
    }
    
    public boolean isFinished() {
        return status == StatusFlag.Completed || status == StatusFlag.Failed;
    }
    
}
