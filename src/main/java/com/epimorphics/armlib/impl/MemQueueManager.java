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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.QueueManager;

/**
 * Non-persistent, non-distributed, implementation of queue manager. Only 
 * useful for test/development.
 */
public class MemQueueManager extends ComponentBase implements QueueManager {
    static Logger log = LoggerFactory.getLogger( MemQueueManager.class );
    
    protected LinkedList<QueueEntry> queue = new LinkedList<>();
    protected List<QueueEntry> completed = new LinkedList<>();
    protected Map<String, QueueEntry> index = new HashMap<>();
    protected long checkInterval = 100;
    
    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }
    
    @Override
    public synchronized BatchStatus submit(BatchRequest request) {
        QueueEntry entry = index.get(request.getKey());
        if (entry == null || entry.getStatusFlag() == StatusFlag.Failed) {
            entry = new QueueEntry(request);
            index.put(request.getKey(), entry);
            queue.add(entry);
        }
        return entry.getStatus();
    }

    
    @Override
    public synchronized BatchStatus resubmit(BatchRequest request) {
        QueueEntry entry = index.get(request.getKey());
        if (entry != null) {
            // Clear out any old copy
            queue.remove(entry);
            completed.remove(entry);
            index.remove(request.getKey());
        }
        entry = new QueueEntry(request);
        index.put(request.getKey(), entry);
        queue.add(entry);
        return entry.getStatus();
    }
    
    @Override
    public synchronized BatchStatus getStatus(String requestKey) {
        QueueEntry entry = index.get(requestKey);
        if (entry != null) {
            return entry.getStatus();
        } else {
            return new BatchStatus(requestKey, StatusFlag.Unknown);
        }
    }

    @Override
    public synchronized List<BatchStatus> getQueue() {
        List<BatchStatus> state = new ArrayList<>( queue.size() );
        for (QueueEntry entry : queue) {
            state.add( entry.getStatus() );
        }
        return state;
    }

    @Override
    public synchronized BatchRequest findRequest(String key) {
        QueueEntry entry = index.get(key);
        if (entry == null) {
            return null;
        } else {
            return entry.getRequest();
        }
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
    public synchronized BatchRequest nextRequest() {
        for (QueueEntry entry : queue) {
            if (entry.status == StatusFlag.Pending) {
                entry.setStarted();
                return entry.getRequest();
            }
        }
        return null;
    }
    
    @Override
    public synchronized void finishRequest(String key) {
        QueueEntry entry = getEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as finished: " + key);
        } else {
            entry.setStatus( StatusFlag.Completed );
            queue.remove(entry);
            completed.add(entry);
        }
    }

    @Override
    public synchronized void abortRequest(String key) {
        QueueEntry entry = getEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't abort: " + key);
        } else {
            entry.setStatus( StatusFlag.Pending );
        }
    }

    @Override
    public synchronized void failRequest(String key) {
        QueueEntry entry = getEntry(key);
        if (entry == null) {
            log.error("Request has been lost, can't mark as failed: " + key);
        } else {
            entry.setStatus( StatusFlag.Failed );
            queue.remove(entry);
            completed.add(entry);
        }
    }
    
    private QueueEntry getEntry(String key) {
        return index.get(key);
    }
}
    