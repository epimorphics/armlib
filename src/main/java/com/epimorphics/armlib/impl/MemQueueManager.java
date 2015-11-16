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

import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.QueueManager;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.util.EpiException;

/**
 * Non-persistent, non-distributed, implementation of queue manager. Only 
 * useful for test/development.
 */
public class MemQueueManager extends ComponentBase implements QueueManager {
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
        if (entry == null) {
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
    public synchronized BatchRequest nextRequest(long timeout) {
        QueueEntry entry = findNext();
        if (entry == null) {
            long count = 0;
            while (entry == null && count < timeout) {
                long sleep = Math.min(checkInterval, timeout - count);
                try {
                    Thread.sleep( sleep );
                } catch (InterruptedException e) {
                    return null;
                }
                count += sleep;
                entry = findNext();
            }
        }
        if (entry == null) {
            return null;
        } else {
            entry.setStarted();
            return entry.getRequest();
        }
    }

    private QueueEntry findNext() {
        for (QueueEntry entry : queue) {
            if (entry.status == StatusFlag.Pending) {
                return entry;
            }
        }
        return null;
    }
    
    @Override
    public synchronized void finishRequest(String key) {
        QueueEntry entry = getEntry(key);
        if (entry == null) {
            throw new EpiException("Request has been lost, can mark as finished");
        }
        entry.setStatus( StatusFlag.Completed );
        queue.remove(entry);
        completed.add(entry);
    }

    @Override
    public synchronized void abortRequest(String key) {
        getEntry(key).setStatus( StatusFlag.Pending );
    }

    @Override
    public synchronized void failRequest(String key) {
        QueueEntry entry = getEntry(key);
        entry.setStatus( StatusFlag.Failed );
        queue.remove(entry);
        completed.add(entry);
    }
    
    private QueueEntry getEntry(String key) {
        QueueEntry entry = index.get(key);
        if (entry == null) {
            throw new EpiException("Could not locate request: " + key);
        }
        return entry;
    }
}
    