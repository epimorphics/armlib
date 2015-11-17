/******************************************************************
 * File:        DefaultRequestManager.java
 * Created by:  Dave Reynolds
 * Created on:  14 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import java.util.List;

import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.BatchStatus;
import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.CacheManager;
import com.epimorphics.armlib.QueueManager;
import com.epimorphics.armlib.RequestManager;

/**
 * Standard implementation of request manager.
 */
public class StandardRequestManager extends ComponentBase implements RequestManager {
    protected QueueManager queueManager;
    protected CacheManager cacheManager;
    
    public void setQueueManager(QueueManager queue) {
        this.queueManager = queue;
    }

    public void setCacheManager(CacheManager cache) {
        this.cacheManager = cache;
    }

    @Override
    public QueueManager getQueueManager() {
        return queueManager;
    }
    
    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public BatchStatus submit(BatchRequest request) {
        String requestKey = request.getKey();
        if (cacheManager.isReady(requestKey)) {
            return new BatchStatus(requestKey, cacheManager.getResultURL(request), StatusFlag.Completed);
        } else {
            BatchStatus status = queueManager.submit(request);
            if (status.getStatus() == StatusFlag.Completed) {
                if ( ! cacheManager.isReady(requestKey)) {
                    // Queue thinks it's previously done this one but lost from the cache, start again
                    status = queueManager.resubmit(request);
                }
            }
            return status;
        }
    }

    @Override
    public BatchStatus getStatus(String requestKey) {
        if (cacheManager.isReady(requestKey)) {
            return new BatchStatus(requestKey, cacheManager.getResultURL(requestKey), StatusFlag.Completed);
        } else {
            BatchStatus status = queueManager.getStatus(requestKey);
            if (status.getStatus() == StatusFlag.Completed) {
                // Supposed to have been completed but not in cache, have to assume answer has been lost
                return new BatchStatus(requestKey, StatusFlag.Unknown);
            } else {
                return status;
            }
        }
    }

    @Override
    public BatchStatus getFullStatus(String requestKey) {
        if (cacheManager.isReady(requestKey)) {
            return new BatchStatus(requestKey, cacheManager.getResultURL(requestKey), StatusFlag.Completed);
        } else {
            BatchStatus status = queueManager.getStatus(requestKey);
            if (status.getStatus() == StatusFlag.Pending) {
                long eta = 0;
                int position = 0;
                for (BatchStatus s : queueManager.getQueue()) {
                    position++;
                    if (s.getEstimatedTime().isPresent()) {
                        eta += s.getEstimatedTime().get();
                    }
                    if (s.getKey().equals(requestKey)) {
                        status.setEta(eta);
                        status.setPositionInQueue(position);
                    }
                }
            } else if (status.getStatus() == StatusFlag.InProgress) {
                if (status.getStarted().isPresent() && status.getEstimatedTime().isPresent()) {
                    long sofar = System.currentTimeMillis() - status.getStarted().get();
                    long expected = status.getEstimatedTime().get();
                    if (sofar < expected) {
                        status.setEta( expected - sofar );
                    }
                    status.setPositionInQueue(0);
                }
            } else if (status.getStatus() == StatusFlag.Completed) {
                // Supposed to have been completed but not in cache, have to assume answer has been lost
                return new BatchStatus(requestKey, StatusFlag.Unknown);
            }
            return status;
        }
    }

    @Override
    public List<BatchStatus> getQueue() {
        return queueManager.getQueue();
    }

    @Override
    public BatchRequest findRequest(String key) {
        return queueManager.findRequest(key);
    }

}
