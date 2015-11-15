/******************************************************************
 * File:        QueueManager.java
 * Created by:  Dave Reynolds
 * Created on:  13 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.util.List;

/**
 * QueueManager implementations allow a set of (possibly long-running) batch requests to be queued
 * and executed in a managed sequence. Implementations may support distributed queues
 * so that multiple instances of the QueueManager running on different hosts share the queue.
 */
public interface QueueManager {
    
    /**
     * Submit a request for processing. If the result of a matching request already
     * exist then the status will contain sufficient information to retrieve the cached
     * result. If a matching request is already in progress it will be returned.
     * Otherwise the request will be added to the processing queue for future action
     */
    public BatchStatus submit(BatchRequest request);
    
    /**
     * Retrieve information on the status of a request. This is intended to be a cheap call
     * to simply identify the processing status, it does not scan the queue to estimate ETA.
     */
    public BatchStatus getStatus(String requestKey);
    
    /**
     * Return information on all the requests in the queue
     */
    public List<BatchStatus> getQueue();
    
    /**
     * Retrieve a submitted request from its key, or return null if there is no such request.
     * Processed requests will remain available for a (configuarable) period after completion.
     */
    public BatchRequest findRequest(String key);

    /**
     * Locate the next request to be processed, mark as as InProgress and return it.
     * Returns null if no request is waiting or arrived before the timeout
     * @param timeout maximum time to wait in ms
     */
    public BatchRequest nextRequest(long timeout); 
    
    /**
     * Mark the request as having been completed, removing it from the queue entirely
     */
    public void finishRequest(String key);
    
    /**
     * Indicates that the calling process has failed to complete the request and returns the request
     * back to the pending queue. 
     */
    public void abortRequest(String key);
    
    /**
     * Indicates that the request cannot be completed and marks it as failed.
     */
    public void failRequest(String key);
}
