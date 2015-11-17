/******************************************************************
 * File:        RequestManager.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.util.List;

/**
 * Provides an interface for how client applications can submit batch requests
 * and monitor their status.
 */
public interface RequestManager {

    /**
     * Return the queue manager which this processor uses to queue requests 
     */
    public QueueManager getQueueManager();

    /**
     * Return the cache manager which this processor uses to cache request results 
     */
    public CacheManager getCacheManager();
    
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
     * Retrieve a submitted request from its key, or return null if there is no such request.
     * Processed requests will remain available for a (configuarable) period after completion.
     */
    public BatchRequest findRequest(String key);
    
    /**
     * Retrieve information on the status of a request, including whatever queue and ETA imformation is available.
     */
    public BatchStatus getFullStatus(String requestKey);
    
    /**
     * Return information on all the requests in the queue
     */
    public List<BatchStatus> getQueue();

}
