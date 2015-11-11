/******************************************************************
 * File:        BatchStatus.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.util.Optional;

/**
 * Represents the status of a batch request. Some query operations 
 * will include only minimal status information so some of the status
 * attributes are optional.
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public class BatchStatus {
    public static enum StatusFlag {Unknown, Pending, InProgress, Completed};
    
    protected StatusFlag status;
    protected String url;
    protected String key;
    protected Optional<Long>    started;
    protected Optional<Integer> positionInQueue;
    protected Optional<Integer> eta;
    
    public BatchStatus(String key, String url, StatusFlag status) {
        this.key = key;
        this.status = status;
        this.url = url;
    }

    /**
     * Return the key that identifies the request whose status this is.
     */
    public String getKey() {
        return key;
    }

    /**
     * Return the status of the batch request. 
     * <ol> 
     *   <li><b>Unknown</b> - the request could not be found (neither queued nor cached)</li>
     *   <li><b>Pending</b> - the request has been queued but processing has not yet started</li>
     *   <li><b>InProgress</b> - processing of the request has started</li>
     *   <li><b>Completed</b> - the request has been processed and the result is available for download</li>
     * </ol> 
     */
    public StatusFlag getStatus() {
        return status;
    }

    public void setStatus(StatusFlag status) {
        this.status = status;
    }

    /**
     * Return the URL from which the completed result can be obtained when it is available.
     */
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Return the time at which processing of request started. Only available
     * if the request is in progress.
     */
    public Optional<Long> getStarted() {
        return started;
    }

    public void setStarted(long started) {
        this.started = Optional.of(started);
    }

    /**
     * Return the position of this request in the processing queue. Only relevant
     * for pending requests and may only be available as a result of some queries.
     */
    public Optional<Integer> getPositionInQueue() {
        return positionInQueue;
    }

    public void setPositionInQueue(int positionInQueue) {
        this.positionInQueue = Optional.of(positionInQueue);
    }

    /**
     * Return the estimated time for completion of this request (in seconds) based on
     * the cumulative estimatedDuration of all the requests in the queue ahead of this one.
     * The estimate is only available for Pending or InProgress requests and may only
     * be available as a result of some queries. Even when available the estimated
     * time may be wildly inaccurate not least due to unknown loading/cache state of the data servers.
     */
    public Optional<Integer> getEta() {
        return eta;
    }

    public void setEta(int eta) {
        this.eta = Optional.of(eta);
    }
    
    
}
