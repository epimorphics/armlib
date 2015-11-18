/******************************************************************
 * File:        BatchStatus.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.apache.jena.atlas.json.JsonObject;

import com.epimorphics.json.JsonUtil;

/**
 * Represents the status of a batch request. Some query operations 
 * will include only minimal status information so some of the status
 * attributes are optional.
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public class BatchStatus {
    public static enum StatusFlag {Unknown, Pending, InProgress, Failed, Completed};
    
    protected StatusFlag status;
    protected String     key;
    protected String            url;
    protected Optional<Long>    started = Optional.empty();
    protected Optional<Integer> positionInQueue = Optional.empty();
    protected Optional<Long>    estimatedTime = Optional.empty();
    protected Optional<Long>    eta = Optional.empty();
    
    public BatchStatus(String key, String url, StatusFlag status) {
        this.key = key;
        this.status = status;
        this.url = url;
    }
    
    public BatchStatus(String key, StatusFlag status) {
        this.key = key;
        this.status = status;
    }

    public void setURL(String url) {
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
     *   <li><b>Failed</b> - the request could not be completed</li>
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
     * Return the estimated time for completion of this request (in ms) based on
     * the cumulative estimatedDuration of all the requests in the queue ahead of this one.
     * The estimate is only available for Pending or InProgress requests and may only
     * be available as a result of some queries. Even when available the estimated
     * time may be wildly inaccurate not least due to unknown loading/cache state of the data servers.
     */
    public Optional<Long> getEta() {
        return eta;
    }

    public void setEta(long eta) {
        this.eta = Optional.of(eta);
    }
    
    /**
     * Get the estimated time to process this request (in ms)
     */
    public Optional<Long> getEstimatedTime() {
        return estimatedTime;
    }

    public void setEstimatedTime(long estimatedTime) {
        this.estimatedTime = Optional.of(estimatedTime);
    }
    
    public JsonObject asJson() {
        JsonObject o = JsonUtil.makeJson("key", key, "status", status.toString());
        if (url != null) {
            o.put("url", url);
        }
        if (positionInQueue.isPresent()) {
            o.put("positionInQueue", positionInQueue.get());
        }
        if (eta.isPresent()) {
            o.put("eta", eta.get());
        }
        if (started.isPresent()) {
            o.put("started",  new SimpleDateFormat().format( new Date( started.get() ) ) );
        }
        return o;
    }
    
}
