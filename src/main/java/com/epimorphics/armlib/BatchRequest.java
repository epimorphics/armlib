/******************************************************************
 * File:        BatchRequest.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import com.epimorphics.util.EpiException;

/**
 * Represents a request, originally submitted via some REST front end, which
 * is to be queued as a batch job. Comprises a request URI and a set of parameter
 * values.
 * <p>Requests can also include a flag ("sticky") to indicate that the result
 * should be persistently cached.</p>
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public class BatchRequest {
    public static final int MAX_KEY_LENGTH = 800;

    protected String requestURI;
    protected MultivaluedMap<String, String> parameters;
    protected String key;
    protected int estimatedTime = 60;
    protected boolean sticky;
    
    public BatchRequest(String requestURI, MultivaluedMap<String, String> parameters) {
        this(requestURI, parameters, false);
    }
    
    public BatchRequest(String requestURI, MultivaluedMap<String, String> parameters, boolean sticky) {
        this.requestURI = requestURI;
        this.parameters = parameters;
        this.sticky = sticky;
    }

    /**
     * The original URI being requested
     */
    public String getRequestURI() {
        return requestURI;
    }

    /**
     * All relevant parameters for the request. Will typically include at least query parameters 
     * but may also include path parameters. Process parameters not relevant to generating
     * the result should be stripped.
     */
    public MultivaluedMap<String, String> getParameters() {
        return parameters;
    }

    /**
     * If true this indicates the result should be cached in a separate persistent cache
     * which is not subject to whatever timeout/garbage collection policy applies to
     * the normal cache.
     */
    public boolean isSticky() {
        return sticky;
    }
    
    /**
     * Assign a key which identifies the request, useful if the key name should
     * be readable.
     * If one is not assigned one will be generated.
     * @param key The key to use must be shorter than MAX_KEY_LENGTH and must 
     * not contain "/" path separators.
     */
    public void setKey(String key) {
        if (key.length() > MAX_KEY_LENGTH || key.contains("/")) {
            throw new IllegalArgumentException("Illegal request key: " + key);
        }
        this.key = key;
    }
    
    /**
     * Returns a short unique key identifying the request. This is guaranteed to fit
     * within the limitations of S3 key lengths.
     */
    public String getKey() {
        if (key == null) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update( requestURI.getBytes("UTF-8") );
                for (String p : sorted(parameters.keySet())) {
                    for (String value : sorted(parameters.get(p))) {
                        String k = p + "=" + value;
                        md.update( k.getBytes("UTF-8") );
                    }
                }
                byte[] array = md.digest();
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < array.length; ++i) {
                    sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
                }
                key = sb.toString();
            } catch (Exception e) {
                throw new EpiException("'Impossible' internal error generating digest", e);
            }
        }
        return key;
    }
    
    /**
     * Get the estimated time to process this request (in seconds)
     */
    public int getEstimatedTime() {
        return estimatedTime;
    }

    public void setEstimatedTime(int estimatedTime) {
        this.estimatedTime = estimatedTime;
    }

    private List<String> sorted(Collection<String> collection) {
        List<String> result = new ArrayList<>(collection);
        Collections.sort(result);
        return result;
    }
}
