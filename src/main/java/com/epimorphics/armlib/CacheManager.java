/******************************************************************
 * File:        CacheManager.java
 * Created by:  Dave Reynolds
 * Created on:  13 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.io.File;
import java.io.InputStream;

/**
 * Cache manager implementations provide a persistent cache for the result
 * of a batch request. Implementations may support a distributed cache so that
 * results generated by one instance of the request processor can be discovered
 * and retrieved from other instances.
 * <p>The URLs used for retrieval will be a prefix, the request Key and a suffix.
 * Configured cache managers can support multiple suffixes but there should be a 
 * single default suffix.
 * </p>
 */
public interface CacheManager {
    
    /**
     * Return the URL from which the result of the request is/will be available
     */
    public String getResultURL(String requestKey);
    
    /**
     * Return true if the result of the given request is available.
     * This tests if the variant with the default suffix is available. 
     */
    public boolean isReady(String requestKey);
    
    /**
     * Return the result of the request as an InputStream.
     * Returns null if the result is not available.
     */
    public InputStream readResult(String requestKey);
    
    /**
     * Return the result of the request as an InputStream.
     * Returns null if the result is not available.
     */
    public InputStream readResult(String requestKey, String suffix);

    /**
     * Upload the result of a request to the persistent cache
     */
    public void upload(String requestKey, File result);

    /**
     * Upload the result of a request to the persistent cache
     */
    public void upload(String requestKey, String suffix, File result);

    /**
     * Upload the result of a request to the persistent cache
     */
    public void upload(String requestKey, InputStream result);

    /**
     * Upload the result of a request to the persistent cache
     */
    public void upload(String requestKey, String suffix, InputStream result);
}
