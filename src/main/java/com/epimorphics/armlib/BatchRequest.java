/******************************************************************
 * File:        BatchRequest.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 *
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import com.epimorphics.util.EpiException;
import jakarta.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public static final int MAX_KEY_LENGTH = 200;

    protected String requestURI;
    protected MultivaluedMap<String, String> parameters;
    protected String key;
    protected long estimatedTime = 60000;
    protected boolean sticky;

    public BatchRequest(String requestURI, MultivaluedMap<String, String> parameters) {
        this(requestURI, parameters, false);
    }

    public BatchRequest(String requestURI, MultivaluedMap<String, String> parameters, boolean sticky) {
        this.requestURI = requestURI;
        this.parameters = parameters;
        this.sticky = sticky;
    }

    public BatchRequest(String requestURI, String parameterString) {
        this(requestURI, parameterString, false);
    }

    public BatchRequest(String requestURI, String parameterString, boolean sticky) {
        this.requestURI = requestURI;
        this.parameters = decodeParameterString(parameterString);
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
     * Return the parameters for the request as a single query string
     */
    public String getParameterString() {
        StringBuilder buff = new StringBuilder();
        boolean started = false;
        for (String param : parameters.keySet()) {
            for (String value : parameters.get(param)) {
                if (started) buff.append("&");
                buff.append(param);
                buff.append("=");
                buff.append(value);
                started = true;
            }
        }
        return buff.toString();
    }

    public static MultivaluedMap<String, String> decodeParameterString(String paramString) {
        MultivaluedMap<String, String> parameters = new MultivaluedStringMap();
        for (String binding : paramString.split("&")) {
            String[] pair = binding.split("=");
            String param = pair[0];
            if (pair.length == 2) {
                parameters.add(param, pair[1]);
            } else {
                parameters.add(param, null);
            }
        }
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

    public void setSticky(boolean sticky) {
        this.sticky = sticky;
    }

    /**
     * Assign a key which identifies the request, useful if the key name should
     * be readable.
     * If one is not assigned one will be generated.
     *
     * @param key The key to use must be shorter than MAX_KEY_LENGTH and must
     *            not contain "/" path separators.
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
            StringBuilder kb = new StringBuilder();
            kb.append(requestURI);
            kb.append("_");
            for (String p : sorted(parameters.keySet())) {
                kb.append(p);
                kb.append("_");
                for (String value : sorted(parameters.get(p))) {
                    kb.append(value);
                    kb.append("_");
                }
            }

            key = kb.toString();
            key = key.replace("/", "%2F");
            key = key.substring(0, key.length() - 1); // Strip last "_"
            if (key.length() > MAX_KEY_LENGTH) {
                // Explicit coding too big, so use digest
                try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(requestURI.getBytes(StandardCharsets.UTF_8));
                    for (String p : sorted(parameters.keySet())) {
                        for (String value : sorted(parameters.get(p))) {
                            String k = p + "=" + value;
                            md.update(k.getBytes(StandardCharsets.UTF_8));
                        }
                    }
                    byte[] array = md.digest();
                    StringBuilder sb = new StringBuilder();
                    for (byte b : array) {
                        sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
                    }
                    key = sb.toString();
                } catch (Exception e) {
                    throw new EpiException("'Impossible' internal error generating digest", e);
                }
            }
        }
        return key;
    }

    /**
     * Get the estimated time to process this request (in ms)
     */
    public long getEstimatedTime() {
        return estimatedTime;
    }

    public void setEstimatedTime(long estimatedTime) {
        this.estimatedTime = estimatedTime;
    }

    private List<String> sorted(Collection<String> collection) {
        List<String> result = new ArrayList<>(collection);
        Collections.sort(result);
        return result;
    }
}
