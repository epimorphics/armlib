/******************************************************************
 * File:        TestBatchRequest.java
 * Created by:  Dave Reynolds
 * Created on:  11 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import javax.ws.rs.core.MultivaluedMap;

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.junit.Test;

public class TestBatchRequest {

    @Test
    public void testBasics() {
        MultivaluedMap<String, String> parameters = new MultivaluedStringMap();
        parameters.add("key1", "value1");
        parameters.add("key1", "value2");
        parameters.add("key2", "value3");
        BatchRequest request = new BatchRequest("http://localhost/service", parameters);
        
        String paramString = request.getParameterString();
        assertTrue( paramString.contains("key1=value1") );
        assertTrue( paramString.contains("key1=value2") );
        assertTrue( paramString.contains("key2=value3") );
        
        assertEquals("http://localhost/service", request.getRequestURI());
        assertEquals("value3", request.getParameters().getFirst("key2"));
        String key = request.getKey();
        assertNotNull(key);
        assertTrue( key.length() <= 800);
        
        request.setKey("NewKey");
        assertEquals("NewKey", request.getKey());
        
        parameters.add("key2", "value4");
        String keyOther = new BatchRequest("http://localhost/service", parameters).getKey();
        assertNotSame(key, keyOther);
        
        parameters.addFirst("key3", null);    // Just parameter no value
        assertTrue( parameters.containsKey("key3") );
        
        request = new BatchRequest("http://localhost/service", parameters);
        String enc = request.getParameterString();
        MultivaluedMap<String, String> recovered = BatchRequest.decodeParameterString( enc );
        
        assertEquals(parameters, recovered);
        
    }
}
