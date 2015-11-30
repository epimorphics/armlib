/******************************************************************
 * File:        MediaTypes.java
 * Created by:  Dave Reynolds
 * Created on:  30 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.util.HashMap;
import java.util.Map;

/**
 * Register of some supported mediatypes mapped from the file extension used in the cache manager.
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public class MediaTypes {

    protected static Map<String, String> types = new HashMap<>();
    
    static {
        types.put("csv",  "text/csv");
        types.put("txt",  "text/plain");
        types.put("html", "text/html");
        types.put("ttl",  "text/turtle");
        types.put("rdf",  "application/rdf+xml");
        types.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        types.put("xls",  "application/vnd.ms-excel");
    }
    
    public static String getMediaTypeForExtension(String ext) {
        return types.get(ext);
    }
}
