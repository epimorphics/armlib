/******************************************************************
 * File:        FileCacheManager.java
 * Created by:  Dave Reynolds
 * Created on:  15 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.CacheManager;
import com.epimorphics.util.EpiException;
import com.epimorphics.util.FileUtil;
import com.epimorphics.util.NameUtils;

/**
 * Non-distributed, file-based implementation primarily for test/dev use.
 */
public class FileCacheManager extends ComponentBase implements CacheManager {
    public static final String PERSISTENT_SEGMENT = "persistent/";
    public static final String TEMPORARY_SEGMENT = "cache/";
    
    protected String cacheDir;
    protected String defaultSuffix = "csv";
    
    public void setCacheDir(String cacheDir) {
        this.cacheDir = NameUtils.ensureLastSlash( expandFileLocation(cacheDir) );
        FileUtil.ensureDir(cacheDir);
    }

    public void setDefaultSuffix(String defaultSuffix) {
        this.defaultSuffix = defaultSuffix;
    }

    @Override
    public String getResultURL(BatchRequest request) {
        return "file:///" + getFileName(request.getKey(), request.isSticky());
    }

    @Override
    public String getResultURL(String requestKey) {
        return "file:///" + findFileFor(requestKey, defaultSuffix).getPath();
    }
    
    private String getFileName(String requestKey, boolean sticky) {
        return getFileName(requestKey, defaultSuffix, sticky);
    }
    
    private String getFileName(String requestKey, String suffix, boolean sticky) {
        return cacheDir + (sticky? PERSISTENT_SEGMENT : TEMPORARY_SEGMENT)+ requestKey + "." + suffix;
    }

    private File findFileFor(String requestKey, String suffix) {
        File file = new File( getFileName(requestKey, suffix, true) );
        if (file.exists()) {
            return file;
        }
        file = new File( getFileName(requestKey, suffix, false) );
        if (file.exists()) {
            return file;
        }
        return null;
    }
    
    @Override
    public boolean isReady(String requestKey) {
        return findFileFor(requestKey, defaultSuffix) != null;
    }

    @Override
    public InputStream readResult(String requestKey) {
        return readResult(requestKey, defaultSuffix);
    }

    @Override
    public InputStream readResult(String requestKey, String suffix) {
        try {
            return new FileInputStream( findFileFor(requestKey, suffix) );
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public void upload(BatchRequest request, File result) {
        upload(request, defaultSuffix, result);
    }

    @Override
    public void upload(BatchRequest request, String suffix, File result) {
        try {
            FileUtil.copyResource(result.getPath(), getFileName(request.getKey(), request.isSticky()));
        } catch (IOException e) {
            throw new EpiException(e);
        }
    }

    @Override
    public void upload(BatchRequest request, InputStream result) {
        upload(request, defaultSuffix, result);
    }

    @Override
    public void upload(BatchRequest request, String suffix, InputStream result) {
        try {
            OutputStream os = new FileOutputStream( getFileName(request.getKey(), request.isSticky()) );
            FileUtil.copyResource(result, os);
            os.close();
        } catch (IOException e) {
            throw new EpiException(e);
        }
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void clearNonSticky() {
        // TODO Auto-generated method stub
        
    }

}