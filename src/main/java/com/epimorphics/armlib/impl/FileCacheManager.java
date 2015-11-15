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
import com.epimorphics.armlib.CacheManager;
import com.epimorphics.util.EpiException;
import com.epimorphics.util.FileUtil;
import com.epimorphics.util.NameUtils;

/**
 * Non-distributed, file-based implementation primarily for test/dev use.
 */
public class FileCacheManager extends ComponentBase implements CacheManager {
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
    public String getResultURL(String requestKey) {
        return "file:///" + getFileName(requestKey);
    }
    
    private String getFileName(String requestKey) {
        return getFileName(requestKey, defaultSuffix);
    }
    
    private String getFileName(String requestKey, String suffix) {
        return cacheDir + requestKey + "." + suffix;
    }

    @Override
    public boolean isReady(String requestKey) {
        return new File( getFileName(requestKey) ).exists();
    }

    @Override
    public InputStream readResult(String requestKey) {
        return readResult(requestKey, defaultSuffix);
    }

    @Override
    public InputStream readResult(String requestKey, String suffix) {
        try {
            return new FileInputStream( new File( getFileName(requestKey, suffix) ) );
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public void upload(String requestKey, File result) {
        upload(requestKey, defaultSuffix, result);
    }

    @Override
    public void upload(String requestKey, String suffix, File result) {
        try {
            FileUtil.copyResource(result.getPath(), getFileName(requestKey, suffix));
        } catch (IOException e) {
            throw new EpiException(e);
        }
    }

    @Override
    public void upload(String requestKey, InputStream result) {
        upload(requestKey, defaultSuffix, result);
    }

    @Override
    public void upload(String requestKey, String suffix, InputStream result) {
        try {
            OutputStream os = new FileOutputStream( getFileName(requestKey, suffix) );
            FileUtil.copyResource(result, os);
            os.close();
        } catch (IOException e) {
            throw new EpiException(e);
        }
    }

}
