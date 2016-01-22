/******************************************************************
 * File:        BaseCacheManager.java
 * Created by:  Dave Reynolds
 * Created on:  22 Jan 2016
 * 
 * (c) Copyright 2016, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.zip.GZIPOutputStream;

import com.epimorphics.appbase.core.ComponentBase;
import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.CacheManager;
import com.epimorphics.armlib.Pipe;
import com.epimorphics.util.EpiException;

public abstract class BaseCacheManager extends ComponentBase implements CacheManager {
    protected String defaultSuffix = "csv";

    public void setDefaultSuffix(String defaultSuffix) {
        this.defaultSuffix = defaultSuffix;
    }

    @Override
    public void upload(BatchRequest request, File result) {
        upload(request, defaultSuffix, result);
    }

    public abstract void upload(BatchRequest request, String suffix, File result);

    @Override
    public Pipe upload(BatchRequest request) {
        return upload(request, defaultSuffix, false);
    }
    
    @Override
    public Pipe upload(BatchRequest request, String suffix, boolean compress) {
        PipeImpl pipe = new PipeImpl(request, suffix, compress);
        pipe.start();
        return pipe;
    }
    
    protected abstract void upload(BatchRequest request, String suffix, InputStream result);

    public class PipeImpl implements Pipe, Runnable {
        protected Thread runner;
        protected BatchRequest request;
        protected String suffix;
        protected OutputStream source;
        protected PipedInputStream  sink;
        
        public PipeImpl(BatchRequest request, String suffix, boolean compress) {
            this.request = request;
            this.suffix = compress ? suffix + ".gz" : suffix;
            PipedOutputStream out = new PipedOutputStream();
            try {
                sink = new PipedInputStream(out);
                source = compress ? new GZIPOutputStream(out) : out;
            } catch (IOException e) {
                throw new EpiException("Failed to create pipe", e);
            }
        }
        
        /**
         * Return the output stream to which the producer should write
         * the data for upload. The producer must close the stream
         */
        public OutputStream getSource() {
            return source;
        }
        
        public void start() {
            runner = new Thread(this);
            runner.start();
        }

        @Override
        public void run() {
            upload(request, suffix, sink);
        }

        @Override
        public void waitForCompletion() throws InterruptedException {
            runner.join();
        }        

    }

}
