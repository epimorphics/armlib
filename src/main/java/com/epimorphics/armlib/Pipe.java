/******************************************************************
 * File:        Pipe.java
 * Created by:  Dave Reynolds
 * Created on:  22 Jan 2016
 * 
 * (c) Copyright 2016, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.io.OutputStream;

/**
 * Support for asynchronous upload. The upload will take place in a separate thread
 * from the producer who initiates the upload and supplies the data. The producer
 * must close the Pipe's outputstream otherwise the thread will last forever.
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public interface Pipe {
    
    /**
     * Return the output stream to which the producer should write
     * the data for upload. The producer must close the stream
     */
    public OutputStream getSource();
    
    /**
     * Wait for the upload to be consumed.
     * This should only be called after the producer has closed the stream;
     */
    public void waitForCompletion() throws InterruptedException;
}
