/******************************************************************
 * File:        TestRequestManager.java
 * Created by:  Dave Reynolds
 * Created on:  17 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.jena.util.FileManager;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.junit.Test;
import static org.junit.Assert.*;

import com.epimorphics.armlib.BatchStatus.StatusFlag;
import com.epimorphics.armlib.impl.FileCacheManager;
import com.epimorphics.armlib.impl.MemQueueManager;
import com.epimorphics.armlib.impl.StandardRequestManager;
import com.epimorphics.util.FileUtil;

/**
 * Generic test for request managers. Can be configured to run
 * AWS based versions as well but default release testing uses
 * test implementations.
 * 
 * @author <a href="mailto:dave@epimorphics.com">Dave Reynolds</a>
 */
public class TestRequestManager {

    @Test
    public void testGenericRequestManager() throws IOException {
        FileCacheManager cache = new FileCacheManager();
        String testDir = Files.createTempDirectory("testmonitor").toFile().getPath();
        cache.setCacheDir( testDir );

        MemQueueManager queue = new MemQueueManager();
        queue.setCheckInterval(5);
        
        doTestStandardRequestManager(queue, cache);
        
        FileUtil.deleteDirectory(testDir);
    }
    
    protected static void doTestStandardRequestManager(QueueManager qm, CacheManager cm) {
        StandardRequestManager rm = new StandardRequestManager();
        rm.setCacheManager(cm);
        rm.setQueueManager(qm);
        
        // Empty queue
        assertNull( qm.nextRequest(12) );
        
        // Request not present in empty queue
        BatchRequest req1 = request("/test1", false, "p", "foo", "q", "bar");
        req1.setEstimatedTime(100);
        assertNull( rm.findRequest(req1.getKey()) );
        BatchStatus s1 = rm.getStatus( req1.getKey() );
        assertNotNull( s1 );
        assertEquals(StatusFlag.Unknown, s1.getStatus());
        
        // Submit request and then its visible
        s1 = rm.submit(req1);
        assertEquals(StatusFlag.Pending, s1.getStatus());
        s1 = rm.getStatus( req1.getKey() );
        assertEquals(StatusFlag.Pending, s1.getStatus());
        
        // Request parameter order can vary
        BatchRequest req1b = rm.findRequest( request("/test1", true, "q", "bar", "p", "foo").getKey() );
        assertEquals(req1.getRequestURI(), req1b.getRequestURI());
        assertEquals(req1.getParameters(), req1b.getParameters());

        // Second request and can find cumulative eta of the two
        BatchRequest req2 = request("/test2", false, "p", "foo");
        req2.setEstimatedTime(50);
        rm.submit(req2);
        BatchStatus s2 = rm.getFullStatus(req2.getKey());
        assertEquals(StatusFlag.Pending, s2.getStatus());
        assertEquals(150, (long)s2.getEta().get());
        assertEquals(2, (int)s2.getPositionInQueue().get());

        // Queue summary looks right
        checkQueue(rm, req1.getKey(), StatusFlag.Pending, req2.getKey(), StatusFlag.Pending);
        
        // Nothing in the cache yet for either
        assertFalse( cm.isReady( req1.getKey() ) );
        assertFalse( cm.isReady( req2.getKey() ) );
        
        // Start one request and that changes its state
        long start1 = System.currentTimeMillis();
        BatchRequest next = qm.nextRequest(12);
        long start2 = System.currentTimeMillis();
        assertNotNull( next );
        assertEquals(req1.getRequestURI(), next.getRequestURI());
        checkQueue(rm, req1.getKey(), StatusFlag.InProgress, req2.getKey(), StatusFlag.Pending);
        s1 = rm.getStatus( req1.getKey() );
        assertTrue( s1.getStarted().isPresent() );
        assertTrue( s1.getStarted().get() >= start1 );
        assertTrue( s1.getStarted().get() <= start2 );

        // Put it back and try again
        qm.abortRequest( next.getKey() );
        checkQueue(rm, req1.getKey(), StatusFlag.Pending, req2.getKey(), StatusFlag.Pending);
        next = qm.nextRequest(12);
        assertEquals(req1.getRequestURI(), next.getRequestURI());
        checkQueue(rm, req1.getKey(), StatusFlag.InProgress, req2.getKey(), StatusFlag.Pending);
        
        // Generate a dummy result for the request and finish it
        cm.upload(next, new ByteArrayInputStream( "Test1 result".getBytes() ));
        qm.finishRequest(next.getKey());
        checkQueue(rm, req2.getKey(), StatusFlag.Pending);
        s2 = rm.getFullStatus(req2.getKey());
        assertEquals(50, (long)s2.getEta().get());
        assertEquals(1, (int)s2.getPositionInQueue().get());
        assertEquals(StatusFlag.Completed, rm.getStatus( req1.getKey() ).getStatus());
        
        // Cache sees it OK
        assertTrue( cm.isReady( req1.getKey() ) );
        String value = FileManager.get().readWholeFileAsUTF8( cm.readResult( req1.getKey() ) );
        assertEquals( "Test1 result", value);
        
        // Get next request but fail it
        next = qm.nextRequest(12);
        s2 = rm.getFullStatus(req2.getKey());
        assertEquals(0, (int)s2.getPositionInQueue().get());
        assertEquals(StatusFlag.InProgress, s2.getStatus());
        assertEquals(req2.getKey(), next.getKey());
        qm.failRequest( next.getKey() );
        assertTrue ( rm.getQueue().isEmpty() );
        s2 = rm.getFullStatus(req2.getKey());
        assertEquals(StatusFlag.Failed, s2.getStatus());
    }
    
    private static BatchRequest request(String url, boolean sticky, String...args) {
        MultivaluedMap<String, String> parameters = new MultivaluedStringMap();
        for (int i = 0; i < args.length;) {
            String key = args[i++];
            String value = args[i++];
            parameters.add(key, value);
        }
        return new BatchRequest(url, parameters, sticky);
    }
    
    private static void checkQueue(RequestManager rm, Object...args) {
        List<BatchStatus> queue = rm.getQueue();
        assertEquals(args.length/2, queue.size());
        for (int i = 0; i < args.length;) {
            BatchStatus status = queue.get( i/2 );
            String key = (String)args[i++];
            StatusFlag s = (StatusFlag)args[i++];
            assertEquals(key, status.getKey());
            assertEquals(s, status.getStatus());
        }
    }
}
