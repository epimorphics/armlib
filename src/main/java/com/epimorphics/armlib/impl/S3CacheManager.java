/******************************************************************
 * File:        S3CacheManager.java
 * Created by:  Dave Reynolds
 * Created on:  26 Nov 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.armlib.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import com.epimorphics.armlib.BatchRequest;
import com.epimorphics.armlib.CacheManager;
import com.epimorphics.armlib.MediaTypes;
import com.epimorphics.util.EpiException;
import com.epimorphics.util.FileUtil;
import com.epimorphics.util.NameUtils;

public class S3CacheManager extends BaseCacheManager implements CacheManager {
    public static final String PERSISTENT_SEGMENT = "persistent/";
    public static final String TEMPORARY_SEGMENT = "cache/";

    protected String S3BaseURL = "https://s3-eu-west-1.amazonaws.com/";
    protected String workArea = "/tmp";
    protected String bucket;
    protected String baseKey;
    protected Region region = Region.EU_WEST_1;
    protected S3Client s3client;
    
    public S3CacheManager() {
        s3client = S3Client.builder().region(region).build();
    }
    
    public void setS3BaseURL(String s3BaseURL) {
        S3BaseURL = NameUtils.ensureLastSlash(s3BaseURL);
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setBaseKey(String baseKey) {
        this.baseKey = NameUtils.ensureLastSlash(baseKey);
    }
    
    public void setWorkArea(String workArea) {
        this.workArea = workArea;
    }

    @Override
    public String getResultURL(BatchRequest request) {
        return S3BaseURL + bucket + "/" + getS3Key(request.getKey(), defaultSuffix, request.isSticky());
    }

    @Override
    public String getResultURL(String requestKey) {
        return S3BaseURL + bucket + "/" + getS3Key(requestKey, defaultSuffix);
    }
    
    private String getS3Key(String requestKey, String suffix, boolean sticky) {
        return baseKey + (sticky? PERSISTENT_SEGMENT : TEMPORARY_SEGMENT)+ requestKey + "." + suffix;
    }

    private String getS3Key(String requestKey, String suffix) {
        String obj = getS3Key(requestKey, suffix, true);
        if ( exists(obj) ) {
            return obj;
        }
        obj = getS3Key(requestKey, suffix, false);
        if (exists(obj)) {
            return obj;
        }
        return null;
    }
    
    @Override
    public boolean isReady(String requestKey) {
        return getS3Key(requestKey, defaultSuffix) != null;
    }

    private boolean exists(String key) {
        try {
            s3client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key)
                    .build());
            return true;
        } catch (S3Exception e) {
            if (e.awsErrorDetails().sdkHttpResponse().statusCode() == 404) {
                return false;
            } else {
                throw new EpiException("Problem accessing S3 bucket", e);
            }
        }
    }
    
    @Override
    public InputStream readResult(String requestKey) {
        return readResult(requestKey, defaultSuffix);
    }

    @Override
    public InputStream readResult(String requestKey, String suffix) {
        // Brute force since it makes three s3 calls but this is not the main interface
        String objkey = getS3Key(requestKey, suffix);
        if (objkey != null) {
            ResponseInputStream<GetObjectResponse> object = s3client.getObject(GetObjectRequest.builder().bucket(bucket).key(objkey)
                    .build());
            return object;
        } else {
            return null;
        }
    }

    @Override
    public void upload(BatchRequest request, String suffix, File result) {
        try {
            doUpload(request, suffix, result);
        } catch (IOException e) {
            throw new EpiException("Failed to access upload file", e);
        }
    }

    @Override
    protected void upload(BatchRequest request, String suffix, InputStream result) {
        try {
            File workDir = new File(workArea);
            File tempFile = new File(workDir, request.getKey()+ "." + suffix);
            OutputStream out = new BufferedOutputStream(new FileOutputStream(tempFile));
            FileUtil.copyResource(result, out);
            out.close();
            doUpload(request, suffix, tempFile);
            tempFile.delete();
        } catch (IOException e) {
            throw new EpiException("Problem buffering results stream for upload");
        }
    }
    
    private void doUpload(BatchRequest request, String suffix, File result) throws IOException {
        String objkey = getS3Key(request.getKey(), suffix, request.isSticky());
        HeadObjectResponse meta = HeadObjectResponse.builder()
                .build();
        String contentType = MediaTypes.getMediaTypeForExtension(suffix);
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder().bucket(bucket).key(objkey).contentLength(result.length());
        if (contentType != null) {
            requestBuilder = requestBuilder.contentType(contentType);
        }
        if (compress) {
            requestBuilder = requestBuilder.contentEncoding("gzip");
        }
        InputStream stream = new BufferedInputStream( new FileInputStream(result) );
        s3client.putObject(requestBuilder.build(), RequestBody.fromInputStream(stream, result.length()));
    }

    @Override
    public void clear() {
        clearFolder( baseKey + TEMPORARY_SEGMENT );
        clearFolder( baseKey + PERSISTENT_SEGMENT );
    }

    @Override
    public void clearNonSticky() {
        clearFolder( baseKey + TEMPORARY_SEGMENT );
    }

    private void clearFolder(String folder) {
        List<String> toDelete = new ArrayList<>();
        do {
            toDelete.clear();
            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                    .bucket(bucket)
                    .prefix(folder)
                    .build();
            ListObjectsResponse listObjectsResponse = s3client.listObjects(listObjectsRequest);
            for (S3Object summary : listObjectsResponse.contents()) {
                toDelete.add(summary.key());
            }
            if (!toDelete.isEmpty()) {
                Collection<ObjectIdentifier> identifiers = toDelete.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toList());
                DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(Delete.builder().objects(identifiers).build())
                        .build();
                s3client.deleteObjects(deleteRequest);
            }
        } while (!toDelete.isEmpty());
    }
}
