/*
 * Keep Alive Plugin
 * Copyright (C) 2012 Jeriadoc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */


package keepalive;

import com.db4o.ObjectContainer;
import freenet.client.ArchiveManager.ARCHIVE_TYPE;
import freenet.client.ClientMetadata;
import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.FetchWaiter;
import freenet.client.FECJob;
import freenet.client.HighLevelSimpleClient;
import freenet.client.InsertContext.CompatibilityMode;
import freenet.client.Metadata;
import freenet.client.SplitfileBlock;
import freenet.client.StandardOnionFECCodec;
import freenet.client.async.ClientContext;
import freenet.client.async.ClientGetState;
import freenet.client.async.ClientRequester;
import freenet.client.async.MinimalSplitfileBlock;
import freenet.client.async.SplitFileFetcher;
import freenet.client.async.SplitFileSegmentKeys;
import freenet.client.async.StreamGenerator;
import freenet.crypt.HashResult;
import freenet.keys.CHKBlock;
import freenet.keys.ClientCHK;
import freenet.keys.FreenetURI;
import freenet.node.RequestClient;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.api.Bucket;
import freenet.support.compress.Compressor;
import freenet.support.plugins.helpers1.PluginContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

        
public class Reinserter extends Thread{
    
    public Plugin plugin;
    private PluginRespirator pr;
    protected int nSiteId;
    private long nLastActivityTime;
    private HashMap<FreenetURI,Metadata> mManifestURIs;
    private HashMap<FreenetURI,Block> mBlocks;
    private long nLastSaveTime = 0;
    private int nParsedSegmentId;
    private int nParsedBlockId;
    protected Vector<Segment> vSegments = new Vector();
    public int nActiveSingleJobCount = 0;
    private boolean bReady = false;

    
    public Reinserter(Plugin plugin, int nSiteId){
        try{

            this.plugin = plugin;
            this.nSiteId = nSiteId;
            
            // stop previous reinserter, start this one
            plugin.stopReinserter();
            plugin.setIntProp("active",nSiteId);            
            plugin.saveProp();
            plugin.reinserter = this;
            
            // activity guard
            (new ActivityGuard(this)).start();
            
        }catch(Exception e){
            plugin.log("Reinserter(): "+e.getMessage(),0);
        }
    }

    
    public void run(){
        try{
            
            // init
            pr = plugin.pluginContext.pluginRespirator;  
            mManifestURIs = new HashMap();
            mBlocks = new HashMap();
            String cUri = plugin.getProp("uri_"+nSiteId);
            plugin.log("start reinserter for site "+cUri+" ("+nSiteId+")",1);
            plugin.clearLog(plugin.getLogFilename(nSiteId));

            // update and register uri
            FreenetURI uri = new FreenetURI(cUri);
            if (uri.isUSK()){
                FreenetURI uri_new = updateUsk(uri);
                if (uri_new != null && !uri_new.equals(uri)){
                    plugin.setProp("uri_"+nSiteId,uri_new.toString());
                    plugin.setProp("blocks_"+nSiteId,"?");
                    uri = uri_new;
                }    
            }    
            registerManifestUri(uri,-1);
            
            // load list of keys (if exists)
            if (!plugin.getProp("blocks_"+nSiteId).equals("?")){
            
                log("*** loading list of blocks ***",0,0);
                loadBlockUris();
            
            }else{
            
                // parse metadata
                log("*** parsing data structure ***",0,0);
                nParsedSegmentId = -1;
                nParsedBlockId = -1;
                while (mManifestURIs.size() > 0){

                    if (!isActive()) return;
                    uri = (FreenetURI)mManifestURIs.keySet().toArray()[0];
                    log(uri.toString(),0);
                    parseMetadata(uri,null,0);
                    mManifestURIs.remove(uri);

                }    
                if (!isActive()) return;
                saveBlockUris();
                plugin.setIntProp("blocks_"+nSiteId,mBlocks.size());
                plugin.saveProp();
            }
            
            // max segment id
            int nMaxSegmentId = -1;
            for (Block block: mBlocks.values()){
                nMaxSegmentId = Math.max(nMaxSegmentId,block.nSegmentId);
            }
            
            // init reinsertion
            if (plugin.getIntProp("segment_"+nSiteId) == nMaxSegmentId)
                plugin.setIntProp("segment_"+nSiteId,-1);
            if (plugin.getIntProp("segment_"+nSiteId) == -1){

                log("*** starting reinsertion ***",0,0);

                // reset success counter
                StringBuffer success = new StringBuffer();
                StringBuffer segmentsSuccess = new StringBuffer();
                for (int i=0; i<=nMaxSegmentId; i++){
                    if (i > 0)
                        success.append(",");
                    success.append("0,0");
                    segmentsSuccess.append("0");
                }                    
                plugin.setProp("success_"+nSiteId,success.toString());
                plugin.setProp("success_segments_"+nSiteId,segmentsSuccess.toString());
                plugin.saveProp();

            }else{

                log("*** continuing reinsertion ***",0,0);

                // add dummy segments
                for (int i=0; i<=plugin.getIntProp("segment_"+nSiteId); i++){
                    vSegments.add(null);
                }

                // reset success counter                
                StringBuffer success = new StringBuffer();
                String[] aSuccess = plugin.getProp("success_"+nSiteId).split(",");
                for (int i=(plugin.getIntProp("segment_"+nSiteId)+1)*2; i<aSuccess.length; i++){
                    aSuccess[i] = "0";
                }
                for (int i=0; i<aSuccess.length; i++){
                    if (i > 0)
                        success.append(",");
                    success.append(aSuccess[i]);
                }                    
                plugin.setProp("success_"+nSiteId,success.toString());
                plugin.saveProp();
                
            }

            // start reinsertion
            HighLevelSimpleClient hlsc = (HighLevelSimpleClient)plugin.pluginContext.hlsc;
            while (true){
                if (!isActive()) return;
                
                // next segment
                int nSegmentSize = 0;
                for (Block block: mBlocks.values()){
                    if (block.nSegmentId == vSegments.size())
                        nSegmentSize++;
                }
                if (nSegmentSize == 0)
                    break;  // ready
                Segment segment = new Segment(this,vSegments.size(),nSegmentSize);
                for (Block block: mBlocks.values()){
                    if (block.nSegmentId == vSegments.size())
                        segment.addBlock(block);
                }
                vSegments.add(segment);
                log(segment,"*** segment size: "+segment.size(),0);
                boolean bDoReinsertions = true;

                // get persistence rate of splitfile segments
                if (segment.size() > 1){
                    log(segment,"starting availability check for segment (n="+plugin.getIntProp("splitfile_test_size")+")",0);

                    // prove blocks
                    Vector<Block> vRequestedBlocks = new Vector();
                    for (int i=0; i<segment.size(); i++){
                        if (Math.random() < (double)plugin.getIntProp("splitfile_test_size")/segment.size()){

                            // wait for next free thread
                            while (nActiveSingleJobCount >= plugin.getIntProp("power")){
                                synchronized(this){
                                    this.wait(1000);
                                }
                                if (!isActive()) return;
                            }
                            checkFinishedSegments();

                            // fetch a block
                            vRequestedBlocks.add(segment.getBlock(i));
                            (new SingleFetch(this,segment.getBlock(i),true)).start();
                        }
                    }

                    // wait for all blocks
                    int nSuccessful = 0;
                    int nFailed = 0;
                    for (int i=0; i<vRequestedBlocks.size(); i++){
                        while (!vRequestedBlocks.get(i).bFetchDone){
                            synchronized(this){
                                this.wait(1000);
                            }
                            if (!isActive()) return;
                        }
                        checkFinishedSegments();
                        if (vRequestedBlocks.get(i).bFetchSuccessfull)
                            nSuccessful++;
                        else
                            nFailed++;
                    }

                    // calculate persistence rate
                    double nPersistenceRate = (double)nSuccessful/(nSuccessful+nFailed);
                    if (nPersistenceRate >= (double)plugin.getIntProp("splitfile_tolerance")/100){
                        bDoReinsertions = false;
                        segment.regFetchSuccess(nPersistenceRate);
                        updateSegmentStatistic(segment,true);
                        log(segment,"availability of segment ok: "+((int)(nPersistenceRate*100))+"% (approximated)",0,1);
                        log(segment,"-> segment not reinserted",0,1);                            
                    }else{
                        log(segment,"<b>availability of segment not ok: "+((int)(nPersistenceRate*100))+"% (approximated)</b>",0,1);
                        log(segment,"-> fetch all available blocks now",0,1);
                    }
                    
                    // get all available blocks and heal the segment
                    if (bDoReinsertions){

                        // get all blocks
                        vRequestedBlocks.clear();
                        for (int i=0; i<segment.size(); i++){
                            
                            // wait for next free thread
                            while (nActiveSingleJobCount >= plugin.getIntProp("power")){
                                synchronized(this){
                                    this.wait(1000);
                                }
                                if (!isActive()) return;
                            }
                            checkFinishedSegments();   
                            
                            // fetch next block that has not been fetched yet
                            if (!segment.getBlock(i).bFetchDone){
                                vRequestedBlocks.add(segment.getBlock(i));
                                SingleFetch fetch = new SingleFetch(this,segment.getBlock(i),true);
                                fetch.start();
                            }
                            
                        }

                        // wait for all blocks
                        for (int i=0; i<vRequestedBlocks.size(); i++){
                            while (!vRequestedBlocks.get(i).bFetchDone){
                                synchronized(this){
                                    this.wait(1000);
                                }
                                if (!isActive()) return;
                            }
                            checkFinishedSegments();   
                        }

                        // heal segment
                        // init
                        log(segment,"starting segment healing",0,1);
                        Bucket[] dataBlocks = new Bucket[segment.dataSize()];
                        Bucket[] checkBlocks = new Bucket[segment.checkSize()];
                        Bucket[] dataBlocksCopy = new Bucket[segment.dataSize()];    // copy-arrays for development only
                        Bucket[] checkBlocksCopy = new Bucket[segment.checkSize()];  // to compare original and restored blocks
                        for (int i=0; i<dataBlocks.length; i++){
                            if (segment.getDataBlock(i) != null){
                                dataBlocks[i] = segment.getDataBlock(i).bucket;
                                dataBlocksCopy[i] = dataBlocks[i];
                            }
                        }
                        for (int i=0; i<checkBlocks.length; i++){
                            if (segment.getCheckBlock(i) != null){
                                checkBlocks[i] = segment.getCheckBlock(i).bucket;
                                checkBlocksCopy[i] = checkBlocks[i];
                            }
                        }
                        
                        // development only (to prove healing)
                        /*
                        dataBlocks[0] = null;                        
                        dataBlocks[1] = null;
                        checkBlocks[0] = null;
                        checkBlocks[1] = null;
                        segment.getDataBlock(0).bFetchSuccessfull = false;
                        segment.getDataBlock(1).bFetchSuccessfull = false;
                        segment.getCheckBlock(0).bFetchSuccessfull = false;
                        segment.getCheckBlock(1).bFetchSuccessfull = false;
                        bDoReinsertions = true;
                        */

                        ClientContext context = plugin.pluginContext.clientCore.clientContext;
                        FECCallback fecCallBack = new FECCallback();
                        StandardOnionFECCodec codec = (StandardOnionFECCodec)StandardOnionFECCodec.getInstance(dataBlocks.length,checkBlocks.length);

                        // decode (= build the data blocks from all received blocks)
                        log(segment,"-> actual status:",0,2);
                        SplitfileBlock[] aSplitfileDataBlocks = new SplitfileBlock[dataBlocks.length];
                        for (int i=0; i<dataBlocks.length; i++){
                            aSplitfileDataBlocks[i] = new MinimalSplitfileBlock(0);
                            aSplitfileDataBlocks[i].assertSetData(dataBlocks[i]);
                            log(segment,"dataBlock_"+i,aSplitfileDataBlocks[i].getData());
                        }
                        SplitfileBlock[] aSplitfileCheckBlocks = new SplitfileBlock[checkBlocks.length];
                        for (int i=0; i<checkBlocks.length; i++){
                            aSplitfileCheckBlocks[i] = new MinimalSplitfileBlock(0);
                            aSplitfileCheckBlocks[i].assertSetData(checkBlocks[i]);
                            log(segment,"checkBlock_"+i,aSplitfileCheckBlocks[i].getData());
                        }
                        log(segment,"start decoding",0,1);
                        FECJob fecJob = new FECJob(codec,context.fecQueue,aSplitfileDataBlocks,aSplitfileCheckBlocks,CHKBlock.DATA_LENGTH,context.tempBucketFactory,fecCallBack,true,(short)2,false);  // (see freenet.client.async.SplitFileInserterSegment)
                        context.fecQueue.addToQueue(fecJob,codec,pr.getNode().db);
                        while (!fecCallBack.finished()){
                            synchronized(this){
                                this.wait(100);
                            }
                        }
                        if (fecCallBack.successful())
                            log(segment,"-> decoding successful",1,2);
                        else
                            log(segment,"-> decoding failed",1,2);

                        // encode (= build all data blocks  and check blocks from data blocks)
                        if (fecCallBack.successful()){
                            log(segment,"start encoding",0,1);
                            aSplitfileDataBlocks = fecCallBack.splitfileDataBlocks;
                            aSplitfileCheckBlocks = fecCallBack.splitfileCheckBlocks;
                            fecCallBack = new FECCallback();
                            fecJob = new FECJob(codec,context.fecQueue,aSplitfileDataBlocks,aSplitfileCheckBlocks,CHKBlock.DATA_LENGTH,context.tempBucketFactory,fecCallBack,false,(short)2,false);  // (see freenet.client.async.SplitFileInserterSegment)
                            context.fecQueue.addToQueue(fecJob,codec,pr.getNode().db);
                            while (!fecCallBack.finished()){
                                synchronized(this){
                                    this.wait(100);
                                }
                            }
                            aSplitfileDataBlocks = fecCallBack.splitfileDataBlocks;
                            aSplitfileCheckBlocks = fecCallBack.splitfileCheckBlocks;
                            if (fecCallBack.successful())
                                log(segment,"-> encoding successful",1,2);
                            else
                                log(segment,"-> encoding  failed",1,2);
                        }
                        
                        // finish
                        for (int i=0; i<aSplitfileDataBlocks.length; i++){
                            log(segment,"dataBlock_"+i,aSplitfileDataBlocks[i].getData());
                            segment.getDataBlock(i).bucket = aSplitfileDataBlocks[i].getData();
                            if (dataBlocks[i] == null && dataBlocksCopy[i] != null) // development only
                                log(segment,"original block equal to restored block: "+compareBuckets(aSplitfileDataBlocks[i].getData(),dataBlocksCopy[i]),2,2);
                        }
                        for (int i=0; i<aSplitfileCheckBlocks.length; i++){
                            log(segment,"checkBlock_"+i,aSplitfileCheckBlocks[i].getData());
                            segment.getCheckBlock(i).bucket = aSplitfileCheckBlocks[i].getData();
                            if (checkBlocks[i] == null && checkBlocksCopy[i] != null) // development only
                                log(segment,"original block equal to restored block: "+compareBuckets(aSplitfileCheckBlocks[i].getData(),checkBlocksCopy[i]),2,2);
                        }
                        if (fecCallBack.successful()){
                            updateSegmentStatistic(segment,true);
                            log(segment,"segment healing (FEC) successful, start with reinsertion",0,1);                        
                        }else{
                            updateSegmentStatistic(segment,false);
                            segment.bHealingNotPossible = true;
                            bDoReinsertions = false;
                            log(segment,"<b>segment healing (FEC) failed, do not reinsert</b>",0,1);
                        }

                    }
                }

                // start reinsertion
                if (bDoReinsertions){
                    
                    log(segment,"starting reinsertion",0,1);
                    segment.initInsert();
                    
                    for (int i=0; i<segment.size(); i++){
                        while (nActiveSingleJobCount >= plugin.getIntProp("power")){
                            synchronized(this){
                                this.wait(1000);
                            }
                            if (!isActive()) return;
                        }
                        checkFinishedSegments();
                        if (segment.size() > 1){
                            if (segment.getBlock(i).bFetchSuccessfull)
                                segment.regFetchSuccess(true);
                            else{
                                segment.regFetchSuccess(false);
                                (new SingleInsert(this,segment.getBlock(i))).start();                                
                            }
                        }else
                            (new SingleInsert(this,segment.getBlock(i))).start();
                    }
                    
                }
                
                // check if segments are finished
                checkFinishedSegments();
            }

            // wait for finishing all segments
            while (true){
                if (plugin.getIntProp("segment_"+nSiteId) == nMaxSegmentId)
                    break;
                synchronized(this){
                    this.wait(10000);
                }
                if (!isActive()) return;
                checkFinishedSegments();
            }

            // add to history
            if (plugin.getIntProp("blocks_"+nSiteId) > 0){
                int nPersistence = (int)((double)plugin.getSuccessValues(nSiteId)[0]/plugin.getIntProp("blocks_"+nSiteId)*100);
                String cHistory = plugin.getProp("history_"+nSiteId);
                String[] aHistory;
                if (cHistory == null)
                    aHistory = new String[]{};
                else
                    aHistory = cHistory.split(",");
                String cThisMonth = (new SimpleDateFormat("MM.yyyy")).format(new Date());
                boolean bNewMonth = true;
                if (cHistory != null && cHistory.indexOf(cThisMonth) != -1){
                    bNewMonth = false;
                    int nOldPersistence = Integer.valueOf(aHistory[aHistory.length-1].split("-")[1]);
                    nPersistence = Math.min(nPersistence,nOldPersistence);
                    aHistory[aHistory.length-1] = cThisMonth+"-"+nPersistence;
                }
                cHistory = ""; 
                for (int i=0; i<aHistory.length; i++){
                    if (cHistory.length() > 0) cHistory += ",";
                    cHistory += aHistory[i];
                }
                if (bNewMonth){
                    if (cHistory.length() > 0) cHistory += ",";
                    cHistory += cThisMonth+"-"+nPersistence;
                }
                plugin.setProp("history_"+nSiteId,cHistory);
                plugin.saveProp();
            }

            // start reinsertion of next site
            bReady = true;
            log("*** reinsertion finished ***",0,0);
            plugin.log("reinsertion finished for "+plugin.getProp("uri_"+nSiteId),1);
            int[] aIds = plugin.getIds();
            int i = -1;
            for (int j=0; j<aIds.length; j++){
                i = j;
                if (nSiteId == aIds[j])
                    break;
            }
            if (!isActive()) return;
            if (i < aIds.length-1)
                plugin.startReinserter(aIds[i+1]);
            else    
                plugin.startReinserter(aIds[0]);
 
        }catch(Exception e){
            plugin.log("Reinserter.run(): "+e.getMessage(),0);
        }
    }
    
    
    private boolean compareBuckets(Bucket bucket1, Bucket bucket2){   // for development only

        boolean bSuccess = true;
        InputStream stream1 = null;
        InputStream stream2 = null;
        try{
            stream1 = bucket1.getInputStream();
            stream2 = bucket2.getInputStream();
            int nByte;
            while ((nByte = stream1.read()) != -1){
                if (nByte != stream2.read()){
                    bSuccess = false;
                    break;
                }
            }
            if (stream2.read() != -1)
                bSuccess = false;
        }catch(Exception e){
            bSuccess = false;
        }
        try{
            if (stream1 != null)
                stream1.close();
            if (stream2 != null)
                stream2.close();
        }catch(Exception e){}
        return bSuccess;
        
    }
    
    
    private void checkFinishedSegments(){
        try{

            int nSegment;
            while((nSegment = plugin.getIntProp("segment_"+nSiteId)) < vSegments.size()-1){
                if (vSegments.get(nSegment+1).isFinished())
                    plugin.setIntProp("segment_"+nSiteId,nSegment+1);
                else
                    break;
            }
            plugin.saveProp();
            
        }catch(Exception e){
            plugin.log("Reinserter.checkFinishedSegments(): "+e.getMessage(),0);
        }
    }
    
    
    private void saveBlockUris(){
        try{
            
            File f = new File(plugin.getPluginDirectory()+plugin.getBlockListFilename(nSiteId));
            if (f.exists()) f.delete();
            RandomAccessFile file = new RandomAccessFile(f,"rw");
            file.setLength(0);
            for (Block block: mBlocks.values()){
                if (file.getFilePointer() > 0)
                    file.writeBytes("\n");
                String cType = "d";
                if (!block.bIsDataBlock)
                    cType = "c";
                file.writeBytes(block.uri.toString()+"#"+block.nSegmentId+"#"+block.nId+"#"+cType);
            }    
            file.close();
            
        }catch(Exception e){
            plugin.log("Reinserter.saveBlockUris(): "+e.getMessage(),0);
        }
    }
    
    
    private void loadBlockUris(){
        try{
            
            RandomAccessFile file = new RandomAccessFile(plugin.getPluginDirectory()+plugin.getBlockListFilename(nSiteId),"r");
            String cValues;
            while((cValues = file.readLine()) != null){
                String[] aValues = cValues.split("#");
                FreenetURI uri = new FreenetURI(aValues[0]);
                int nSegmentId = Integer.parseInt(aValues[1]);
                int nId = Integer.parseInt(aValues[2]);
                boolean bIsDataBlock = aValues[3].equals("d");
                mBlocks.put(uri,new Block(uri,nSegmentId,nId,bIsDataBlock));
            }  
            file.close();

        }catch(Exception e){
            plugin.log("Reinserter.loadBlockUris(): "+e.getMessage(),0);
        }
    }
    
    
    private void parseMetadata(FreenetURI uri, Metadata metadata, int nLevel){
        try{
            
            // activity flag
            if (!isActive()) return;
            
            
            // register uri
            registerBlockUri(uri,metadata,true,true,nLevel);
            
            
            // constructs top level simple manifest (= first action on a new uri)
            if (metadata == null){

                metadata = fetchManifest(uri,null,null);
                if (metadata == null){
                    log("no metadata",nLevel);
                    return;
                }    
                
            }
            
            
            // internal manifest (simple manifest)
            if (metadata.isSimpleManifest()){
                
                log("manifest ("+getMetadataType(metadata)+"): "+metadata.getResolvedName(),nLevel);
                HashMap<String,Metadata> mTargetList = null;
                try{
                    mTargetList = metadata.getDocuments();
                }catch(Exception e){}
                if (mTargetList != null){                 
                    for (Entry<String,Metadata> entry: mTargetList.entrySet()){
                        if (!isActive()) return;
                        // get document
                        Metadata target = entry.getValue();
                        // remember document name
                        target.resolve(entry.getKey());  
                        // parse document
                        parseMetadata(uri,target,nLevel+1);
                    }
                }    
                return;
                
            }    
            
  
            // redirect to submanifest
            if (metadata.isArchiveMetadataRedirect()){
                
                log("document ("+getMetadataType(metadata)+"): "+metadata.getResolvedName(),nLevel);
                Metadata subManifest = fetchManifest(uri,metadata.getArchiveType(),metadata.getArchiveInternalName());
                parseMetadata(uri,subManifest,nLevel);
                return;
                
            }
            
            
            // internal redirect
            if (metadata.isArchiveInternalRedirect()){
                
                log("document ("+getMetadataType(metadata)+"): "+metadata.getArchiveInternalName(),nLevel);                
                return;
                
            }
            
            
            // single file redirect with external key (only possible if archive manifest or simple redirect but not splitfile)
            if (metadata.isSingleFileRedirect()){  
                
                log("document ("+getMetadataType(metadata)+"): "+metadata.getResolvedName(),nLevel);
                FreenetURI targetUri = metadata.getSingleTarget();
                log("-> redirect to: "+targetUri,nLevel);
                registerManifestUri(targetUri,nLevel);
                registerBlockUri(targetUri,metadata,true,true,nLevel);
                return;

            }    
                
    
            // splitfile
            if (metadata.isSplitfile()){
                
                // splitfile type
                if (metadata.isSimpleSplitfile())
                    log("simple splitfile: "+metadata.getResolvedName(),nLevel);
                else
                    log("splitfile (not simple): "+metadata.getResolvedName(),nLevel);
                
                // register blocks
                Metadata metadata2 = (Metadata)metadata.clone();
                SplitFileSegmentKeys[] segmentKeys = metadata2.grabSegmentKeys(null);
                for (int i=0; i<segmentKeys.length; i++){
                    int nDataBlocks = segmentKeys[i].getDataBlocks();
                    int nCheckBlocks = segmentKeys[i].getCheckBlocks();
                    log("segment_"+i+": "+(nDataBlocks+nCheckBlocks)+" (data="+nDataBlocks+", check="+nCheckBlocks+")",nLevel+1);                    
                    for (int j=0; j<nDataBlocks+nCheckBlocks; j++){
                        FreenetURI splitUri = segmentKeys[i].getKey(j,null,false).getURI();
                        log("block: "+splitUri,nLevel+1);
                        registerBlockUri(splitUri,metadata,(j==0),(j<nDataBlocks),nLevel+1);
                    }    
                }
                
                // create metadata from splitfile (if not simple splitfile)
                if (!metadata.isSimpleSplitfile()){
                    FetchContext fetchContext = pr.getHLSimpleClient().getFetchContext();
                    FetchWaiter fetchWaiter = new FetchWaiter();                
                    freenet.client.async.ClientContext clientContext = pr.getNode().clientCore.clientContext;
                    List<Compressor.COMPRESSOR_TYPE> decompressors = new LinkedList<Compressor.COMPRESSOR_TYPE>();
                    if (metadata.isCompressed()){
                        log("is compressed: "+metadata.getCompressionCodec(),nLevel+1);                    
                        decompressors.add(metadata.getCompressionCodec());
                    }else
                        log("is not compressed",nLevel+1);
                    SplitfileGetCompletionCallback cb = new SplitfileGetCompletionCallback(fetchWaiter);
                    VerySimpleGetter vsg = new VerySimpleGetter((short)2,null,(RequestClient)pr.getHLSimpleClient());
    		    SplitFileFetcher sf = new SplitFileFetcher(metadata,cb,vsg,fetchContext,false,true,decompressors,metadata.getClientMetadata(),null,0,0,metadata.topDontCompress,metadata.topCompatibilityMode,null,clientContext);
                    sf.schedule(null,clientContext);
                    //fetchWaiter.waitForCompletion();
                    while (cb.getDecompressedData() == null){   // workaround because in some cases fetchWaiter.waitForCompletion() never finished
                        if (!isActive()) return;
                        synchronized(this){
                            wait(100);
                        }
                    }
                    sf.cancel(null,clientContext);
                    metadata = fetchManifest(cb.getDecompressedData(),null,null);   
                    parseMetadata(null,metadata,nLevel+1);
                }    
                
            }

        }catch(Exception e){
            plugin.log("Reinserter.parseMetadata(): "+e.getMessage());
        }
    }
    
    
    private String getMetadataType(Metadata metadata){
        try{
            
            String cTypes = "";
            
            if (metadata.isArchiveManifest())
                cTypes += ",AM";                        
            if (metadata.isSimpleManifest())
                cTypes += ",SM";                        

            if (metadata.isArchiveInternalRedirect())
                cTypes += ",AIR";                        
            if (metadata.isArchiveMetadataRedirect())
                cTypes += ",AMR";                        
            if (metadata.isSymbolicShortlink())
                cTypes += ",SSL";                        
            
            if (metadata.isSingleFileRedirect())
                cTypes += ",SFR";                        
            if (metadata.isSimpleRedirect())
                cTypes += ",SR";                                    
            if (metadata.isMultiLevelMetadata())
                cTypes += ",MLM";                        

            if (metadata.isSplitfile())
                cTypes += ",SF";
            if (metadata.isSimpleSplitfile())
                cTypes += ",SSF";                        

            if (cTypes.length() > 0)
                cTypes = cTypes.substring(1);
            
            return cTypes;
            
        }catch(Exception e){
            plugin.log("Reinserter.getMetadataType(): "+e.getMessage());
            return null;
        }
    }
    
    
    private class SplitfileGetCompletionCallback implements freenet.client.async.GetCompletionCallback{

        private FetchWaiter fetchWaiter;
        private byte[] aDecompressedSplitFileData = null;
        
        public SplitfileGetCompletionCallback(FetchWaiter fetchWaiter){
            this.fetchWaiter = fetchWaiter;
        }
        
        public void onFailure(FetchException e, ClientGetState state, ObjectContainer container, ClientContext context) {
            fetchWaiter.onFailure(e,null,null);
        }
        
        public void onSuccess(StreamGenerator streamGenerator,
                              ClientMetadata clientMetadata,
                              List<? extends Compressor> decompressors,
                              ClientGetState state, ObjectContainer container,
                              ClientContext context)
        {
            try{

                // get data
                ByteArrayOutputStream rawOutStream = new ByteArrayOutputStream();
                streamGenerator.writeTo(rawOutStream,null,null);
                rawOutStream.close();
                byte[] aCompressedSplitFileData = rawOutStream.toByteArray();

                // decompress (if necessary)
                if (decompressors.size() > 0){
                    ByteArrayInputStream compressedInStream = new ByteArrayInputStream(aCompressedSplitFileData);
                    ByteArrayOutputStream decompressedOutStream = new ByteArrayOutputStream();
                    decompressors.get(0).decompress(compressedInStream,decompressedOutStream,Integer.MAX_VALUE,-1);
                    compressedInStream.close();
                    decompressedOutStream.close();
                    aDecompressedSplitFileData = decompressedOutStream.toByteArray();
                    fetchWaiter.onSuccess(null,null,null);
                }else
                    aDecompressedSplitFileData = aCompressedSplitFileData;
                
            }catch(Exception e){
                plugin.log("SplitfileGetCompletionCallback.onSuccess(): "+e.getMessage());
            }
        }
        
        public byte[] getDecompressedData(){
            return aDecompressedSplitFileData;
        }

        public void onBlockSetFinished(ClientGetState state, ObjectContainer container, ClientContext context){}
        public void onExpectedMIME(ClientMetadata metadata, ObjectContainer container, ClientContext context){}
        public void onExpectedSize(long size, ObjectContainer container, ClientContext context){}
        public void onFinalizedMetadata(ObjectContainer container){}
        public void onTransition(ClientGetState oldState, ClientGetState newState, ObjectContainer container){}
        public void onExpectedTopSize(long size, long compressed, int blocksReq, int blocksTotal, ObjectContainer container, ClientContext context){}
        public void onHashes(HashResult[] hashes, ObjectContainer container, ClientContext context){}
        public void onSplitfileCompatibilityMode(CompatibilityMode min, CompatibilityMode max, byte[] customSplitfileKey, boolean compressed, boolean bottomLayer, boolean definitiveAnyway, ObjectContainer container, ClientContext context){}
    }

    
    private class VerySimpleGetter extends ClientRequester{
        private FreenetURI uri;

        public VerySimpleGetter(short priorityclass, FreenetURI uri, RequestClient client){
                super(priorityclass,client);
                this.uri = uri;
        }

        public FreenetURI getURI() {
                return uri;
        }

        public boolean isFinished(){
                return false;
        }

        public void cancel(ObjectContainer container, ClientContext context) {}
        public void notifyClients(ObjectContainer container, ClientContext context) {}
        public void onTransition(ClientGetState oldState, ClientGetState newState, ObjectContainer container) {}
        protected void innerToNetwork(ObjectContainer container, ClientContext context) {}
    }
    
    
    public Metadata fetchManifest(FreenetURI uri, ARCHIVE_TYPE archiveType, String cManifestName){
        try{
            
            // init
            uri = normalizeUri(uri);
            if (uri.isCHK())
                uri.getExtra()[2] = 0;  // deactivate control flag
            
            // fetch raw data
            HighLevelSimpleClient hlsc = pr.getHLSimpleClient();
            FetchContext fetchContext = hlsc.getFetchContext();
            fetchContext.returnZIPManifests = true;
            FetchWaiter fetchWaiter = new FetchWaiter();
            hlsc.fetch(uri,-1,(RequestClient)hlsc,fetchWaiter,fetchContext);
            FetchResult result = fetchWaiter.waitForCompletion();
            
            return fetchManifest(result.asByteArray(),archiveType,cManifestName);
            
        }catch(Exception e){
            plugin.log("Reinserter.fetchManifest(uri): "+e.getMessage());
            return null;
        }
    }
    
    
    public Metadata fetchManifest(byte[] aData, ARCHIVE_TYPE archiveType, String cManifestName){
        try{
            
            // init
            ByteArrayInputStream fetchedDataStream = new ByteArrayInputStream(aData);
            Metadata metadata = null;
            if (cManifestName == null)
                cManifestName = ".metadata";
            
            // unzip and construct metadata
            try{
                
                InputStream inStream = null;
                String cEntryName = null;
                
                // get archive stream (try if archive type unknown)
                if (archiveType == ARCHIVE_TYPE.TAR || archiveType == null){
                    try{
                        inStream = new TarInputStream(fetchedDataStream);
                        cEntryName = ((TarInputStream)inStream).getNextEntry().getName();
                        archiveType = ARCHIVE_TYPE.TAR;
                    }catch(Exception e){}    
                }
                if (archiveType == ARCHIVE_TYPE.ZIP || archiveType == null){
                    try{
                        inStream = new ZipInputStream(fetchedDataStream);
                        cEntryName = ((ZipInputStream)inStream).getNextEntry().getName();
                        archiveType = ARCHIVE_TYPE.ZIP;
                    }catch(Exception e){}    
                }
                
                // construct metadata
                while (cEntryName != null){
                    if (cEntryName.equals(cManifestName)){
                        byte[] buf = new byte[32768];
                        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                        int nBytes;
                        while ((nBytes = inStream.read(buf)) > 0){
                            outStream.write(buf,0,nBytes);
                        }
                        outStream.close();
                        metadata = Metadata.construct(outStream.toByteArray());
                        break;
                    }
                    if (archiveType == ARCHIVE_TYPE.TAR)
                        cEntryName = ((TarInputStream)inStream).getNextEntry().getName();
                    else    
                        cEntryName = ((ZipInputStream)inStream).getNextEntry().getName();
                }
                
            }catch(Exception e){}
        
            // if not tar or zip then try to construct metadata directly
            if (metadata == null){
                try{
                    metadata = Metadata.construct(aData);
                }catch(Exception e){}
            }    
            
            // finish
            fetchedDataStream.close();
            if (metadata != null){
                if (archiveType != null)
                    cManifestName += " ("+archiveType.name()+")";
                metadata.resolve(cManifestName);
            }    
            return metadata;
                
        }catch(Exception e){
            plugin.log("Reinserter.fetchManifest(data): "+e.getMessage());
            return null;
        }
    }
    
    
    private FreenetURI updateUsk(FreenetURI uri){
        try{
            
            HighLevelSimpleClient hlsc = pr.getHLSimpleClient();
            FetchContext fetchContext = hlsc.getFetchContext();
            fetchContext.returnZIPManifests = true;
            FetchWaiter fetchWaiter = new FetchWaiter();
            try{
            hlsc.fetch(uri,-1,(RequestClient)hlsc,fetchWaiter,fetchContext);
            fetchWaiter.waitForCompletion();
            }catch(freenet.client.FetchException e){
                if (e.getMode() == e.PERMANENT_REDIRECT)
                    uri = updateUsk(e.newURI);
            }
            return uri;
        
        }catch(Exception e){
            plugin.log("Reinserter.updateUsk(): "+e.getMessage());
            return null;
        }
    }


    private FreenetURI normalizeUri(FreenetURI uri){
        try{
            
            if (uri.isUSK())
                uri = uri.sskForUSK();
            if (uri.hasMetaStrings())
                uri = uri.setMetaString(null);
            return uri;
            
        }catch(Exception e){
            plugin.log("Reinserter.normalizeUri(): "+e.getMessage(),0);
            return null;
        }
    }
    
    
    private void registerManifestUri(FreenetURI uri, int nLevel){
        try{
            
            uri = normalizeUri(uri);
            if (mManifestURIs.containsKey(uri)){
                log("-> already registered",nLevel,2);
            }else{
                mManifestURIs.put(uri,null);
                if (nLevel != -1)
                    log("-> registered",nLevel,2);
            }            
            
        }catch(Exception e){
            plugin.log("Reinserter.registerManifestUri(): "+e.getMessage(),0);
        }
    }
    
    
    private void registerBlockUri(FreenetURI uri, Metadata metadata, boolean bNewSegment, boolean bIsDataBlock, int nLevel){
        try{

            if (uri != null){   // uri is null if metadata is created from splitfile
                
                // no reinsertion for SSK but go to sublevel
                if (!uri.isCHK()){
                    log("-> no reinsertion of USK, SSK or KSK",nLevel,2);

                // check if uri already reinserted during this session
                }else if (mBlocks.containsKey(normalizeUri(uri))){
                    log("-> already registered",nLevel,2);

                // register
                }else{
                    if (bNewSegment){
                        nParsedSegmentId++;
                        nParsedBlockId = -1;
                    }
                    uri = normalizeUri(uri); 
                    mBlocks.put(uri,new Block(uri,nParsedSegmentId,++nParsedBlockId,bIsDataBlock));
                    log("-> registered",nLevel,2);
                }

            }

        }catch(Exception e){
            plugin.log("Reinserter.registerBlockUri(): "+e.getMessage(),0);
        }
    }
    
    
    public void registerBlockFetchSuccess(Block block){
        try{

            vSegments.get(block.nSegmentId).regFetchSuccess(block.bFetchSuccessfull);
            
        }catch(Exception e){
            plugin.log("Reinserter.registerBlockSuccess(): "+e.getMessage(),0);
        }
    }
    
    
    public synchronized void updateSegmentStatistic(Segment segment, boolean bSuccess){
        try{

            String cSuccess = plugin.getProp("success_segments_"+nSiteId);
            if (bSuccess)
                cSuccess = cSuccess.substring(0,segment.nId)+"1"+cSuccess.substring(segment.nId+1);
            plugin.setProp("success_segments_"+nSiteId,cSuccess);
            plugin.saveProp();
            
        }catch(Exception e){
            plugin.log("Reinserter.updateSegmentStatistic(): "+e.getMessage(),0);
        }
    }

    
    public synchronized void updateBlockStatistic(int nId, int nSuccess, int nFailed){
        try{

            String[] aSuccess = plugin.getProp("success_"+nSiteId).split(",");
            aSuccess[nId*2] = String.valueOf(nSuccess);
            aSuccess[nId*2+1] = String.valueOf(nFailed);
            StringBuffer success = new StringBuffer();
            for (int i=0; i<aSuccess.length; i++){
                if (i > 0)
                    success.append(",");
                success.append(aSuccess[i]);
            }
            plugin.setProp("success_"+nSiteId,success.toString());
            plugin.saveProp();

        }catch(Exception e){
            plugin.log("Reinserter.updateBlockStatistic(): "+e.getMessage(),0);
        }
    }

        
    public void terminate(){
        try{

            if (!bReady && isActive() && isAlive()){
                plugin.log("stop reinserter ("+nSiteId+")",1);
                log("*** stopped ***",0);
                nLastActivityTime = Integer.MIN_VALUE;
                plugin.setIntProp("active",-1);
                plugin.saveProp();
            }
            
        }catch(Exception e){
            plugin.log("Reinserter.terminate(): "+e.getMessage(),0);
        }
    }
    
    
    public boolean isActive(){
        if (nLastActivityTime != Integer.MIN_VALUE)
            nLastActivityTime = System.currentTimeMillis();
        long nDelay = (System.currentTimeMillis()-nLastActivityTime)/60/1000;
        return (nDelay < SingleJob.MAX_LIFETIME+5);
    }
    
    
    public void log(int nSegmentId, String cMessage, int nLevel, int nLogLevel){
        String cPrefix = "";
        for (int i=0; i<nLevel; i++){
            cPrefix += "    ";
        }
        if (nSegmentId != -1)
            cPrefix = "("+nSegmentId+") "+cPrefix;
        try{
            if (plugin.getIntProp("log_links") == 1){
                int nKeyPos = cMessage.indexOf("K@");
                if (nKeyPos != -1){
                    nKeyPos = nKeyPos-2;
                    int nKeyPos2 = Math.max(cMessage.indexOf(" ",nKeyPos),cMessage.indexOf("<",nKeyPos));
                    if (nKeyPos2 == -1)
                        nKeyPos2 = cMessage.length();
                    String cKey = cMessage.substring(nKeyPos,nKeyPos2);
                    cMessage = cMessage.substring(0,nKeyPos)+"<a href=\"/"+cKey+"\">"+cKey+"</a>"+cMessage.substring(nKeyPos2);
                }
            }
        }catch(Exception e){}
        plugin.log(plugin.getLogFilename(nSiteId),cPrefix+cMessage,nLogLevel);
    }

    
    public void log(Segment segment, String cMessage, int nLevel, int nLogLevel){
        log(segment.nId,cMessage,nLevel,nLogLevel);
    }
    
    
    public void log(Segment segment, String cMessage, int nLevel){
        log(segment,cMessage,nLevel,1);
    }
    
    
    public void log(String cMessage, int nLevel, int nLogLevel){
        log(-1,cMessage,nLevel,nLogLevel);
    }
    
    
    public void log(String cMessage, int nLevel){
        log(-1,cMessage,nLevel,1);
    }
    
    
    public void log(Segment segment, String cMessage, Object obj){
        if (obj != null)
            log(segment,cMessage+" = ok",1,2);
        else
            log(segment,cMessage+" = null",1,2);
    }
    
    
    public void clearLog(){
        plugin.clearLog(plugin.getLogFilename(nSiteId));
    }
    
    
    private class FECCallback implements freenet.client.FECCallback{
        
        private byte nStatus = 0;
        public SplitfileBlock[] splitfileDataBlocks;
        public SplitfileBlock[] splitfileCheckBlocks;
        
	public void onEncodedSegment(ObjectContainer container, ClientContext context, FECJob job, Bucket[] dataBuckets, Bucket[] checkBuckets, SplitfileBlock[] dataBlocks, SplitfileBlock[] checkBlocks){
            this.splitfileDataBlocks = dataBlocks;
            this.splitfileCheckBlocks = checkBlocks;
            nStatus = 1;
        }

	public void onDecodedSegment(ObjectContainer container, ClientContext context, FECJob job, Bucket[] dataBuckets, Bucket[] checkBuckets, SplitfileBlock[] dataBlocks, SplitfileBlock[] checkBlocks){
            onEncodedSegment(null,null,null,null,null,dataBlocks,checkBlocks);
        }

	public void onFailed(Throwable t, ObjectContainer container, ClientContext context){
            log(vSegments.lastElement().nId,"FEC failed: "+t.getMessage(),1,2);
            nStatus = -1;
        }
        
        public boolean finished(){
            return nStatus != 0;
        }
        
        public boolean successful(){
            return nStatus == 1;
        }
        
    }
    
    
    private class ActivityGuard extends Thread{

        private Reinserter reinserter;
        
        public ActivityGuard(Reinserter reinserter){
            this.reinserter = reinserter;
            nLastActivityTime = System.currentTimeMillis();
        }
        
        public synchronized void run(){
            try{

                while(reinserter.isActive()){
                    wait(100);
                }
                reinserter.terminate();
                long nStopCheckBegin = System.currentTimeMillis();
                while (reinserter.isAlive() && nStopCheckBegin > System.currentTimeMillis()-30*60*60*1000){
                    try{
                        wait(100);
                    }catch(Exception e){}
                }
                if (!reinserter.isAlive())
                    plugin.log("reinserter stopped ("+nSiteId+")");
                else
                    plugin.log("reinserter not stopped - stop was indicated 30 minutes before ("+nSiteId+")");

            }catch(Exception e){
                plugin.log("Reinserter.ActivityGuard.run(): "+e.getMessage(),0);
            }
        }
        
    }    
    
    
}
