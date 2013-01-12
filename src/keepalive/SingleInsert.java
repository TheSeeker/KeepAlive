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

import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.HighLevelSimpleClientImpl;
import freenet.client.InsertBlock;
import freenet.client.InsertContext;
import freenet.client.InsertException;
import freenet.keys.FreenetURI;
import freenet.support.compress.Compressor;


public class SingleInsert extends SingleJob{

    
    public SingleInsert(Reinserter reinserter, Block block){
        super(reinserter,"insertion",block);
    }

    
    public void run(){
        super.run();
        try{

            HighLevelSimpleClientImpl hlsc = (HighLevelSimpleClientImpl)plugin.pluginContext.hlsc;
            FreenetURI fetchUri = block.uri.clone();
            block.bInsertDone = false;
            block.bInsertSuccessfull = false;

            // modify the control flag of the URI to get always the raw data
            byte[] aExtra = fetchUri.getExtra();
            aExtra[2] = 0;
            
            // get the compression algorithm of the block
            String cCompressor = null;
            if (aExtra[4] >= 0)
                cCompressor = Compressor.COMPRESSOR_TYPE.getCompressorByMetadataID((short)aExtra[4]).name;
            else
                cCompressor = "none";
            
            // fetch
            if (block.bucket == null){
                SingleFetch singleFetch = new SingleFetch(reinserter,block,false);
                singleFetch.start();
                singleFetch.join();
                if (!reinserter.isActive()) return;
            }
            
            // insert
            if (block.bucket != null){
                FreenetURI insertUri = null;

                try{
                    
                    InsertBlock insertBlock = new InsertBlock(block.bucket,null,fetchUri);                                
                    InsertContext insertContext = hlsc.getInsertContext(true);
                    if (cCompressor != null || !cCompressor.equals("none"))
                        insertContext.compressorDescriptor = cCompressor;
                    if (aExtra[1] == 2) // switch to crypto_algorithm  2 (instead of using the new one that is introduced since 1416)
                        insertContext.setCompatibilityMode(InsertContext.CompatibilityMode.COMPAT_1255);
                    insertUri = hlsc.insert(insertBlock,false,null,false,(short)2,insertContext,fetchUri.getCryptoKey());

                    // insert finished
                    if (!reinserter.isActive()) return;
                    int nSiteId = plugin.getIntProp("reinserter_site_id");
                    if (insertUri != null){
                        if (fetchUri.equals(insertUri)){
                            block.bInsertSuccessfull = true;
                            block.setResultLog("-> inserted: "+insertUri.toString());
                        }else
                            block.setResultLog("-> insertion failed - different uri: "+insertUri.toString());
                    }else
                        block.setResultLog("-> insertion failed");

                }catch(InsertException e){
                    block.setResultLog("-> insertion error: "+e.getMessage());
                }   
                
            }else
                block.setResultLog("-> insertion failed: fetch failed");
            
            // reg success if single-block-segment
            Segment segment = reinserter.vSegments.get(block.nSegmentId);
            if (segment.size() == 1)
                reinserter.updateSegmentStatistic(segment,block.bInsertSuccessfull);
            
            // finish
            block.bInsertDone = true;
            finish();

        }catch(Exception e){
            plugin.log("SingleInsert.run(): "+e.getMessage(),0);
        }
    } 
    
    
}
