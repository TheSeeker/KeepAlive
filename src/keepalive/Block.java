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

import freenet.keys.FreenetURI;
import freenet.support.api.Bucket;


public class Block {
    
    
    FreenetURI uri;
    int nSegmentId;
    int nId;
    boolean bIsDataBlock;
    Bucket bucket;
    boolean bFetchDone; // done but not necessarily successful
    boolean bInsertDone; // done but not necessarily successful
    boolean bFetchSuccessfull;
    boolean bInsertSuccessfull;
    String cResultLog;
    
    
    public Block(FreenetURI uri, int nSegmentId, int nId, boolean bIsDataBlock){
        this.uri = uri;
        this.nSegmentId = nSegmentId;
        this.nId = nId;
        this.bIsDataBlock = bIsDataBlock;
    }
    
    
    void setResultLog(String cResult){
        cResultLog = cResult;
    }
    
}
