/*
 * Plugin Base Package
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


package pluginbase;

import pluginbase.de.todesbaum.util.freenet.fcp2.Connection;
import java.io.ByteArrayInputStream;
import java.io.InputStream;


public class FcpCommands extends FcpCommandBase{

    
    public FcpCommands(Connection fcpConnection, PageBase page){
        super(fcpConnection,page);
    }

    
    public void sendGenerateSSK(String cId) throws Exception{
        try{
            
            init("GenerateSSK",cId);
            send();
            
        }catch(Exception e){
            throw new Exception("FcpCommand.sendGenerateSSK(): "+e.getMessage());
        }    
    }
    
    
    public void sendClientGet(String cId, String cUri) throws Exception{
        try{
            
            init("ClientGet",cId);
            field("URI",cUri);
            send();
            
        }catch(Exception e){
            throw new Exception("FcpCommand.sendClientGet(): "+e.getMessage());
        }    
    }

    
    public void sendClientPut(String cId, String cUri, InputStream dataStream, int nDataLength) throws Exception{
        try{
            
            init("ClientPut",cId);
            field("URI",cUri);
            field("DataLength",nDataLength);
            send(dataStream,nDataLength);
            
        }catch(Exception e){
            throw new Exception("FcpCommand.sendClientPut(): "+e.getMessage());
        }    
    }

    
    public void sendClientPut(String cId, String cUri, String cContent) throws Exception{
        try{
            
            byte[] aContent = cContent.getBytes("UTF-8");
            ByteArrayInputStream dataStream = new ByteArrayInputStream(aContent);
            sendClientPut(cId,cUri,dataStream,aContent.length);
            
        }catch(Exception e){
            throw new Exception("FcpCommand.sendClientPut(): "+e.getMessage());
        }    
    }
    
    
}
