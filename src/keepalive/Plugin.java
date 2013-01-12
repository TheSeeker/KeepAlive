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
import freenet.pluginmanager.PluginRespirator;
import java.io.File;
import java.net.URLDecoder;
import java.util.Vector;
import pluginbase.PluginBase;


public class Plugin extends PluginBase{

    
    AdminPage adminPage;
    Reinserter reinserter;
    long nPropSavingTimestamp;
    
    
    public Plugin(){
        super("KeepAlive","Keep Alive","prop.txt");
        setVersion("0.3.2");
        addPluginToMenu("Keep Alive","Reinsert sites and files in the background");
        clearLog();
    }
    
    
    public void runPlugin(PluginRespirator pr){
        super.runPlugin(pr);
        try{
            
            // migrate from 0.2 to 0.3
            if (getProp("version") == null || !getProp("version").substring(0,3).equals("0.3")){
                // remove boost params
                int[] aIds = getIds();
                for (int i=0; i<aIds.length; i++){
                    removeProp("boost_"+aIds[i]);
                }
                // empty all block list
                for (int i=0; i<aIds.length; i++){
                    setProp("blocks_"+aIds[i],"?");
                }
                setProp("version","0.3");
            }
            
            // initial values
            setIntProp("loglevel",1);
            if (getProp("ids") == null)
                setProp("ids","");
            if (getProp("power") == null)
                setIntProp("power",5);
            if (getProp("active") == null)
                setIntProp("active",-1);
            if (getProp("splitfile_tolerance") == null)
                setIntProp("splitfile_tolerance",70);
            if (getProp("splitfile_test_size") == null)
                setIntProp("splitfile_test_size",50);
            if (getProp("log_links") == null)
                setIntProp("log_links",1);
            if (getProp("log_utc") == null)
                setIntProp("log_utc",1);
            saveProp();
            
            if (getIntProp("log_utc") == 1){
                setTimezoneUTC();                
            }

            // build page and menu
            adminPage = new AdminPage(this);
            addPage(adminPage);
            addMenuItem("Documentation","Go to the documentation site","/USK@l9wlbjlCA7kfcqzpBsrGtLoAB4-Ro3vZ6q2p9bQ~5es,bGAKUAFF8UryI04sxBKnIQSJWTSa08BDS-8jmVQdE4o,AQACAAE/keepalive/10",true);

            // start reinserter
            if (getIntProp("active") != -1)
                startReinserter(getIntProp("active"));
            
        }catch(Exception e){
            log("Plugin.runPlugin(): "+e.getMessage(),0);
        }
    }
    
    
    public void startReinserter(int nSiteId){
        try{

            (new Reinserter(this,nSiteId)).start();
            
        }catch(Exception e){
            log("Plugin.startReinserter(): "+e.getMessage(),0);
        }    
    }
    
    
    public void stopReinserter(){
        try{

            if (reinserter != null)
                reinserter.terminate();

        }catch(Exception e){
            log("Plugin.stopReinserter(): "+e.getMessage(),0);
        }    
    }
    
    
    public int[] getIds(){
        try{
            
            if (getProp("ids") == null || getProp("ids").equals(""))
                return new int[]{};
            else{
                String[] aIds = getProp("ids").split(",");
                int[] aIntIds = new int[aIds.length];
                for (int i=0; i<aIntIds.length; i++){
                    aIntIds[i] = Integer.parseInt(aIds[i]);
                }
                return aIntIds;
            }    
        
        }catch(Exception e){
            log("Plugin.getIds(): "+e.getMessage(),0);
            return null;
        }   
    }
    
    
    public int[] getSuccessValues(int nSiteId){
        try{
            
            // available blocks
            int nSuccess = 0;
            int nFailed = 0;
            String[] aSuccess = getProp("success_"+nSiteId).split(",");
            if (aSuccess.length >= 2){
                for (int i=0; i<aSuccess.length; i+=2){
                    nSuccess += Integer.parseInt(aSuccess[i]);
                    nFailed += Integer.parseInt(aSuccess[i+1]);
                }
            }
            
            // available segments
            int nAvailableSegments = 0;
            String cAvailableSegments = getProp("success_segments_"+nSiteId);
            if (cAvailableSegments != null){
                for (int i=0; i<=getIntProp("segment_"+nSiteId); i++){
                    if (cAvailableSegments.charAt(i) == '1')
                        nAvailableSegments++;
                }
            }
            
            return new int[]{nSuccess,nFailed,nAvailableSegments};
        
        }catch(Exception e){
            log("Plugin.getSuccessValues(): "+e.getMessage(),0);
            return null;
        }
    }
    
    
    public String getLogFilename(int nSiteId){
            return "log"+nSiteId+".txt";
    }
    
    
    public String getBlockListFilename(int nSiteId){
            return "keys"+nSiteId+".txt";
    }
    
    
    public void saveProp(){
        if (nPropSavingTimestamp < System.currentTimeMillis()-10*1000){
            super.saveProp();
            nPropSavingTimestamp = System.currentTimeMillis();
        }
    }
    
    
    public void terminate(){
        super.terminate();
        if (reinserter != null)
           reinserter.terminate();
        log("plugin terminated",0);
    }
    
    
}
