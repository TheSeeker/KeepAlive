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
import java.io.File;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import pluginbase.PageBase;

public class AdminPage extends PageBase {

	Plugin kaPlugin;

	public AdminPage(Plugin plugin) {
		super("", "Keep Alive", plugin, true);
		this.kaPlugin = plugin;
		addPageToMenu("Start reinsertion of sites", "Add or remove sites you like to reinsert");
	}

	@Override
	protected void handleRequest() {
		try {

			// start reinserter
			if (getParam("start") != null) {
				kaPlugin.startReinserter(getIntParam("start"));
			}

			// stop reinserter
			if (getParam("stop") != null) {
				kaPlugin.stopReinserter();
			}

			// modify power
			if (getParam("modify_power") != null) {
				setIntPropByParam("power", 1);
				saveProp();
			}

			// modify splitfile tolerance
			if (getParam("splitfile_tolerance") != null) {
				setIntPropByParam("splitfile_tolerance", 0);
				saveProp();
			}

			// modify splitfile tolerance
			if (getParam("splitfile_test_size") != null) {
				setIntPropByParam("splitfile_test_size", 10);
				saveProp();
			}

			// modify log level
			if (getParam("modify_loglevel") != null || getParam("show_log") != null) {
				setIntPropByParam("loglevel", 0);
				saveProp();
			}

			// clear logs
			if (getParam("clear_logs") != null) {
				kaPlugin.clearAllLogs();
			}

			// clear history
			if (getParam("clear_history") != null) {
				removeProp("history_" + getParam("clear_history"));
			}

			// add uris
			if (getParam("uris") != null) {
				String[] splitURIs = getParam("uris").split("\n");
				for (String splitURI : splitURIs) {
					// validate
					String cUriOrig = URLDecoder.decode(splitURI, "UTF8").trim();
					if (cUriOrig.equals("")) {
						continue;  //ignore blank lines.
					}
					String cUri = cUriOrig;
					int nBegin = cUri.indexOf("@") - 3;
					if (nBegin > 0) {
						cUri = cUri.substring(nBegin);
					}
					boolean bValid = true;
					try {
						FreenetURI uri = new FreenetURI(cUri);
						cUri = uri.toString();
					} catch (MalformedURLException e) {
						bValid = false;
						addBox("URI not valid!", "You have typed:<br><br>" + cUriOrig);
					}

					// add if not already on the list.
					if (bValid && !isDuplicate(cUri)) {
						int[] aIds = kaPlugin.getIds();
						int nId;
						if (aIds.length == 0) {
							nId = 0;
						} else {
							nId = aIds[aIds.length - 1] + 1;
						}
						setProp("ids", getProp("ids") + nId + ",");
						setProp("uri_" + nId, cUri);
						setProp("blocks_" + nId, "?");
						setProp("success_" + nId, "");
						setIntProp("segment_" + nId, -1);
					}
				}
			}

			// remove uri
			if (getParam("remove") != null) {

				// stop reinserter
				int nId = getIntParam("remove");
				if (nId == kaPlugin.getIntProp("active")) {
					kaPlugin.stopReinserter();
				}

				// remove log and key files
				File file = new File(kaPlugin.getPluginDirectory() + kaPlugin.getLogFilename(nId));
				if (file.exists()) {
					file.delete();
				}
				file = new File(kaPlugin.getPluginDirectory() + kaPlugin.getBlockListFilename(nId));
				if (file.exists()) {
					file.delete();
				}

				// remove items
				removeProp("uri_" + nId);
				removeProp("blocks_" + nId);
				removeProp("success_" + nId);
				removeProp("success_segments_" + nId);
				removeProp("segment_" + nId);
				removeProp("history_" + nId);
				String cIds = ("," + getProp("ids")).replaceAll("," + nId + ",", ",");
				setProp("ids", cIds.substring(1));
				saveProp();

			}

			// unsupported keys box
			int[] aIds = kaPlugin.getIds();
			String cZeroBlockSites = "";
			for (int i = 0; i < aIds.length; i++) {
				if (getProp("blocks_" + aIds[i]).equals("0")) {
					if (cZeroBlockSites.length() > 0) {
						cZeroBlockSites += "<br>";
					}
					cZeroBlockSites += getProp("uri_" + aIds[i]);
				}
			}
			if (cZeroBlockSites.length() > 0) {
				addBox("Unsupported keys", html("unsupported_keys").replaceAll("#", cZeroBlockSites));
			}

			// sites box
			String cHtml = html("add_key") + "<br><table><tr style=\"text-align:center;\"><td style='border:0'></td><td>total<br>blocks</td><td>available<br>blocks</td><td>missed<br>blocks</td><td>blocks<br>availability</td><td>segments<br>availability</td><td style='border:0'></td><td style='border:0'></td></tr>";
			for (int i = 0; i < aIds.length; i++) {
				int nId = aIds[i];
				String cUri;
				cUri = getProp("uri_" + nId);
				int nSuccess = kaPlugin.getSuccessValues(nId)[0];
				int nFailure = kaPlugin.getSuccessValues(nId)[1];
				int nPersistence = 0;
				if (nSuccess > 0) {
					nPersistence = (int) ((double) nSuccess / (nSuccess + nFailure) * 100);
				}
				int nAvailableSegments = kaPlugin.getSuccessValues(nId)[2];
				int nSegmentsAvailability = 0;
				int nFinishedSegmentsCount = getIntProp("segment_" + nId) + 1;
				if (nFinishedSegmentsCount > 0) {
					nSegmentsAvailability = (int) ((double) nAvailableSegments / nFinishedSegmentsCount * 100);
				}
				cHtml += "<tr>"
						+ "<td><a href='/" + cUri + "'>" + getShortUri(nId) + "</a></td>"
						+ "<td align=\"center\">" + getProp("blocks_" + nId) + "</td>"
						+ "<td align=\"center\">" + nSuccess + "</td>"
						+ "<td align=\"center\">" + nFailure + "</td>"
						+ "<td align=\"center\">" + nPersistence + " %</td>"
						+ "<td align=\"center\">" + nSegmentsAvailability + " %</td>"
						+ "<td><a href='?remove=" + nId + "'>remove</a></td>"
						+ "<td><a href='?log=" + nId + "'>log</a></td>";
				if (nId == getIntProp("active")) {
					cHtml += "<td><a href='?stop=" + nId + "'>stop</a></td>";
					cHtml += "<td><b>active</b></td>";
				} else {
					cHtml += "<td><a href='?start=" + nId + "'>start</a></td>";
					cHtml += "<td></td>";
				}
				cHtml += "</tr>";
			}
			cHtml += "</table>";
			addBox("Add or remove a key", cHtml);

			// log box
			if (getParam("master_log") != null || getParam("log") != null) {
				String cLog;
				if (getParam("master_log") != null) {
					cLog = kaPlugin.getLog();
				} else {
					cLog = kaPlugin.getLog(kaPlugin.getLogFilename(getIntParam("log")));
				}
				if (cLog == null) {
					cLog = "";
				}
				cHtml = ("<small>" + cLog + "</small>").replaceAll("\n", "<br>").replaceAll("  ", "&nbsp; &nbsp; ");
				if (getParam("master_log") != null) {
					addBox("Master log", cHtml);
				} else {
					addBox("Log for " + getShortUri(getIntParam("log")), cHtml);
				}
			}

			// configuration box
			cHtml = html("properties");
			cHtml = cHtml.replaceAll("#1", getProp("power"));
			cHtml = cHtml.replaceAll("#2", getProp("loglevel"));
			cHtml = cHtml.replaceAll("#3", getProp("splitfile_tolerance"));
			cHtml = cHtml.replaceAll("#4", getProp("splitfile_test_size"));
			addBox("Configuration", cHtml);

			// history box
			cHtml = "<table>";
			for (int i = 0; i < aIds.length; i++) {
				cHtml += "<tr><td>" + getShortUri(aIds[i]) + "</td><td>";
				if (getProp("history_" + aIds[i]) != null) {
					cHtml += getProp("history_" + aIds[i]).replaceAll("-", "=").replaceAll(",", "%, ") + "%";
				}
				cHtml += "</td><td><a href=\"?clear_history=" + aIds[i] + "\">clear</a></td></tr>";
			}
			cHtml += "</table>";
			addBox("Lowest rate of blocks availability (monthly)", cHtml);

			// info box
			cHtml = html("info");
			cHtml = cHtml.replaceAll("#1", kaPlugin.getVersion());
			addBox("Information", cHtml);

		} catch (Exception e) {
			log("AdminPage.handleRequest(): " + e.getMessage(), 0);
		}
	}

	protected synchronized void updateUskEdition(int nSiteId) {
		try {

			String cSiteUri = kaPlugin.getProp("uri_" + nSiteId);
			String cId = "updateUskEdition" + System.currentTimeMillis();
			fcp.sendClientGet(cId, cSiteUri);
			int nSecs = 0;
			while (nSecs < 300 && getMessage(cId, "AllData") == null) {
				wait(1000);
				nSecs++;
			}
			if (getRedirectURI() != null) {
				kaPlugin.setProp("uri_" + nSiteId, getRedirectURI());
				log("RedirectURI: " + getRedirectURI(), 1);
			}

		} catch (Exception e) {
			log("AdminPage.updateUskEdition(): " + e.getMessage(), 0);
		}
	}

	private String getShortUri(int nSiteId) {
		try {

			String cUri = getProp("uri_" + nSiteId);
			if (cUri.length() > 80) {
				return cUri.substring(0, 20) + "...." + cUri.substring(cUri.length() - 50);
			} else {
				return cUri;
			}

		} catch (Exception e) {
			log("AdminPage.getShortUri(): " + e.getMessage(), 0);
			return null;
		}
	}

	private void setIntPropByParam(String cPropName, int nMinValue) {
		try {

			int nValue = nMinValue;
			try {
				nValue = getIntParam(cPropName);
			} catch (Exception e) {
			}
			if (nValue != -1 && nValue < nMinValue) {
				nValue = nMinValue;
			}
			setIntProp(cPropName, nValue);
			saveProp();

		} catch (Exception e) {
			log("AdminPage.setPropByParam(): " + e.getMessage(), 0);
		}
	}

	public synchronized boolean isDuplicate(String cUri) {

		try {
			String iId;
			for (int i : kaPlugin.getIds()) {
				iId = getProp("uri_" + i);
				if (iId.equals(cUri)) {
					addBox("Duplicate URI", "We are already keeping this key alive:<br><br>" + cUri);
					return true;
				}
			}
		} catch (Exception ex) {
			log("AdminPage.isDuplicate(): " + ex.getMessage(), 0);
		}
		return false;
	}
}
