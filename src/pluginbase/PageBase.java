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

import pluginbase.de.todesbaum.util.freenet.fcp2.Message;
import freenet.clients.http.InfoboxNode;
import freenet.clients.http.PageNode;
import freenet.clients.http.Toadlet;
import freenet.clients.http.ToadletContext;
import freenet.clients.http.ToadletContextClosedException;
import freenet.l10n.BaseL10n.LANGUAGE;
import freenet.pluginmanager.*;
import freenet.support.api.HTTPRequest;
import freenet.support.HTMLNode;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;
import java.util.ArrayList;

abstract public class PageBase extends Toadlet implements FredPluginL10n {

	protected FcpCommands fcp;

	public PluginBase plugin;
	PageNode page;
	ArrayList<HTMLNode> vBoxes = new ArrayList();
	TreeMap<String, Message> mMessages = new TreeMap<>();
	String cPageName;
	String cPageTitle;
	String cMenuTitle = null;
	String cMenuTooltip = null;
	String cRefreshTarget;
	int nRefreshPeriod = -1;
	URI uri;
	HTTPRequest httpRequest;
	TreeMap<String, String> mRedirectURIs = new TreeMap<>();
	private String cRedirectURI;
	private boolean bFullAccessHostsOnly;

	public PageBase(String cPageName, String cPageTitle, PluginBase plugin, boolean bFullAccessHostsOnly) {
		super(plugin.pluginContext.node.clientCore.makeClient((short) 3, false, false));

		try {
			this.cPageName = cPageName;
			this.cPageTitle = cPageTitle;
			this.plugin = plugin;
			this.bFullAccessHostsOnly = bFullAccessHostsOnly;

			// register this page and add to menu
			plugin.webInterface.registerInvisible(this);
			plugin.log("page '" + cPageName + "' registered");

			// fcp request object
			fcp = new FcpCommands(plugin.fcpConnection, this);

		} catch (Exception e) {
			plugin.log("PageBase(): " + e.getMessage(), 1);
		}
	}

	public String getName() {
		return cPageName;
	}

	@Override
	public String path() {
		return plugin.getPath() + "/" + cPageName;
	}

	/**
	 *
	 * @param cKey
	 * @return
	 */
	@Override
	public String getString(String cKey) {       // FredPluginL10n
		return plugin.getString(cKey);
	}

	/**
	 *
	 * @param newLanguage
	 */
	@Override
	public void setLanguage(LANGUAGE newLanguage) {      // FredPluginL10n
		plugin.setLanguage(newLanguage);
	}

	@Override
	public void handleMethodGET(URI uri, HTTPRequest request, ToadletContext ctx) throws ToadletContextClosedException, IOException {
		try {

			vBoxes.clear();
			if (!bFullAccessHostsOnly || ctx.isAllowedFullAccess()) {
				this.uri = uri;
				this.httpRequest = request;
				handleRequest();
			} else {
				addBox("Access denied!", "Access to this page for hosts with full access rights only.");
			}

			makePage(uri, ctx);

		} catch (Exception e) {
			log("PageBase.handleMethodGET(): " + e.getMessage(), 1);
		}
	}

	public void handleMethodPOST(URI uri, HTTPRequest request, ToadletContext ctx) throws ToadletContextClosedException, IOException {
		handleMethodGET(uri, request, ctx);
	}

	@Override
	public boolean allowPOSTWithoutPassword() {
		return true;
	}

	private String getIdentifier(Message message) throws Exception {
		try {

			String[] aIdentifier = message.getIdentifier().split("_");
			String cIdentifier = aIdentifier[1];
			for (int i = 2; i < aIdentifier.length - 1; i++) {
				cIdentifier += "_" + aIdentifier[i];
			}
			return cIdentifier;

		} catch (Exception e) {
			throw new Exception("PageBase.getIdentifier(): " + e.getMessage());
		}
	}

	void addMessage(Message message) throws Exception {
		try {

			// existing message with same id becomes replaced (e.g. AllData replaces DataFound)
			mMessages.put(getIdentifier(message), message);

		} catch (Exception e) {
			throw new Exception("PageBase.addMessage(): " + e.getMessage());
		}
	}

	void updateRedirectUri(Message message) throws Exception {
		try {

			// existing RedirectUri with same id becomes replaced
			mRedirectURIs.put(getIdentifier(message), message.get("RedirectUri"));

		} catch (Exception e) {
			throw new Exception("PageBase.updateRedirectUri(): " + e.getMessage());
		}
	}

	private void makePage(URI uri, ToadletContext ctx) throws Exception {
		try {

			page = plugin.pagemaker.getPageNode(cPageTitle, ctx);

			// refresh page
			if (nRefreshPeriod != -1) {
				if (cRefreshTarget == null) {
					cRefreshTarget = uri.getPath();
					if (uri.getQuery() != null) {
						cRefreshTarget += "?" + uri.getQuery();
					}
				}
				page.headNode.addChild("meta", new String[]{"http-equiv", "content"}, new String[]{"refresh", nRefreshPeriod + ";URL=" + cRefreshTarget});
			}

			// boxes
			for (HTMLNode box : vBoxes) {
				page.content.addChild(box);
			}

			// write
			writeHTMLReply(ctx, 200, "OK", page.outer.generate());

		} catch (ToadletContextClosedException | IOException e) {
			throw new Exception("PageBase.html(): " + e.getMessage());
		}
	}

	// ********************************************
	// methods to use in the derived plugin class:
	// ********************************************
	// file log
	public void log(String cText, int nLogLevel) {
		plugin.log(cText, nLogLevel);
	}

	public void log(String cText) {
		plugin.log(cText);
	}

	// methods to add this page to the plugins' menu (fproxy)
	protected void addPageToMenu(String cMenuTitle, String cMenuTooltip) {
		try {

			plugin.pluginContext.pluginRespirator.getToadletContainer().unregister(this);
			plugin.pluginContext.pluginRespirator.getToadletContainer().register(this, plugin.getCategory(), this.path(), true, cMenuTitle, cMenuTooltip, bFullAccessHostsOnly, null);
			log("page '" + cPageName + "' added to menu");

		} catch (Exception e) {
			log("PageBase.addPageToMenu(): " + e.getMessage(), 1);
		}
	}

	// methods to build the page
	protected void addBox(String cTitle, String cHtmlBody) {
		try {

			InfoboxNode box = plugin.pagemaker.getInfobox(cTitle);
			cHtmlBody = cHtmlBody.replaceAll("'", "\"");
			box.content.addChild("%", cHtmlBody);
			vBoxes.add(box.outer);

		} catch (Exception e) {
			log("PluginBase.addBox(): " + e.getMessage(), 1);
		}
	}

	protected String html(String cName) throws Exception {
		try {

			byte[] aContent = new byte[1024];
			String cContent;
			try (InputStream stream = getClass().getResourceAsStream("/" + getClass().getPackage().getName().replace('.', '/') + "/html/" + cName)) {
				int nLen;
				cContent = "";
				while ((nLen = stream.read(aContent)) != -1) {
					cContent += new String(aContent, 0, nLen, "UTF-8");
				}
			}
			return cContent;

		} catch (IOException e) {
			throw new Exception("PageBase.html(): " + e.getMessage());
		}
	}

	// methods to make the page refresh
	protected void setRefresh(int nPeriod, String cTarget) {
		this.nRefreshPeriod = nPeriod;
		this.cRefreshTarget = cTarget;
	}

	protected void setRefresh(int nPeriod) {
		setRefresh(nPeriod, null);
	}

	// methods to handle http requests (both get and post)
	abstract protected void handleRequest();

	protected String getQuery() throws Exception {
		try {

			return uri.getQuery();

		} catch (Exception e) {
			throw new Exception("PageBase.getQuery(): " + e.getMessage());
		}
	}

	protected HTTPRequest getRequest() {
		return httpRequest;
	}

	protected String getParam(String cKey) throws Exception {
		try {

			if (httpRequest.getMethod().toUpperCase().equals("GET") && !httpRequest.getParam(cKey).equals("")) {
				return httpRequest.getParam(cKey);
			} else if (httpRequest.getMethod().toUpperCase().equals("POST") && httpRequest.getPart(cKey) != null) {
				byte[] aContent = new byte[(int) httpRequest.getPart(cKey).size()];
				httpRequest.getPart(cKey).getInputStream().read(aContent);
				return new String(aContent, "UTF-8");
			} else {
				return null;
			}

		} catch (IOException e) {
			throw new Exception("PageBase.getParam(): " + e.getMessage());
		}
	}

	protected int getIntParam(String cKey) throws Exception {
		try {

			return Integer.valueOf(getParam(cKey));

		} catch (Exception e) {
			throw new Exception("PageBase.getIntParam(): " + e.getMessage());
		}
	}

	// methods to handle fcp messages
	protected Message getMessage(String cId, String cMessageType) throws Exception {
		try {

			Message message = mMessages.get(cId);
			if (message != null && !message.getName().equals(cMessageType)) {
				message = null;
			}
			if (message != null) {
				mMessages.remove(cId);
				cRedirectURI = mRedirectURIs.get(cId);
				mRedirectURIs.remove(cId);
			}
			return message;                 // returns null if no message

		} catch (Exception e) {
			throw new Exception("PageBase.getMessage(): " + e.getMessage());
		}
	}

	protected String getRedirectURI() {
		return cRedirectURI;
	}

	protected String[] getSSKKeypair(String cId) {
		try {

			Message message = getMessage(cId, "SSKKeypair");
			if (message != null) {
				return new String[]{message.get("InsertURI"), message.get("RequestURI")};
			} else {
				return null;
			}

		} catch (Exception e) {
			log("PageBase.getSSKKeypair(): " + e.getMessage(), 1);
			return null;
		}
	}

	// methods to set and get persistent properties
	public void saveProp() {
		plugin.saveProp();
	}

	public void setProp(String cKey, String cValue) throws Exception {
		plugin.setProp(cKey, cValue);
	}

	public String getProp(String cKey) throws Exception {
		return plugin.getProp(cKey);
	}

	public void setIntProp(String cKey, int nValue) throws Exception {
		plugin.setIntProp(cKey, nValue);
	}

	public int getIntProp(String cKey) throws Exception {
		return plugin.getIntProp(cKey);
	}

	public void removeProp(String cKey) {
		plugin.removeProp(cKey);
	}

	// method to allow access to this page for full-access-hosts only
	public void restrictToFullAccessHosts(boolean bRestrict) {
		bFullAccessHostsOnly = bRestrict;
	}

}
