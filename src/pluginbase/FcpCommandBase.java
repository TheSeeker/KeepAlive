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

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Vector;
import pluginbase.de.todesbaum.util.freenet.fcp2.Command;
import pluginbase.de.todesbaum.util.freenet.fcp2.Connection;
import freenet.support.SimpleFieldSet;

abstract public class FcpCommandBase extends Command {

	protected PageBase page;
	private Connection fcpConnection;
	private String cCommandName;
	private Vector<String> vFields = new Vector();
	private InputStream dataStream;
	private int nDataLength;

	public FcpCommandBase(Connection fcpConnection, PageBase page) {
		super(null, null);
		this.fcpConnection = fcpConnection;
		this.page = page;
	}

	public FcpCommandBase(Connection fcpConnection) {
		super(null, null);
		this.fcpConnection = fcpConnection;
	}

	public String getCommandName() {
		return cCommandName;
	}

	public void setCommandName(String cCommandName) {
		this.cCommandName = cCommandName;
	}

	public String getIdentifier(String cIdentifier) throws Exception {
		try {

			return page.getName() + "_" + cIdentifier + "_" + System.currentTimeMillis();

		} catch (Exception e) {
			throw new Exception("FcpCommandBase.getIdentifier(): " + e.getMessage());
		}
	}

	protected boolean hasPayload() {
		return (dataStream != null);
	}

	protected InputStream getPayload() {
		return dataStream;
	}

	protected long getPayloadLength() {
		return nDataLength;
	}

	protected void write(Writer writer) throws IOException {
		try {

			for (String cField : vFields) {
				writer.write(cField + "\n");
			}

		} catch (IOException e) {
			throw new IOException("FcpCommandBase.write(): " + e.getMessage());
		} catch (Exception e) {
			page.log("FcpCommandBase.write(): " + e.getMessage(), 1);
		}
	}

	protected void init(String cCommand, String cIdentifierSuffix) throws Exception {
		try {

			if (cIdentifierSuffix == null) {
				cIdentifierSuffix = "";
			}
			vFields.clear();
			setCommandName(cCommand);
			field("Identifier", getIdentifier(cIdentifierSuffix));

		} catch (Exception e) {
			throw new Exception("FcpCommandBase.init(): " + e.getMessage());
		}
	}

	protected void field(String cKey, String cValue) throws Exception {
		try {

			vFields.add(cKey + "=" + cValue);

		} catch (Exception e) {
			throw new Exception("FcpCommandBase.field(): " + e.getMessage());
		}
	}

	protected void field(String cKey, int nValue) throws Exception {
		try {

			vFields.add(cKey + "=" + nValue);

		} catch (Exception e) {
			throw new Exception("FcpCommandBase.field(): " + e.getMessage());
		}
	}

	protected void send(InputStream dataStream, int nDataLength) throws Exception {
		try {

			this.dataStream = dataStream;
			this.nDataLength = nDataLength;
			fcpConnection.execute(this);

		} catch (Exception e) {
			throw new Exception("FcpCommandBase.send(): " + e.getMessage());
		}
	}

	protected void send() throws Exception {
		send(null, 0);
	}

}
