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
import freenet.keys.FreenetURI;
import freenet.support.compress.Compressor;

public class SingleFetch extends SingleJob {

	boolean bPersistenceCheck;

	public SingleFetch(Reinserter reinserter, Block block, boolean bPersistenceCheck) {
		super(reinserter, "fetch", block);

		this.bPersistenceCheck = bPersistenceCheck;
	}

	@Override
	public String toString() {
		return "KeepAlive - SingleFetch";
	}

	public void run() {
		super.run();
		try {

			// init
			//HighLevelSimpleClientImpl hlsc = (HighLevelSimpleClientImpl) plugin.pluginContext.node.clientCore.makeClient((short) 3, false, false);
			HLSCignoreStore hlscIgnoreStore = new HLSCignoreStore(plugin.hlsc);

			FreenetURI fetchUri = block.uri.clone();
			block.bFetchDone = false;
			block.bFetchSuccessfull = false;

			// modify the control flag of the URI to get always the raw data
			byte[] aExtra = fetchUri.getExtra();
			aExtra[2] = 0;

			// get the compression algorithm of the block
			String cCompressor = null;
			if (aExtra[4] >= 0) {
				cCompressor = Compressor.COMPRESSOR_TYPE.getCompressorByMetadataID((short) aExtra[4]).name;
			} else {
				cCompressor = "none";
			}

			// request
			FetchResult fetchResult = null;
			try {

				log("request: " + block.uri.toString() + " (crypt=" + aExtra[1] + ",control=" + block.uri.getExtra()[2] + ",compress=" + aExtra[4] + "=" + cCompressor + ")", 2);
				if (!bPersistenceCheck) {
					fetchResult = plugin.hlsc.fetch(fetchUri);
				} else {
					fetchResult = hlscIgnoreStore.fetch(fetchUri);
				}

			} catch (FetchException e) {
				block.setResultLog("-> fetch error: " + e.getMessage());
			}

			// log / success flag
			if (block.cResultLog == null) {
				if (fetchResult == null) {
					block.setResultLog("-> fetch failed");
				} else {
					block.bucket = fetchResult.asBucket();
					block.bFetchSuccessfull = true;
					block.setResultLog("-> fetch successful");
				}
			}

			//finish
			reinserter.registerBlockFetchSuccess(block);
			block.bFetchDone = true;
			finish();

		} catch (Exception e) {
			plugin.log("SingleFetch.run(): " + e.getMessage(), 0);
		}
	}

	private class HLSCignoreStore extends HighLevelSimpleClientImpl {

		public HLSCignoreStore(HighLevelSimpleClientImpl hlsc) {
			super(hlsc);
		}

		public FetchContext getFetchContext() {
			FetchContext fc = super.getFetchContext();
			fc.ignoreStore = true;
			return fc;
		}
	}
}
