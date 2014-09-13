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

import java.util.Vector;

public class Segment {

	private Reinserter reinserter;
	protected int nId;
	private int nSize;
	private int nDataBlocksCount;
	private Block[] aBlocks;
	private int nSuccess = 0;
	private int nFailed = 0;
	private boolean bPersistenceCheckOk = false;
	protected boolean bHealingNotPossible = false;

	public Segment(Reinserter reinserter, int nId, int nSize) {
		this.reinserter = reinserter;
		this.nId = nId;
		this.nSize = nSize;
		aBlocks = new Block[nSize];
	}

	public void addBlock(Block block) {
		aBlocks[block.nId] = block;
		if (block.bIsDataBlock) {
			nDataBlocksCount++;
		}
	}

	public Block getBlock(int nId) {
		return aBlocks[nId];
	}

	public Block getDataBlock(int nId) {
		return aBlocks[nId];
	}

	public Block getCheckBlock(int nId) {
		return aBlocks[dataSize() + nId];
	}

	public int size() {
		return nSize;  // aBlocks.length can produce null-exception (see isFinished())
	}

	public int dataSize() {
		return nDataBlocksCount;
	}

	public int checkSize() {
		return nSize - nDataBlocksCount;
	}

	public void initInsert() {
		nSuccess = 0;
		nFailed = 0;
	}

	public void regFetchSuccess(double nPersistenceRate) {
		bPersistenceCheckOk = true;
		nSuccess = (int) Math.round(nPersistenceRate * nSize);
		nFailed = nSize - nSuccess;
		reinserter.updateBlockStatistic(nId, nSuccess, nFailed);
	}

	public void regFetchSuccess(boolean bSuccess) {
		if (bSuccess) {
			nSuccess++;
		} else {
			nFailed++;
		}
		reinserter.updateBlockStatistic(nId, nSuccess, nFailed);
	}

	public boolean isFinished() {

		boolean bFinished = true;
		if (!bPersistenceCheckOk && !bHealingNotPossible) {
			if (nSize == 1) {
				bFinished = getBlock(0).bInsertDone;
			} else {
				for (int i = 0; i < nSize; i++) {
					if (!getBlock(i).bFetchSuccessfull && !getBlock(i).bInsertDone) {
						bFinished = false;
						break;
					}
				}
			}
		}

		// free blocks (especially buckets)
		if (bFinished) {
			aBlocks = null;
		}

		return bFinished;
	}
}
