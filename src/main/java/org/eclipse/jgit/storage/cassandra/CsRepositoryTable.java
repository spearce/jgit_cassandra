/*
 * Copyright (C) 2011, Google Inc.
 * and other copyright owners as documented in the project's IP log.
 *
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which
 * accompanies this distribution, is reproduced below, and is
 * available at http://www.eclipse.org/org/documents/edl-v10.php
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Eclipse Foundation, Inc. nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.eclipse.jgit.storage.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.eclipse.jgit.storage.dht.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;

class CsRepositoryTable implements RepositoryTable {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private static final String CF = "Repository";

	private final CassandraDatabase db;

	private final ColumnMatcher colChunkInfo;

	private final ColumnMatcher colCachedPack;

	CsRepositoryTable(CassandraDatabase db) {
		this.db = db;
		this.colChunkInfo = new ColumnMatcher("chunkInfo:");
		this.colCachedPack = new ColumnMatcher("cachedPack:");
	}

	public RepositoryKey nextKey() throws DhtException {
		// TODO(spearce) Use a unique assignment.
		long now = System.currentTimeMillis() / 1000L;
		now -= 1297547467L; // Feb 12 2011
		return RepositoryKey.create((int) now);
	}

	public Collection<CachedPackInfo> getCachedPacks(RepositoryKey repo)
			throws DhtException, TimeoutException {
		ColumnSlice<byte[], byte[]> slice = HFactory
				.createSliceQuery(db.getKeyspace(Context.LOCAL), S, S, S)
				.setColumnFamily(CF)
				.setKey(repo.asBytes())
				.setRange(colCachedPack.append(new byte[] {}),
						colCachedPack.append(new byte[] { '~' }), false,
						Integer.MAX_VALUE).execute().get();
		if (slice == null || slice.getColumns().isEmpty())
			return Collections.emptyList();

		int estCnt = slice.getColumns().size();
		List<CachedPackInfo> info = new ArrayList<CachedPackInfo>(estCnt);
		for (HColumn<byte[], byte[]> col : slice.getColumns()) {
			byte[] name = col.getName();
			if (colCachedPack.sameFamily(name))
				info.add(CachedPackInfo.fromBytes(col.getValue()));
		}
		return info;
	}

	public void put(RepositoryKey repo, CachedPackInfo info, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		buf.put(CF, //
				repo.asBytes(), //
				colCachedPack.append(info.getRowKey()), //
				info.asBytes());
	}

	public void remove(RepositoryKey repo, CachedPackKey key, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		buf.delete(CF, repo.asBytes(), colCachedPack.append(key.asBytes()));
	}

	public void put(RepositoryKey repo, ChunkInfo info, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		ChunkKey key = info.getChunkKey();
		buf.put(CF, //
				repo.asBytes(), //
				colChunkInfo.append(key.asBytes()), //
				info.asBytes());
	}

	public void remove(RepositoryKey repo, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		buf.delete(CF, repo.asBytes(), colChunkInfo.append(chunk.asBytes()));
	}
}
