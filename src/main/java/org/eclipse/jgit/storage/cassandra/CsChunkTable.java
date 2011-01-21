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

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.MultigetSliceQuery;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.ChunkMeta;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;

final class CsChunkTable implements ChunkTable {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private static final String CF = "Chunk";

	private final CassandraDatabase db;

	private final ColumnMatcher colChunk;

	private final ColumnMatcher colIndex;

	private final ColumnMatcher colMeta;

	private final byte[][] getCols;

	CsChunkTable(CassandraDatabase db) {
		this.db = db;
		this.colChunk = new ColumnMatcher("chunk");
		this.colIndex = new ColumnMatcher("index");
		this.colMeta = new ColumnMatcher("meta");
		this.getCols = new byte[][] { colChunk.name(),
				colIndex.name(),
				colMeta.name(),
		};
	}

	public void get(Context options, Set<ChunkKey> keys,
			final AsyncCallback<Collection<PackChunk.Members>> callback) {
		final MultigetSliceQuery<byte[], byte[], byte[]> q;

		q = createMultigetSliceQuery(db.getKeyspace(options), S, S, S);
		q.setColumnFamily(CF);
		q.setKeys(CsUtil.asByteArrays(keys));
		q.setColumnNames(getCols);

		db.submit(new Runnable() {
			public void run() {
				try {
					callback.onSuccess(parseChunks(q.execute().get()));
				} catch (Throwable err) {
					callback.onFailure(new DhtException(err));
				}
			}
		});
	}

	private Collection<PackChunk.Members> parseChunks(
			Rows<byte[], byte[], byte[]> rows) throws DhtException {
		Collection<PackChunk.Members> chunkList;
		chunkList = new ArrayList<PackChunk.Members>(rows.getCount());

		for (Row<byte[], byte[], byte[]> r : rows) {
			PackChunk.Members m = new PackChunk.Members();
			ChunkKey key = ChunkKey.fromBytes(r.getKey());
			m.setChunkKey(key);

			List<HColumn<byte[], byte[]>> t = r.getColumnSlice().getColumns();
			for (HColumn<byte[], byte[]> cell : t) {
				byte[] col = cell.getName();

				if (colChunk.sameName(col))
					m.setChunkData(cell.getValue());

				else if (colIndex.sameName(col))
					m.setChunkIndex(cell.getValue());

				else if (colMeta.sameName(col))
					m.setMeta(ChunkMeta.fromBytes(key, cell.getValue()));
			}
			chunkList.add(m);
		}
		return chunkList;
	}

	public void put(PackChunk.Members chunk, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		List<HColumn<byte[], byte[]>> cols = new ArrayList<HColumn<byte[], byte[]>>(4);

		if (chunk.getChunkData() != null)
			cols.add(createColumn(colChunk.name(), chunk.getChunkData(), S, S));

		if (chunk.getChunkIndex() != null)
			cols.add(createColumn(colIndex.name(), chunk.getChunkIndex(), S, S));

		if (chunk.getMeta() != null)
			cols.add(createColumn(colMeta.name(), chunk.getMeta().asBytes(), S, S));

		buf.put(CF, chunk.getChunkKey().asBytes(), cols);
	}

	public void remove(ChunkKey key, WriteBuffer buffer) throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		buf.deleteRow(CF, key.asBytes());
	}
}
