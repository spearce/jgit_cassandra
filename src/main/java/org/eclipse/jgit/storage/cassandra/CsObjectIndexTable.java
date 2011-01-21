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

import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.MultigetSliceQuery;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;

final class CsObjectIndexTable implements ObjectIndexTable {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private static final String CF = "ObjectIndex";

	private final CassandraDatabase db;

	private final ColumnMatcher colInfo;

	CsObjectIndexTable(CassandraDatabase db) {
		this.db = db;
		this.colInfo = new ColumnMatcher("info:");
	}

	public void get(
			Context options,
			Set<ObjectIndexKey> objects,
			final AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
		final MultigetSliceQuery<byte[], byte[], byte[]> q;

		q = createMultigetSliceQuery(db.getKeyspace(options), S, S, S);
		q.setColumnFamily(CF);
		q.setKeys(CsUtil.asByteArraysArray(objects));
		q.setRange( //
				colInfo.name(), //
				colInfo.append(new byte[] { 'Z' }), //
				false, Integer.MAX_VALUE);

		db.submit(new Runnable() {
			public void run() {
				try {
					callback.onSuccess(findChunks(q.execute().get()));
				} catch (Throwable err) {
					callback.onFailure(new DhtException(err));
				}
			}
		});
	}

	private Map<ObjectIndexKey, Collection<ObjectInfo>> findChunks(
			Rows<byte[], byte[], byte[]> rows) {
		Map<ObjectIndexKey, Collection<ObjectInfo>> map;

		map = new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
		for (Row<byte[], byte[], byte[]> r : rows) {
			List<HColumn<byte[], byte[]>> cols = r.getColumnSlice().getColumns();
			if (cols.isEmpty())
				continue;

			ObjectIndexKey key = ObjectIndexKey.fromBytes(r.getKey());
			Collection<ObjectInfo> list = map.get(key);
			if (list == null) {
				list = new ArrayList<ObjectInfo>(cols.size());
				map.put(key, list);
			}

			for (HColumn<byte[], byte[]> cell : cols) {
				byte[] name = cell.getName();
				if (!colInfo.sameFamily(name))
					continue;
				ChunkKey k = ChunkKey.fromBytes(colInfo.suffix(name));
				long time = cell.getClock();
				list.add(ObjectInfo.fromBytes(k, cell.getValue(), time));
			}
		}
		return map;
	}

	public void add(ObjectIndexKey objId, ObjectInfo link, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		ChunkKey key = link.getChunkKey();
		buf.put(CF, //
				objId.asBytes(), //
				colInfo.append(key.asBytes()), //
				link.asBytes());
	}

	public void remove(ObjectIndexKey objId, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		CsBuffer buf = (CsBuffer) buffer;
		buf.delete(CF, objId.asBytes(), colInfo.append(chunk.asBytes()));
	}
}
