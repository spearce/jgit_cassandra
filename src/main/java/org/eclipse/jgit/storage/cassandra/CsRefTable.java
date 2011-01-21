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

import static org.eclipse.jgit.storage.cassandra.CassandraDatabase.addInsertion;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.util.RawParseUtils;

final class CsRefTable implements RefTable {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private static final String CF = "Ref";

	private final CassandraDatabase db;

	CsRefTable(CassandraDatabase db) {
		this.db = db;
	}

	public Map<RefKey, RefData> getAll(Context options, RepositoryKey repository)
			throws DhtException {
		try {
			SliceQuery<byte[], byte[], byte[]> q;

			q = HFactory.createSliceQuery(db.getKeyspace(options), S, S, S);
			q.setColumnFamily(CF);
			q.setKey(repository.asBytes());
			q.setRange(new byte[] { 0 }, new byte[] {}, false,
					Integer.MAX_VALUE);

			Map<RefKey, RefData> r = new HashMap<RefKey, RefData>();
			for (HColumn<byte[], byte[]> c : q.execute().get().getColumns()) {
				r.put(
					RefKey.create(repository, RawParseUtils.decode(c.getName())),
					RefData.fromBytes(c.getValue()));
			}
			return r;

		} catch (HectorException err) {
			throw new DhtException(err);
		}
	}

	public boolean compareAndPut(RefKey refKey, RefData oldData, RefData newData)
			throws DhtException, TimeoutException {
		// TODO Use ZooKeeper for a proper transaction on refKey;
		try {
			addInsertion(db.createMutator(), CF, //
					refKey.getRepositoryKey().asBytes(), //
					Constants.encode(refKey.getName()), //
					newData.asBytes()).execute();
			return true;
		} catch (HectorException err) {
			throw new DhtException(err);
		}
	}

	public boolean compareAndRemove(RefKey refKey, RefData oldData)
			throws DhtException, TimeoutException {
		// TODO Use ZooKeeper for a proper transaction on refKey;
		db.createMutator().addDeletion( //
				refKey.getRepositoryKey().asBytes(), //
				CF, //
				Constants.encode(refKey.getName()), //
				S).execute();
		return true;
	}
}
