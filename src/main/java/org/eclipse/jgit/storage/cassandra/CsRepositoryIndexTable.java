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

import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;

import java.util.concurrent.TimeoutException;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;

class CsRepositoryIndexTable implements RepositoryIndexTable {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private static final String CF_REPOSITORY_INDEX = "RepositoryIndex";

	private static final String CF_REPOSITORY = "Repository";

	private static final byte[] TRUE = { '1' };

	private final CassandraDatabase db;

	private final ColumnMatcher colName;

	private final ColumnMatcher colId;

	CsRepositoryIndexTable(CassandraDatabase db) {
		this.db = db;
		this.colName = new ColumnMatcher("name:");
		this.colId = new ColumnMatcher("id");
	}

	public RepositoryKey get(RepositoryName name) throws DhtException,
			TimeoutException {
		try {
			HColumn<byte[], byte[]> r = createColumnQuery(
					db.getKeyspace(Context.LOCAL), S, S, S)
					.setColumnFamily(CF_REPOSITORY_INDEX) //
					.setKey(name.asBytes()) //
					.setName(colId.name()) //
					.execute().get();
			if (r != null)
				return RepositoryKey.fromBytes(r.getValue());

			r = createColumnQuery(db.getKeyspace(Context.READ_REPAIR), S, S, S)
					.setColumnFamily(CF_REPOSITORY_INDEX) //
					.setKey(name.asBytes()) //
					.setName(colId.name()) //
					.execute().get();
			if (r != null)
				return RepositoryKey.fromBytes(r.getValue());
			return null;
		} catch (HectorException err) {
			throw new DhtException(err);
		}
	}

	public void putUnique(RepositoryName name, RepositoryKey key)
			throws DhtException, TimeoutException {
		// TODO Add proper row locking using ZooKeeper.
		db.put(CF_REPOSITORY_INDEX, name.asBytes(), colId.name(), key.asBytes());

		db.put(CF_REPOSITORY, key.asBytes(), colName.append(name.asBytes()), TRUE);
	}
}
