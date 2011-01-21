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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.util.AbstractWriteBuffer;

/** Buffers write operations to Cassandra to create larger batches. */
final class CsBuffer extends AbstractWriteBuffer {
	private static final BytesArraySerializer S = CassandraDatabase.S;

	private final CassandraDatabase db;

	private Mutator<byte[]> buf;

	CsBuffer(CassandraDatabase db, int bufferSize) {
		super(db.getExecutorService(), bufferSize);
		this.db = db;
	}

	void put(String colFam, byte[] key, byte[] col, byte[] val)
			throws DhtException {
		HColumn<byte[], byte[]> c = createColumn(col, val, S, S);
		put(colFam, key, Collections.singletonList(c));
	}

	void put(String colFam, byte[] key, List<HColumn<byte[], byte[]>> cols)
			throws DhtException {
		int sz = colFam.length() + key.length;
		for (HColumn<byte[], byte[]> col : cols)
			sz += col.getName().length + col.getValue().length;

		if (add(sz)) {
			init();
			for (HColumn<byte[], byte[]> col : cols)
				buf.addInsertion(key, colFam, col);
			queued(sz);
		} else {
			Mutator<byte[]> op = db.createMutator();
			for (HColumn<byte[], byte[]> col : cols)
				op.addInsertion(key, colFam, col);
			start(op, sz);
		}
	}

	void delete(String colFam, byte[] key, byte[] col) throws DhtException {
		int sz = colFam.length() + key.length + col.length;
		add(sz);
		init();
		buf.addDeletion(key, colFam, col, CassandraDatabase.S);
		queued(sz);
	}

	void deleteRow(String colFam, byte[] key) throws DhtException {
		int sz = colFam.length() + key.length;
		add(sz);
		init();
		buf.addDeletion(key, colFam, null, CassandraDatabase.S);
		queued(sz);
	}

	private void init() {
		if (buf == null)
			buf = db.createMutator();
	}

	@Override
	protected void startQueuedOperations(int bufferedByteCount)
			throws DhtException {
		Mutator<byte[]> op = buf;
		buf = null;
		start(op, bufferedByteCount);
	}

	@Override
	public void abort() throws DhtException {
		buf = null;
		super.abort();
	}

	private void start(final Mutator<byte[]> op, int size) throws DhtException {
		start(new Callable<MutationResult>() {
			public MutationResult call() throws Exception {
				try {
					return op.execute();
				} catch (HectorException err) {
					throw new DhtException(err);
				}
			}
		}, size);
	}
}
