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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

/**
 * Stores Git repositories in an Apache Cassandra database.
 * <p>
 * To construct a database connection to the Cassandra cluster, use
 * {@link CassandraDatabaseBuilder}.
 */
public class CassandraDatabase implements Database {
	static final BytesArraySerializer S = BytesArraySerializer.get();

	private final Cluster cluster;

	private final ExecutorService executors;

	private final Keyspace keyspaceFastMissingOk;

	private final Keyspace keyspaceLocal;

	private final Keyspace keyspaceReadRepair;

	private final CsRepositoryIndexTable repositoryIndex;

	private final CsRepositoryTable repository;

	private final CsRefTable ref;

	private final CsChunkTable chunk;

	private final CsObjectIndexTable objectIndex;

	CassandraDatabase(CassandraDatabaseBuilder builder) {
		this.cluster = builder.getCluster();
		this.executors = builder.getExecutorService();

		String keyspaceName = builder.getKeyspaceName();

		keyspaceFastMissingOk = HFactory.createKeyspace(keyspaceName, cluster,
				new FastMissingOk(), FailoverPolicy.FAIL_FAST);

		keyspaceReadRepair = HFactory.createKeyspace(keyspaceName, cluster,
				new ReadRepair(), FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);

		if (builder.isClusterDataCenterAware())
			keyspaceLocal = HFactory.createKeyspace(keyspaceName, cluster,
					new Local(), FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);
		else
			keyspaceLocal = keyspaceReadRepair;

		repositoryIndex = new CsRepositoryIndexTable(this);
		repository = new CsRepositoryTable(this);
		ref = new CsRefTable(this);
		chunk = new CsChunkTable(this);
		objectIndex = new CsObjectIndexTable(this);
	}

	/** Shutdown the connection(s) to the cluster. */
	public void shutdown() {
		cluster.getConnectionManager().shutdown();
	}

	public RepositoryIndexTable repositoryIndex() {
		return repositoryIndex;
	}

	public RepositoryTable repository() {
		return repository;
	}

	public RefTable ref() {
		return ref;
	}

	public ChunkTable chunk() {
		return chunk;
	}

	public ObjectIndexTable objectIndex() {
		return objectIndex;
	}

	public WriteBuffer newWriteBuffer() {
		return new CsBuffer(this, 10 * 1024 * 1024);
	}

	Keyspace getKeyspace(Context context) {
		switch (context) {
		case FAST_MISSING_OK:
			return keyspaceFastMissingOk;
		case LOCAL:
			return keyspaceLocal;
		case READ_REPAIR:
			return keyspaceReadRepair;
		default:
			throw new IllegalArgumentException();
		}
	}

	ExecutorService getExecutorService() {
		return executors;
	}

	Future<?> submit(Runnable task) {
		return getExecutorService().submit(task);
	}

	void put(String columnFamily, byte[] key, byte[] col, byte[] val)
			throws DhtException {
		try {
			Mutator<byte[]> op = createMutator();
			addInsertion(op, columnFamily, key, col, val);
			op.execute();
		} catch (HectorException err) {
			throw new DhtException(err);
		}
	}

	Mutator<byte[]> createMutator() {
		return HFactory.createMutator(getKeyspace(Context.READ_REPAIR), S);
	}

	static Mutator<byte[]> addInsertion(Mutator<byte[]> mutation,
			String columnFamily, byte[] key, byte[] col, byte[] val) {
		HColumn<byte[], byte[]> c = HFactory.createColumn(col, val, S, S);
		return mutation.addInsertion(key, columnFamily, c);
	}

	private static class FastMissingOk implements ConsistencyLevelPolicy {
		public HConsistencyLevel get(OperationType op) {
			return HConsistencyLevel.ONE;
		}

		public HConsistencyLevel get(OperationType op, String cfName) {
			return HConsistencyLevel.ONE;
		}
	}

	private static class Local implements ConsistencyLevelPolicy {
		public HConsistencyLevel get(OperationType op) {
			return HConsistencyLevel.LOCAL_QUORUM;
		}

		public HConsistencyLevel get(OperationType op, String cfName) {
			return HConsistencyLevel.LOCAL_QUORUM;
		}
	}

	private static class ReadRepair implements ConsistencyLevelPolicy {
		public HConsistencyLevel get(OperationType op) {
			return HConsistencyLevel.QUORUM;
		}

		public HConsistencyLevel get(OperationType op, String cfName) {
			return HConsistencyLevel.QUORUM;
		}
	}
}
