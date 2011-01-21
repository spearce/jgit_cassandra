/*
 * Copyright (C) 2010, Google Inc.
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

package org.eclipse.jgit.storage.cassandra.pgm;

import java.io.File;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.pgm.CLIText;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.cassandra.CassandraDatabase;
import org.eclipse.jgit.storage.cassandra.CassandraDatabaseBuilder;
import org.eclipse.jgit.storage.cassandra.CassandraRepositoryBuilder;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.storage.pack.PackConfig;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.DaemonService;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.util.FS;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

class CassandraDaemon extends TextBuiltin {
	@Option(name = "--config-file", metaVar = "metaVar_configFile", usage = "usage_configFile")
	File configFile;

	@Option(name = "--port", metaVar = "metaVar_port", usage = "usage_portNumberToListenOn")
	int port = org.eclipse.jgit.transport.Daemon.DEFAULT_PORT;

	@Option(name = "--listen", metaVar = "metaVar_hostName", usage = "usage_hostnameOrIpToListenOn")
	String host;

	@Option(name = "--timeout", metaVar = "metaVar_seconds", usage = "usage_abortConnectionIfNoActivity")
	int timeout = -1;

	@Option(name = "--enable", metaVar = "metaVar_service", usage = "usage_enableTheServiceInAllRepositories", multiValued = true)
	final List<String> enable = new ArrayList<String>();

	@Option(name = "--disable", metaVar = "metaVar_service", usage = "usage_disableTheServiceInAllRepositories", multiValued = true)
	final List<String> disable = new ArrayList<String>();

	@Option(name = "--allow-override", metaVar = "metaVar_service", usage = "usage_configureTheServiceInDaemonServicename", multiValued = true)
	final List<String> canOverride = new ArrayList<String>();

	@Option(name = "--forbid-override", metaVar = "metaVar_service", usage = "usage_configureTheServiceInDaemonServicename", multiValued = true)
	final List<String> forbidOverride = new ArrayList<String>();

	@Argument(index = 0, required = true, metaVar = "git+cassandra://")
	String uri;

	protected boolean requiresRepository() {
		return false;
	}

	protected void run() throws Exception {
		PackConfig packConfig = new PackConfig();

		if (configFile != null) {
			if (!configFile.exists()) {
				throw die(MessageFormat.format(
						CLIText.get().configFileNotFound, //
						configFile.getAbsolutePath()));
			}

			FileBasedConfig cfg = new FileBasedConfig(configFile, FS.DETECTED);
			cfg.load();
			packConfig.fromConfig(cfg);
		}

		int threads = packConfig.getThreads();
		if (threads <= 0)
			threads = Runtime.getRuntime().availableProcessors();
		if (1 < threads)
			packConfig.setExecutor(Executors.newFixedThreadPool(threads));

		final org.eclipse.jgit.transport.Daemon d;

		d = new org.eclipse.jgit.transport.Daemon(
				host != null ? new InetSocketAddress(host, port)
						: new InetSocketAddress(port));

		final CassandraDatabase db = new CassandraDatabaseBuilder() //
				.setURI(uri) //
				.build();

		RepositoryResolver<DaemonClient> resolver = new RepositoryResolver<DaemonClient>() {
			public Repository open(DaemonClient req, String name)
					throws RepositoryNotFoundException {
				try {
					return new CassandraRepositoryBuilder()
					  .setDatabase(db)
					  .setRepositoryName(name)
					  .setMustExist(true)
					  .build();
				} catch (DhtException e) {
					throw new RepositoryNotFoundException(name, e);
				}
			}
		};

		d.setPackConfig(packConfig);
		d.setRepositoryResolver(resolver);
		if (0 <= timeout)
			d.setTimeout(timeout);

		for (final String n : enable)
			service(d, n).setEnabled(true);
		for (final String n : disable)
			service(d, n).setEnabled(false);

		for (final String n : canOverride)
			service(d, n).setOverridable(true);
		for (final String n : forbidOverride)
			service(d, n).setOverridable(false);

		d.start();
		out.println(MessageFormat.format(CLIText.get().listeningOn,
				d.getAddress()));
	}

	private DaemonService service(final org.eclipse.jgit.transport.Daemon d,
			final String n) {
		final DaemonService svc = d.getService(n);
		if (svc == null)
			throw die(MessageFormat
					.format(CLIText.get().serviceNotSupported, n));
		return svc;
	}
}