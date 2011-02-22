/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import java.io.File;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.pgm.CLIText;
import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.storage.pack.PackConfig;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.DaemonService;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.util.FS;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.sap.poc.jgit.storage.jdbc.JdbcDatabase;
import com.sap.poc.jgit.storage.jdbc.JdbcDatabaseBuilder;
import com.sap.poc.jgit.storage.jdbc.JdbcRepositoryBuilder;

@Command(name = "jdbc-daemon")
class JdbcDaemon extends TextBuiltin {
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

	@Argument(index = 0, required = true, metaVar = Main.GIT_JDBC_PREFIX)
	String uri;

	@Override
	protected boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		final PackConfig packConfig = new PackConfig();

		if (configFile != null) {
			if (!configFile.exists())
				throw die(MessageFormat.format(
						CLIText.get().configFileNotFound,
						configFile.getAbsolutePath()));

			final FileBasedConfig cfg = new FileBasedConfig(configFile,
					FS.DETECTED);
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

		final JdbcDatabase db = new JdbcDatabaseBuilder().setURI(uri).build();

		final RepositoryResolver<DaemonClient> resolver = new RepositoryResolver<DaemonClient>() {
			@Override
			public Repository open(DaemonClient req, String name)
					throws RepositoryNotFoundException {
				try {
					return new JdbcRepositoryBuilder().setDatabase(db)
							.setRepositoryName(name).build();
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
