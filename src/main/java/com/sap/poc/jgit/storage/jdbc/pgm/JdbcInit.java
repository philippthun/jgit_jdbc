/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.dht.DhtRepository;
import org.kohsuke.args4j.Argument;

import com.sap.poc.jgit.storage.jdbc.JdbcRepositoryBuilder;

@Command(name = "jdbc-init")
class JdbcInit extends TextBuiltin {
	@Argument(index = 0, required = true, metaVar = Main.GIT_JDBC_PREFIX)
	String uri;

	@Override
	protected boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		final JdbcRepositoryBuilder builder = new JdbcRepositoryBuilder()
				.setURI(uri).setMustExist(false);
		final DhtRepository repository = builder.build();
		repository.create(true);
		System.out.println("Created "
				+ repository.getRepositoryKey().asString() + ":");
		System.out.println("  database:   " + builder.getVendor());
		System.out.println("  vendor:     " + builder.getHost());
		System.out.println("  host:       " + builder.getDatabaseName());
		System.out.println("  repository: " + builder.getRepositoryName());
	}
}
