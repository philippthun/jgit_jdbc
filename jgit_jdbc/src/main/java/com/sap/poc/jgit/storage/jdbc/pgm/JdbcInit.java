/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.kohsuke.args4j.Argument;

import com.sap.poc.jgit.storage.jdbc.JdbcRepositoryBuilder;

@Command(name = "jdbc-init")
class JdbcInit extends TextBuiltin {
	@Argument(index = 0, required = true, metaVar = "git+jdbc+")
	String uri;

	@Override
	protected boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		final int now = (int) (System.currentTimeMillis() / 1000L);
		final RepositoryKey key = RepositoryKey.create(now);
		final JdbcRepositoryBuilder builder = new JdbcRepositoryBuilder()
				.setURI(uri).setRepositoryKey(key);
		builder.build().create(true);
		System.out.println("Created " + key + ":");
		System.out.println("  database:   " + builder.getVendor());
		System.out.println("  vendor:     " + builder.getHost());
		System.out.println("  host:       " + builder.getDatabaseName());
		System.out.println("  repository: " + builder.getRepositoryName());
	}
}
