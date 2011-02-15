/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.util.ExecutorTools;
import org.eclipse.jgit.transport.URIish;

public class JdbcDatabaseBuilder {
	private String vendor;
	private String host;
	private String databaseName;
	private ExecutorService executorService;

	public JdbcDatabaseBuilder setURI(final String uri)
			throws URISyntaxException {
		final URIish u = new URIish(uri);

		final String scheme = u.getScheme();
		if (!scheme.startsWith("git+jdbc+"))
			throw new IllegalArgumentException();
		final int beginVendor = scheme.lastIndexOf("+") + 1;
		final String vendor = scheme.substring(beginVendor);
		if (vendor.length() < 1)
			throw new IllegalArgumentException();
		setVendor(vendor);

		final String host = u.getHost();
		if (host == null || host.length() < 1)
			throw new IllegalArgumentException();
		final int port = u.getPort();
		if (port == -1)
			throw new IllegalArgumentException();
		setHost(host + ":" + port);

		String path = u.getPath();
		if (path.startsWith("/"))
			path = path.substring(1);

		int endDatabaseName = path.indexOf("/");
		if (endDatabaseName < 0)
			endDatabaseName = path.length();
		setDatabaseName(path.substring(0, endDatabaseName));
		return this;
	}

	public String getVendor() {
		return vendor;
	}

	public JdbcDatabaseBuilder setVendor(String vendor) {
		this.vendor = vendor;
		return this;
	}

	public String getHost() {
		return host;
	}

	public JdbcDatabaseBuilder setHost(String host) {
		this.host = host;
		return this;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public JdbcDatabaseBuilder setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
		return this;
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

	public JdbcDatabaseBuilder setExecutorService(
			ExecutorService executorService) {
		this.executorService = executorService;
		return this;
	}

	public JdbcDatabase build() throws IllegalArgumentException, DhtException {
		if (getVendor() == null)
			throw new IllegalArgumentException("No vendor set");

		if (getHost() == null)
			throw new IllegalArgumentException("No host set");

		if (getDatabaseName() == null)
			throw new IllegalArgumentException("No database name set");

		if (getExecutorService() == null)
			executorService = ExecutorTools.getDefaultExecutorService();

		return new JdbcDatabase(this);
	}
}
