/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
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
package com.sap.poc.jgit.storage.jdbc;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.util.ExecutorTools;
import org.eclipse.jgit.transport.URIish;

import com.sap.poc.jgit.storage.jdbc.pgm.Main;

public class JdbcDatabaseBuilder {
	private String vendor;
	private String host;
	private String dbName;
	private ExecutorService executor;

	public JdbcDatabaseBuilder setURI(final String uri)
			throws URISyntaxException {
		final URIish u = new URIish(uri);

		final String scheme = u.getScheme();
		if (!scheme.startsWith(Main.GIT_JDBC_PREFIX))
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

		int endDbName = path.indexOf("/");
		if (endDbName < 0)
			endDbName = path.length();
		setDatabaseName(path.substring(0, endDbName));
		return this;
	}

	public String getVendor() {
		return vendor;
	}

	public JdbcDatabaseBuilder setVendor(final String vendor) {
		this.vendor = vendor;
		return this;
	}

	public String getHost() {
		return host;
	}

	public JdbcDatabaseBuilder setHost(final String host) {
		this.host = host;
		return this;
	}

	public String getDatabaseName() {
		return dbName;
	}

	public JdbcDatabaseBuilder setDatabaseName(final String dbName) {
		this.dbName = dbName;
		return this;
	}

	public ExecutorService getExecutorService() {
		return executor;
	}

	public JdbcDatabaseBuilder setExecutorService(final ExecutorService executor) {
		this.executor = executor;
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
			executor = ExecutorTools.getDefaultExecutorService();

		return new JdbcDatabase(this);
	}
}
