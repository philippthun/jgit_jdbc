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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class JdbcDatabase implements Database {
	private final ExecutorService executor;
	private final ConnectionPool pool;
	private final JdbcRepositoryIndexTable repoIdx;
	private final JdbcRepositoryTable repo;
	private final JdbcRefTable ref;
	private final JdbcChunkTable chunk;
	private final JdbcObjectIndexTable objIdx;

	JdbcDatabase(final JdbcDatabaseBuilder builder) {
		executor = builder.getExecutorService();
		pool = new ConnectionPool("jdbc:" + builder.getVendor() + "://"
				+ builder.getHost() + "/" + builder.getDatabaseName());
		repoIdx = new JdbcRepositoryIndexTable(this);
		repo = new JdbcRepositoryTable(this);
		ref = new JdbcRefTable(this);
		chunk = new JdbcChunkTable(this);
		objIdx = new JdbcObjectIndexTable(this);
	}

	ExecutorService getExecutorService() {
		return executor;
	}

	synchronized Connection getConnection() throws SQLException {
		return pool.getConnection();
	}

	public void shutdown() {
		pool.shutdown();
	}

	@Override
	public RepositoryIndexTable repositoryIndex() {
		return repoIdx;
	}

	@Override
	public RepositoryTable repository() {
		return repo;
	}

	@Override
	public RefTable ref() {
		return ref;
	}

	@Override
	public ChunkTable chunk() {
		return chunk;
	}

	@Override
	public ObjectIndexTable objectIndex() {
		return objIdx;
	}

	@Override
	public WriteBuffer newWriteBuffer() {
		return new WriteBuffer() {
			@Override
			public void flush() throws DhtException {
			}

			@Override
			public void abort() throws DhtException {
			}
		};
	}
}

class ConnectionPool {
	private final String url;
	private final Collection<JdbcConnection> connList;

	ConnectionPool(final String url) {
		this.url = url;
		connList = new ArrayList<JdbcConnection>();
	}

	Connection getConnection() throws SQLException {
		JdbcConnection conn;

		for (final JdbcConnection c : connList) {
			conn = c.reuse();
			if (conn != null)
				return conn;
		}

		conn = new JdbcConnection(DriverManager.getConnection(url));
		connList.add(conn);

		return conn;
	}

	void shutdown() {
		for (final JdbcConnection conn : connList)
			try {
				conn.doClose();
			} catch (SQLException e) {
				// Ignore
			}
	}
}
