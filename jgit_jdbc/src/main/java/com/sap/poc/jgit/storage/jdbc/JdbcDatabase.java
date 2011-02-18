/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
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
