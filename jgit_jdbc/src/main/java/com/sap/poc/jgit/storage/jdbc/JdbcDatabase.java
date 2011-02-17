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
	private final JdbcRepositoryIndexTable repositoryIndex;
	private final JdbcRepositoryTable repository;
	private final JdbcRefTable ref;
	private final JdbcChunkTable chunk;
	private final JdbcObjectIndexTable objectIndex;

	JdbcDatabase(JdbcDatabaseBuilder builder) {
		executor = builder.getExecutorService();
		pool = new ConnectionPool("jdbc:" + builder.getVendor() + "://"
				+ builder.getHost() + "/" + builder.getDatabaseName());
		repositoryIndex = new JdbcRepositoryIndexTable(this);
		repository = new JdbcRepositoryTable(this);
		ref = new JdbcRefTable(this);
		chunk = new JdbcChunkTable(this);
		objectIndex = new JdbcObjectIndexTable(this);
	}

	ExecutorService getExecutorService() {
		return executor;
	}

	synchronized Connection getConnection() throws SQLException {
		return pool.getConnection();
	}

	static void closeConnection(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			// Ignore
		}
	}

	public void shutdown() {
		pool.shutdown();
	}

	@Override
	public RepositoryIndexTable repositoryIndex() {
		return repositoryIndex;
	}

	@Override
	public RepositoryTable repository() {
		return repository;
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
		return objectIndex;
	}

	@Override
	public WriteBuffer newWriteBuffer() {
		return new JdbcWriteBuffer(executor, 1);
	}
}

class ConnectionPool {
	private final String url;
	private final Collection<JdbcConnection> connList;

	ConnectionPool(String url) {
		this.url = url;
		connList = new ArrayList<JdbcConnection>();
	}

	Connection getConnection() throws SQLException {
		JdbcConnection conn;

		for (JdbcConnection c : connList) {
			conn = c.reuse();
			if (conn != null)
				return conn;
		}

		conn = new JdbcConnection(DriverManager.getConnection(url));
		connList.add(conn);

		return conn;
	}

	void shutdown() {
		for (JdbcConnection conn : connList)
			try {
				conn.doClose();
			} catch (SQLException e) {
				// Ignore
			}
	}
}
