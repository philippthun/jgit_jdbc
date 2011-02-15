/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
	private final Connection conn;
	private final JdbcRepositoryIndexTable repositoryIndex;
	private final JdbcRepositoryTable repository;
	private final JdbcRefTable ref;
	private final JdbcChunkTable chunk;
	private final JdbcObjectIndexTable objectIndex;

	JdbcDatabase(JdbcDatabaseBuilder builder) throws DhtException {
		try {
			executor = builder.getExecutorService();
			final String url = "jdbc:" + builder.getVendor() + "://"
					+ builder.getHost() + "/" + builder.getDatabaseName();
			conn = DriverManager.getConnection(url);
			repositoryIndex = new JdbcRepositoryIndexTable(this);
			repository = new JdbcRepositoryTable(this);
			ref = new JdbcRefTable(this);
			chunk = new JdbcChunkTable(this);
			objectIndex = new JdbcObjectIndexTable(this);
		} catch (SQLException e) {
			shutdown();
			throw new DhtException(e);
		}
	}

	ExecutorService getExecutorService() {
		return executor;
	}

	Connection getConnection() {
		return conn;
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

	public void shutdown() {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				// Ignore
			}
	}
}
