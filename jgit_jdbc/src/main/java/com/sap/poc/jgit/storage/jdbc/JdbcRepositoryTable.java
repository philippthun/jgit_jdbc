/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeCachPackInfo;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeCachPackInfo;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeCachPackKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkInfo;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRepoKey;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class JdbcRepositoryTable extends JdbcSqlHelper implements
		RepositoryTable {
	private JdbcDatabase db;

	public JdbcRepositoryTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public void put(final RepositoryKey repo, final ChunkInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && info != null) {
				final String sRepoKey = encodeRepoKey(repo);
				final String sChunkKey = encodeChunkKey(info.getChunkKey());
				final String sChunkInfo = encodeChunkInfo(info);
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_EXISTS_FROM_CHUNK_INFO(sRepoKey, sChunkKey));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next())
					// Exists -> update
					conn.createStatement().executeUpdate(
							UPDATE_IN_CHUNK_INFO(sRepoKey, sChunkKey,
									sChunkInfo));
				else
					// Not exists -> insert
					conn.createStatement().executeUpdate(
							INSERT_INTO_CHUNK_INFO(sRepoKey, sChunkKey,
									sChunkInfo));
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && chunk != null) {
				conn = db.getConnection();
				conn.createStatement().executeUpdate(
						DELETE_FROM_CHUNK_INFO(encodeRepoKey(repo),
								encodeChunkKey(chunk)));
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public Collection<CachedPackInfo> getCachedPacks(final RepositoryKey repo)
			throws DhtException, TimeoutException {
		final Collection<CachedPackInfo> cachedPackInfoList = new ArrayList<CachedPackInfo>();
		Connection conn = null;

		try {
			if (repo != null) {
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_M_FROM_CACH_PACK(encodeRepoKey(repo)));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null)
					while (resSet.next()) {
						final String sCachedPackInfo = resSet.getString(1);
						if (sCachedPackInfo != null
								&& sCachedPackInfo.length() > 0)
							cachedPackInfoList.add(CachedPackInfo.fromBytes(
									repo, decodeCachPackInfo(sCachedPackInfo)));
					}
				return cachedPackInfoList;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (final SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void put(final RepositoryKey repo, final CachedPackInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && info != null) {
				final String sRepoKey = encodeRepoKey(repo);
				final String sCachedPackKey = encodeCachPackKey(info
						.getRowKey());
				final String sCachedPackInfo = encodeCachPackInfo(info);
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_EXISTS_FROM_CACH_PACK(sRepoKey,
						sCachedPackKey));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next())
					// Exists -> update
					conn.createStatement().executeUpdate(
							UPDATE_IN_CACH_PACK(sRepoKey, sCachedPackKey,
									sCachedPackInfo));
				else
					// Not exists -> insert
					conn.createStatement().executeUpdate(
							INSERT_INTO_CACH_PACK(sRepoKey, sCachedPackKey,
									sCachedPackInfo));
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final CachedPackKey key,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && key != null) {
				conn = db.getConnection();
				conn.createStatement().executeUpdate(
						DELETE_FROM_CACH_PACK(encodeRepoKey(repo),
								encodeCachPackKey(key)));
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}
}
