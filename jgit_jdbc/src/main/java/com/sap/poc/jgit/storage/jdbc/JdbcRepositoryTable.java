/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

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
import org.eclipse.jgit.util.Base64;

public class JdbcRepositoryTable implements RepositoryTable {
	private JdbcDatabase database;

	public JdbcRepositoryTable(JdbcDatabase jdbcDatabase) {
		database = jdbcDatabase;
	}

	@Override
	public void put(final RepositoryKey repo, final ChunkInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;
		try {
			if (repo != null && info != null) {
				final String dbKey = Base64.encodeBytes(repo.toBytes());
				final String dbChunkKey = Base64.encodeBytes(info.getChunkKey()
						.toBytes());
				final String dbChunkInfo = Base64.encodeBytes(info.toBytes());

				conn = database.getConnection();
				Statement statement = conn.createStatement();
				String sql = "SELECT r_chunk_key FROM repository WHERE r_key = '"
						+ dbKey + "' AND r_chunk_key = '" + dbChunkKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null && resultSet.next()) {
					// Exists -> update
					statement = conn.createStatement();
					sql = "UPDATE repository SET r_chunk_info = '"
							+ dbChunkInfo + "' WHERE r_key = '" + dbKey
							+ "' AND r_chunk_key = '" + dbChunkKey + "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				} else {
					// Not exists -> insert
					statement = conn.createStatement();
					sql = "INSERT INTO repository (r_key, r_chunk_key, r_chunk_info) VALUES "
							+ "('"
							+ dbKey
							+ "', '"
							+ dbChunkKey
							+ "', '"
							+ dbChunkInfo + "')";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				}
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;
		try {
			if (repo != null && chunk != null) {
				final String dbKey = Base64.encodeBytes(repo.toBytes());
				final String dbChunkKey = Base64.encodeBytes(chunk.toBytes());

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "DELETE FROM repository WHERE r_key = '"
						+ dbKey + "' AND r_chunk_key = '" + dbChunkKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public Collection<CachedPackInfo> getCachedPacks(final RepositoryKey repo)
			throws DhtException, TimeoutException {
		final Collection<CachedPackInfo> cachedPackInfoList = new ArrayList<CachedPackInfo>();
		Connection conn = null;
		try {
			if (repo != null) {
				final String dbKey = Base64.encodeBytes(repo.toBytes());

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "SELECT r_cached_pack_info FROM repository WHERE r_key = '"
						+ dbKey + "' AND NOT r_cached_pack_key = ''";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null)
					while (resultSet.next()) {
						final String cachedPackInfo = resultSet.getString(1);
						if (cachedPackInfo != null
								&& cachedPackInfo.length() > 0)
							cachedPackInfoList.add(CachedPackInfo.fromBytes(
									repo, Base64.decode(cachedPackInfo)));
					}
			}
			return cachedPackInfoList;
		} catch (final SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public void put(final RepositoryKey repo, final CachedPackInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;
		try {
			if (repo != null && info != null) {
				final String dbKey = Base64.encodeBytes(repo.toBytes());
				final String dbCachedPackKey = Base64.encodeBytes(info
						.getRowKey().toBytes());
				final String dbCachedPackInfo = Base64.encodeBytes(info
						.toBytes());

				conn = database.getConnection();
				Statement statement = conn.createStatement();
				String sql = "SELECT r_cached_pack_key FROM repository WHERE r_key = '"
						+ dbKey
						+ "' AND r_cached_pack_key = '"
						+ dbCachedPackKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null && resultSet.next()) {
					// Exists -> update
					statement = conn.createStatement();
					sql = "UPDATE repository SET r_cached_pack_info = '"
							+ dbCachedPackInfo + "' WHERE r_key = '" + dbKey
							+ "' AND r_cached_pack_key = '" + dbCachedPackKey
							+ "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				} else {
					// Not exists -> insert
					statement = conn.createStatement();
					sql = "INSERT INTO repository (r_key, r_cached_pack_key, r_cached_pack_info) VALUES "
							+ "('"
							+ dbKey
							+ "', '"
							+ dbCachedPackKey
							+ "', '"
							+ dbCachedPackInfo + "')";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				}
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final CachedPackKey key,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;
		try {
			if (repo != null && key != null) {
				final String dbKey = Base64.encodeBytes(repo.toBytes());
				final String dbCachedPackKey = Base64
						.encodeBytes(key.toBytes());

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "DELETE FROM repository WHERE r_key = '"
						+ dbKey + "' AND r_cached_pack_key = '"
						+ dbCachedPackKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}
}
