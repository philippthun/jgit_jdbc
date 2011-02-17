/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RefTransaction;
import org.eclipse.jgit.util.Base64;

public class JdbcRefTable implements RefTable {
	private JdbcDatabase database;

	public JdbcRefTable(JdbcDatabase jdbcDatabase) {
		database = jdbcDatabase;
	}

	@Override
	public Map<RefKey, RefData> getAll(Context options,
			final RepositoryKey repository) throws DhtException,
			TimeoutException {
		final Map<RefKey, RefData> refMap = new HashMap<RefKey, RefData>();
		Connection conn = null;
		try {
			if (repository != null) {
				final String dbRepositoryKey = Base64.encodeBytes(repository
						.toBytes());

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "SELECT r_key, r_data FROM ref WHERE r_repository_key = '"
						+ dbRepositoryKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null)
					while (resultSet.next()) {
						final String key = resultSet.getString(1);
						final String data = resultSet.getString(2);
						if (key != null && key.length() > 0 && data != null
								&& data.length() > 0)
							refMap.put(RefKey.fromBytes(Base64.decode(key)),
									RefData.fromBytes(Base64.decode(data)));
					}
			}
			return refMap;
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public RefTransaction newTransaction(final RefKey refKey)
			throws DhtException, TimeoutException {
		if (refKey == null)
			return null;

		final String dbKey = Base64.encodeBytes(refKey.toBytes());
		final String dbRepositoryKey = Base64.encodeBytes(refKey
				.getRepositoryKey().toBytes());

		Connection conn = null;
		RefData refData = null;

		try {
			conn = database.getConnection();
			final Statement statement = conn.createStatement();
			final String sql = "SELECT r_data FROM ref WHERE r_key = '" + dbKey
					+ "' AND r_repository_key = '" + dbRepositoryKey + "'";
			System.out.println("-----");
			System.out.println(sql);
			statement.execute(sql);
			final ResultSet resultSet = statement.getResultSet();
			if (resultSet != null && resultSet.next()) {
				// Exists
				final String data = resultSet.getString(1);
				refData = RefData.fromBytes(Base64.decode(data));
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}

		final RefData oldData;
		if (refData != null)
			oldData = refData;
		else
			oldData = RefData.NONE;

		return new RefTransaction() {
			@Override
			public RefData getOldData() {
				return oldData;
			}

			@Override
			public boolean compareAndPut(final RefData newData)
					throws DhtException {
				Connection conn = null;
				try {
					// TODO compare
					if (newData != null) {
						final String dbData = Base64.encodeBytes(newData
								.toBytes());

						conn = database.getConnection();
						Statement statement = conn.createStatement();
						String sql = "SELECT r_key FROM ref WHERE r_key = '"
								+ dbKey + "' AND r_repository_key = '"
								+ dbRepositoryKey + "'";
						System.out.println("-----");
						System.out.println(sql);
						statement.execute(sql);
						final ResultSet resultSet = statement.getResultSet();
						if (resultSet != null && resultSet.next()) {
							// Exists -> update
							statement = conn.createStatement();
							sql = "UPDATE ref SET r_data = '" + dbData
									+ "' WHERE r_key = '" + dbKey
									+ "' AND r_repository_key = '"
									+ dbRepositoryKey + "'";
							System.out.println("-----");
							System.out.println(sql);
							statement.executeUpdate(sql);
						} else {
							// Not exists -> insert
							statement = conn.createStatement();
							sql = "INSERT INTO ref (r_key, r_data, r_repository_key) VALUES "
									+ "('"
									+ dbKey
									+ "', '"
									+ dbData
									+ "', '"
									+ dbRepositoryKey + "')";
							System.out.println("-----");
							System.out.println(sql);
							statement.executeUpdate(sql);
						}
						return true;
					} else {
						return false;
					}
				} catch (SQLException e) {
					throw new DhtException(e);
				} finally {
					JdbcDatabase.closeConnection(conn);
				}
			}

			@Override
			public boolean compareAndRemove() throws DhtException,
					TimeoutException {
				Connection conn = null;
				try {
					// TODO compare
					conn = database.getConnection();
					final Statement statement = conn.createStatement();
					final String sql = "DELETE FROM ref WHERE r_key = '"
							+ dbKey + "' AND r_repository_key = '"
							+ dbRepositoryKey + "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
					return true;
				} catch (SQLException e) {
					throw new DhtException(e);
				} finally {
					JdbcDatabase.closeConnection(conn);
				}
			}

			@Override
			public void abort() {
			}
		};
	}
}
