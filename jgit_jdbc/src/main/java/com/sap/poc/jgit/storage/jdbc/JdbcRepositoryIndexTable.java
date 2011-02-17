/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.util.Base64;

public class JdbcRepositoryIndexTable implements RepositoryIndexTable {
	private JdbcDatabase database;

	public JdbcRepositoryIndexTable(JdbcDatabase jdbcDatabase) {
		database = jdbcDatabase;
	}

	@Override
	public RepositoryKey get(final RepositoryName name) throws DhtException,
			TimeoutException {
		Connection conn = null;
		try {
			if (name != null) {
				final String dbName = name.asString();

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "SELECT ri_key FROM repository_index WHERE ri_name = '"
						+ dbName + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null && resultSet.next()) {
					// Exists
					final String key = resultSet.getString(1);
					return RepositoryKey.fromBytes(Base64.decode(key));
				}
			}
			return null;
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			JdbcDatabase.closeConnection(conn);
		}
	}

	@Override
	public void putUnique(final RepositoryName name, final RepositoryKey key)
			throws DhtException, TimeoutException {
		Connection conn = null;
		try {
			if (name != null && key != null) {
				final String dbKey = Base64.encodeBytes(key.toBytes());
				final String dbName = name.asString();

				conn = database.getConnection();
				final Statement statement = conn.createStatement();
				final String sql = "INSERT INTO repository_index (ri_key, ri_name) VALUES ('"
						+ dbKey + "', '" + dbName + "')";
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
