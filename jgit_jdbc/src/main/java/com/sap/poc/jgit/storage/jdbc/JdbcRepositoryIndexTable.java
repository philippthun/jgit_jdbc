/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeRepoKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRepoKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRepoName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;

public class JdbcRepositoryIndexTable extends JdbcSqlHelper implements
		RepositoryIndexTable {
	private JdbcDatabase db;

	public JdbcRepositoryIndexTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public RepositoryKey get(final RepositoryName name) throws DhtException,
			TimeoutException {
		Connection conn = null;

		try {
			if (name != null) {
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_REPO_KEY_FROM_REPO_IDX(encodeRepoName(name)));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next()) {
					// Exists
					final String sRepoKey = resSet.getString(1);
					if (sRepoKey != null && sRepoKey.length() > 0)
						return RepositoryKey.fromBytes(decodeRepoKey(sRepoKey));
				}
				throw new DhtException("Repository not exists"); // TODO
																	// externalize
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void putUnique(final RepositoryName name, final RepositoryKey key)
			throws DhtException, TimeoutException {
		Connection conn = null;

		try {
			if (name != null && key != null) {
				final String sRepoName = encodeRepoName(name);
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_EXISTS_FROM_REPO_IDX(sRepoName));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next())
					// Exists
					throw new DhtException("Repository name already exists"); // TODO
																				// externalize
				conn.createStatement().executeUpdate(
						INSERT_INTO_REPO_IDX(encodeRepoKey(key), sRepoName));
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
