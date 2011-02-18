/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeRefData;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeRefKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRefData;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRefKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRepoKey;

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

public class JdbcRefTable extends JdbcSqlHelper implements RefTable {
	private JdbcDatabase db;

	public JdbcRefTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public Map<RefKey, RefData> getAll(Context options,
			final RepositoryKey repository) throws DhtException,
			TimeoutException {
		// TODO use options
		final Map<RefKey, RefData> refMap = new HashMap<RefKey, RefData>();
		Connection conn = null;

		try {
			if (repository != null) {
				conn = db.getConnection();
				final Statement stmt = conn.createStatement();
				stmt.execute(SELECT_M_FROM_REF(encodeRepoKey(repository)));
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null)
					while (resSet.next()) {
						final String sRefKey = resSet.getString(1);
						final String sRefData = resSet.getString(2);
						if (sRefKey != null && sRefKey.length() > 0
								&& sRefData != null && sRefData.length() > 0)
							refMap.put(RefKey.fromBytes(decodeRefKey(sRefKey)),
									RefData.fromBytes(decodeRefData(sRefData)));
					}
				return refMap;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public RefTransaction newTransaction(final RefKey refKey)
			throws DhtException, TimeoutException {
		if (refKey == null)
			throw new DhtException("Invalid parameter"); // TODO externalize

		final String sRefKey = encodeRefKey(refKey);
		final String sRepoKey = encodeRepoKey(refKey.getRepositoryKey());
		Connection conn = null;
		RefData refData = null;

		try {
			conn = db.getConnection();
			final Statement stmt = conn.createStatement();
			stmt.execute(SELECT_FROM_REF(sRepoKey, sRefKey));
			final ResultSet resSet = stmt.getResultSet();
			if (resSet != null && resSet.next()) {
				// Exists
				final String sRefData = resSet.getString(1);
				if (sRefData != null && sRefData.length() > 0)
					refData = RefData.fromBytes(decodeRefData(sRefData));
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
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
						final String sRefData = encodeRefData(newData);
						conn = db.getConnection();
						final Statement stmt = conn.createStatement();
						stmt.execute(SELECT_EXISTS_FROM_REF(sRepoKey, sRefKey));
						final ResultSet resSet = stmt.getResultSet();
						if (resSet != null && resSet.next())
							// Exists -> update
							conn.createStatement().executeUpdate(
									UPDATE_IN_REF(sRepoKey, sRefKey, sRefData));
						else
							// Not exists -> insert
							conn.createStatement()
									.executeUpdate(
											INSERT_INTO_REF(sRepoKey, sRefKey,
													sRefData));
						// TODO check result
						return true;
					} else {
						throw new DhtException("Invalid parameter"); // TODO
																		// externalize
					}
				} catch (SQLException e) {
					throw new DhtException(e);
				} finally {
					closeConnection(conn);
				}
			}

			@Override
			public boolean compareAndRemove() throws DhtException,
					TimeoutException {
				Connection conn = null;

				try {
					// TODO compare

					conn = db.getConnection();
					conn.createStatement().executeUpdate(
							DELETE_FROM_REF(sRepoKey, sRefKey));
					// TODO check result
					return true;
				} catch (SQLException e) {
					throw new DhtException(e);
				} finally {
					closeConnection(conn);
				}
			}

			@Override
			public void abort() {
			}
		};
	}
}
