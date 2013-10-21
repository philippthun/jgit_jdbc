/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeRowKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRowKey;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;

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
				final PreparedStatement stmt = conn
						.prepareStatement(SELECT_M_FROM_REF);
				stmt.setString(1, encodeRowKey(repository));
				stmt.execute();
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null)
					while (resSet.next()) {
						final String sRefKey = resSet.getString(1);
						final byte[] refData = resSet.getBytes(2);
						if (sRefKey != null && sRefKey.length() > 0
								&& refData != null && refData.length > 0)
							refMap.put(RefKey.fromBytes(decodeRowKey(sRefKey)),
									RefData.fromBytes(refData));
					}
				return refMap;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public boolean compareAndRemove(RefKey refKey, RefData oldData)
			throws DhtException, TimeoutException {
		Connection conn = null;

		try {
			// TODO compare

			conn = db.getConnection();
			final PreparedStatement stmt = conn
					.prepareStatement(DELETE_FROM_REF);
			stmt.setString(1, encodeRowKey(refKey.getRepositoryKey()));
			stmt.setString(2, encodeRowKey(refKey));
			stmt.executeUpdate();
			// TODO check result
			return true;
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public boolean compareAndPut(RefKey refKey, RefData oldData, RefData newData)
			throws DhtException, TimeoutException {
		Connection conn = null;

		try {
			// TODO compare

			final String sRefKey = encodeRowKey(refKey);
			final String sRepoKey = encodeRowKey(refKey.getRepositoryKey());
			if (newData != null) {
				conn = db.getConnection();
				PreparedStatement stmt = conn
						.prepareStatement(SELECT_EXISTS_FROM_REF);
				stmt.setString(1, sRepoKey);
				stmt.setString(2, sRefKey);
				stmt.execute();
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next()) {
					// Exists -> update
					stmt = conn.prepareStatement(UPDATE_IN_REF);
					stmt.setBytes(1, newData.toBytes());
					stmt.setString(2, sRepoKey);
					stmt.setString(3, sRefKey);
					stmt.executeUpdate();
				} else {
					// Not exists -> insert
					stmt = conn.prepareStatement(INSERT_INTO_REF);
					stmt.setString(1, sRepoKey);
					stmt.setString(2, sRefKey);
					stmt.setBytes(3, newData.toBytes());
					stmt.executeUpdate();
				}
				// TODO check result
				return true;
			} else {
				throw new DhtException("Invalid parameter"); // TODO
																// externalize
			}
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}
}
