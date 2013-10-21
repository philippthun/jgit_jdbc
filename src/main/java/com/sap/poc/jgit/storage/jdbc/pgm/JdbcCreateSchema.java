/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.kohsuke.args4j.Argument;

import com.sap.poc.jgit.storage.jdbc.JdbcDatabaseBuilder;
import com.sap.poc.jgit.storage.jdbc.JdbcSqlConstants;

@Command(name = "jdbc-create-schema")
class JdbcCreateSchema extends TextBuiltin implements JdbcSqlConstants {
	@Argument(index = 0, required = true, metaVar = Main.GIT_JDBC_PREFIX)
	String uri;

	@Override
	protected boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		Connection conn = null;

		try {
			final JdbcDatabaseBuilder builder = new JdbcDatabaseBuilder()
					.setURI(uri);
			conn = DriverManager.getConnection("jdbc:" + builder.getVendor()
					+ "://" + builder.getHost() + "/"
					+ builder.getDatabaseName());
			createRepositoryIndexTable(conn);
			createChunkInfoTable(conn);
			createCachedPackTable(conn);
			createRefTable(conn);
			createChunkTable(conn);
			createObjectIndexTable(conn);
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				// Ignore
			}
		}
	}

	private void createRepositoryIndexTable(final Connection conn)
			throws SQLException {
		conn.createStatement().executeUpdate(CREATE_REPO_IDX_TAB);
	}

	private void createChunkInfoTable(final Connection conn)
			throws SQLException {
		conn.createStatement().executeUpdate(CREATE_CHUNK_INFO_TAB);
	}

	private void createCachedPackTable(final Connection conn)
			throws SQLException {
		conn.createStatement().executeUpdate(CREATE_CACH_PACK_TAB);
	}

	private void createRefTable(final Connection conn) throws SQLException {
		conn.createStatement().executeUpdate(CREATE_REF_TAB);
	}

	private void createChunkTable(final Connection conn) throws SQLException {
		conn.createStatement().executeUpdate(CREATE_CHUNK_TAB);
	}

	private void createObjectIndexTable(final Connection conn)
			throws SQLException {
		conn.createStatement().executeUpdate(CREATE_OBJ_IDX_TAB);
	}
}
