/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.jgit.pgm.Command;
import org.eclipse.jgit.pgm.TextBuiltin;
import org.kohsuke.args4j.Argument;

import com.sap.poc.jgit.storage.jdbc.JdbcDatabaseBuilder;

@Command(name = "jdbc-create-schema")
class JdbcCreateSchema extends TextBuiltin {
	@Argument(index = 0, required = true, metaVar = "git+jdbc+")
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
			final String url = "jdbc:" + builder.getVendor() + "://"
					+ builder.getHost() + "/" + builder.getDatabaseName();
			conn = DriverManager.getConnection(url);
			createRepositoryIndex(conn);
			createRepository(conn);
			createRef(conn);
			createChunk(conn);
			createObjectIndex(conn);
		} catch (SQLException e) {
			throw e;
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// Ignore
				}
		}
	}

	private void createRepositoryIndex(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		final String sql = "CREATE TABLE repository_index (ri_key CHAR(128), ri_name TEXT UNIQUE);";
		System.out.println("-----");
		System.out.println(sql);
		statement.executeUpdate(sql);
	}

	private void createRepository(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		final String sql = "CREATE TABLE repository (r_key CHAR(128), r_chunk_key CHAR(128), r_chunk_info TEXT, r_cached_pack_key CHAR(128), r_cached_pack_info TEXT);";
		System.out.println("-----");
		System.out.println(sql);
		statement.executeUpdate(sql);
	}

	private void createRef(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		final String sql = "CREATE TABLE ref (r_key CHAR(128), r_data TEXT, r_repository_key CHAR(128));";
		System.out.println("-----");
		System.out.println(sql);
		statement.executeUpdate(sql);
	}

	private void createChunk(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		final String sql = "CREATE TABLE chunk (c_key CHAR(128), c_chunk TEXT, c_index TEXT, c_meta TEXT);";
		System.out.println("-----");
		System.out.println(sql);
		statement.executeUpdate(sql);
	}

	private void createObjectIndex(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		final String sql = "CREATE TABLE object_index (oi_key CHAR(128), oi_object_key CHAR(128), oi_object_info TEXT);";
		System.out.println("-----");
		System.out.println(sql);
		statement.executeUpdate(sql);
	}
}
