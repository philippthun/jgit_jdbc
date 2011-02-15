/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.ChunkMeta;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk.Members;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.util.Base64;

public class JdbcChunkTable implements ChunkTable {
	private JdbcDatabase database;

	public JdbcChunkTable(JdbcDatabase jdbcDatabase) {
		database = jdbcDatabase;
	}

	@Override
	public void get(Context options, final Set<ChunkKey> keys,
			final AsyncCallback<Collection<Members>> callback) {
		final Collection<Members> memberList = new ArrayList<Members>();
		try {
			if (keys != null)
				for (ChunkKey key : keys) {
					final String dbKey = Base64.encodeBytes(key.toBytes());

					final Statement statement = database.getConnection()
							.createStatement();
					final String sql = "SELECT c_chunk, c_index, c_meta FROM chunk WHERE c_key = '"
							+ dbKey + "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.execute(sql);
					final ResultSet resultSet = statement.getResultSet();
					if (resultSet != null && resultSet.next()) {
						// Exists
						final String chunk = resultSet.getString(1);
						final String index = resultSet.getString(2);
						final String meta = resultSet.getString(3);

						final Members member = new Members();
						member.setChunkKey(key);
						if (chunk != null && chunk.length() > 0)
							member.setChunkData(Base64.decode(chunk));
						if (index != null && index.length() > 0)
							member.setChunkIndex(Base64.decode(index));
						if (meta != null && meta.length() > 0)
							member.setMeta(ChunkMeta.fromBytes(key,
									Base64.decode(meta)));
						memberList.add(member);
					}
				}
		} catch (final SQLException e) {
			database.getExecutorService().submit(new Runnable() {
				@Override
				public void run() {
					callback.onFailure(new DhtException(e));
				}
			});
		}

		database.getExecutorService().submit(new Runnable() {
			@Override
			public void run() {
				callback.onSuccess(memberList);
			}
		});
	}

	@Override
	public void put(final Members chunk, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		try {
			if (chunk != null) {
				final String dbKey = Base64.encodeBytes(chunk.getChunkKey()
						.toBytes());
				String dbChunk = chunk.getChunkData() != null ? Base64
						.encodeBytes(chunk.getChunkData()) : "";
				String dbIndex = chunk.getChunkIndex() != null ? Base64
						.encodeBytes(chunk.getChunkIndex()) : "";
				String dbMeta = chunk.getMeta() != null ? Base64
						.encodeBytes(chunk.getMeta().toBytes()) : "";

				Statement statement = database.getConnection()
						.createStatement();
				String sql = "SELECT c_chunk, c_index, c_meta FROM chunk WHERE c_key = '"
						+ dbKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.execute(sql);
				final ResultSet resultSet = statement.getResultSet();
				if (resultSet != null && resultSet.next()) {
					// Exists -> update
					if (dbChunk.length() == 0)
						dbChunk = resultSet.getString(1);
					if (dbIndex.length() == 0)
						dbIndex = resultSet.getString(2);
					if (dbMeta.length() == 0)
						dbMeta = resultSet.getString(3);

					statement = database.getConnection().createStatement();
					sql = "UPDATE chunk SET c_chunk = '" + dbChunk
							+ "', c_index = '" + dbIndex + "', c_meta = '"
							+ dbMeta + "' WHERE c_key = '" + dbKey + "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				} else {
					// Not exists -> insert
					statement = database.getConnection().createStatement();
					sql = "INSERT INTO chunk (c_key, c_chunk, c_index, c_meta) VALUES ('"
							+ dbKey
							+ "', '"
							+ dbChunk
							+ "', '"
							+ dbIndex
							+ "', '" + dbMeta + "')";
					System.out.println("-----");
					System.out.println(sql);
					statement.executeUpdate(sql);
				}
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		}
	}

	@Override
	public void remove(final ChunkKey key, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		try {
			if (key != null) {
				final String dbKey = Base64.encodeBytes(key.toBytes());

				final Statement statement = database.getConnection()
						.createStatement();
				final String sql = "DELETE FROM chunk WHERE c_key = '" + dbKey
						+ "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		}
	}
}
