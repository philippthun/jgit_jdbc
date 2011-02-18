/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeChunkData;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeChunkIdx;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeChunkMeta;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkData;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkIdx;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkMeta;

import java.sql.Connection;
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

public class JdbcChunkTable extends JdbcSqlHelper implements ChunkTable {
	private JdbcDatabase db;

	public JdbcChunkTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public void get(final Context options, final Set<ChunkKey> keys,
			final AsyncCallback<Collection<Members>> callback) {
		db.getExecutorService().submit(new Runnable() {
			@Override
			public void run() {
				try {
					callback.onSuccess(getMembers(options, keys));
				} catch (DhtException e) {
					callback.onFailure(e);
				}
			}
		});
	}

	private Collection<Members> getMembers(Context options,
			final Set<ChunkKey> keys) throws DhtException {
		// TODO use options
		Connection conn = null;

		try {
			final Collection<Members> memberList = new ArrayList<Members>();
			if (keys != null) {
				for (ChunkKey chunkKey : keys) {
					conn = db.getConnection();
					final Statement stmt = conn.createStatement();
					stmt.execute(SELECT_FROM_CHUNK(encodeChunkKey(chunkKey)));
					final ResultSet resSet = stmt.getResultSet();
					if (resSet != null && resSet.next()) {
						// Exists
						final String sChunkData = resSet.getString(1);
						final String sChunkIdx = resSet.getString(2);
						final String sChunkMeta = resSet.getString(3);
						final Members member = new Members();
						member.setChunkKey(chunkKey);
						if (sChunkData != null && sChunkData.length() > 0)
							member.setChunkData(decodeChunkData(sChunkData));
						if (sChunkIdx != null && sChunkIdx.length() > 0)
							member.setChunkIndex(decodeChunkIdx(sChunkIdx));
						if (sChunkMeta != null && sChunkMeta.length() > 0)
							member.setMeta(ChunkMeta.fromBytes(chunkKey,
									decodeChunkMeta(sChunkMeta)));
						memberList.add(member);
					}
				}
				return memberList;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void put(final Members chunk, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (chunk != null) {
				final String sChunkKey = encodeChunkKey(chunk.getChunkKey());
				conn = db.getConnection();
				if (chunk.getChunkData() != null) {
					// Assume insert of ChunkData
					conn.createStatement().executeUpdate(
							INSERT_INTO_CHUNK(sChunkKey,
									encodeChunkData(chunk.getChunkData())));
				} else {
					// Assume update of ChunkIndex and ChunkMeta
					final String sChunkIdx = encodeChunkIdx(chunk
							.getChunkIndex());
					final String sChunkMeta = encodeChunkMeta(chunk.getMeta());
					conn.createStatement().executeUpdate(
							UPDATE_IN_CHUNK(sChunkKey,
									sChunkIdx != null ? sChunkIdx : "",
									sChunkMeta != null ? sChunkMeta : ""));
				}
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void remove(final ChunkKey key, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (key != null) {
				conn = db.getConnection();
				conn.createStatement().executeUpdate(
						DELETE_FROM_CHUNK(encodeChunkKey(key)));
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}
}
