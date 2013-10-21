/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 * and other copyright owners as documented in the project's IP log.
 *
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which
 * accompanies this distribution, is reproduced below, and is
 * available at http://www.eclipse.org/org/documents/edl-v10.php
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Eclipse Foundation, Inc. nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRowKey;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
					final PreparedStatement stmt = conn
							.prepareStatement(SELECT_FROM_CHUNK);
					stmt.setString(1, encodeRowKey(chunkKey));
					stmt.execute();
					final ResultSet resSet = stmt.getResultSet();
					if (resSet != null && resSet.next()) {
						// Exists
						final byte[] chunkData = resSet.getBytes(1);
						final byte[] chunkIdx = resSet.getBytes(2);
						final byte[] chunkMeta = resSet.getBytes(3);
						final Members member = new Members();
						member.setChunkKey(chunkKey);
						if (chunkData != null && chunkData.length > 0)
							member.setChunkData(chunkData);
						if (chunkIdx != null && chunkIdx.length > 0)
							member.setChunkIndex(chunkIdx);
						if (chunkMeta != null && chunkMeta.length > 0)
							member.setMeta(ChunkMeta.fromBytes(chunkKey,
									chunkMeta));
						memberList.add(member);
					}
				}
				return memberList;
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
	public void put(final Members chunk, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (chunk != null) {
				final String sChunkKey = encodeRowKey(chunk.getChunkKey());
				conn = db.getConnection();
				if (chunk.getChunkData() != null) {
					// Assume insert of ChunkData
					final PreparedStatement stmt = conn
							.prepareStatement(INSERT_INTO_CHUNK);
					stmt.setString(1, sChunkKey);
					stmt.setBytes(2, chunk.getChunkData());
					stmt.executeUpdate();
				} else {
					// Assume update of ChunkIndex and ChunkMeta
					final byte[] chunkIdx = chunk.getChunkIndex();
					final ChunkMeta chunkMeta = chunk.getMeta();
					final PreparedStatement stmt = conn
							.prepareStatement(UPDATE_IN_CHUNK);
					stmt.setBytes(1, chunkIdx != null ? chunkIdx : new byte[0]);
					stmt.setBytes(2, chunkMeta != null ? chunkMeta.toBytes()
							: new byte[0]);
					stmt.setString(3, sChunkKey);
					stmt.executeUpdate();
				}
				// TODO check result
				return;
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
	public void remove(final ChunkKey key, WriteBuffer buffer)
			throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (key != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(DELETE_FROM_CHUNK);
				stmt.setString(1, encodeRowKey(key));
				stmt.executeUpdate();
				// TODO check result
				return;
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
}
