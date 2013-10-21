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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jgit.storage.dht.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class JdbcRepositoryTable extends JdbcSqlHelper implements
		RepositoryTable {
	private final AtomicInteger nextId = new AtomicInteger();
	private JdbcDatabase db;

	public JdbcRepositoryTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public void put(final RepositoryKey repo, final ChunkInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && info != null) {
				final String sRepoKey = encodeRowKey(repo);
				final String sChunkKey = encodeRowKey(info.getChunkKey());
				conn = db.getConnection();
				PreparedStatement stmt = conn
						.prepareStatement(SELECT_EXISTS_FROM_CHUNK_INFO);
				stmt.setString(1, sRepoKey);
				stmt.setString(2, sChunkKey);
				stmt.execute();
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next()) {
					// Exists -> update
					stmt = conn.prepareStatement(UPDATE_IN_CHUNK_INFO);
					stmt.setBytes(1, info.toBytes());
					stmt.setString(2, sRepoKey);
					stmt.setString(3, sChunkKey);
					stmt.executeUpdate();
				} else {
					// Not exists -> insert
					stmt = conn.prepareStatement(INSERT_INTO_CHUNK_INFO);
					stmt.setString(1, sRepoKey);
					stmt.setString(2, sChunkKey);
					stmt.setBytes(3, info.toBytes());
					stmt.executeUpdate();
				}
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && chunk != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(DELETE_FROM_CHUNK_INFO);
				stmt.setString(1, encodeRowKey(repo));
				stmt.setString(2, encodeRowKey(chunk));
				stmt.executeUpdate();
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public Collection<CachedPackInfo> getCachedPacks(final RepositoryKey repo)
			throws DhtException, TimeoutException {
		final Collection<CachedPackInfo> cachedPackInfoList = new ArrayList<CachedPackInfo>();
		Connection conn = null;

		try {
			if (repo != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(SELECT_M_FROM_CACH_PACK);
				stmt.setString(1, encodeRowKey(repo));
				stmt.execute();
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null)
					while (resSet.next()) {
						final byte[] cachedPackInfo = resSet.getBytes(1);
						if (cachedPackInfo != null && cachedPackInfo.length > 0)
							cachedPackInfoList.add(CachedPackInfo.fromBytes(
									repo, cachedPackInfo));
					}
				return cachedPackInfoList;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (final SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void put(final RepositoryKey repo, final CachedPackInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && info != null) {
				final String sRepoKey = encodeRowKey(repo);
				final String sCachedPackKey = encodeRowKey(info.getRowKey());
				conn = db.getConnection();
				PreparedStatement stmt = conn
						.prepareStatement(SELECT_EXISTS_FROM_CACH_PACK);
				stmt.setString(1, sRepoKey);
				stmt.setString(2, sCachedPackKey);
				stmt.execute();
				final ResultSet resSet = stmt.getResultSet();
				if (resSet != null && resSet.next()) {
					// Exists -> update
					stmt = conn.prepareStatement(UPDATE_IN_CACH_PACK);
					stmt.setBytes(1, info.toBytes());
					stmt.setString(2, sRepoKey);
					stmt.setString(3, sCachedPackKey);
					stmt.executeUpdate();
				} else {
					// Not exists -> insert
					stmt = conn.prepareStatement(INSERT_INTO_CACH_PACK);
					stmt.setString(1, sRepoKey);
					stmt.setString(2, sCachedPackKey);
					stmt.setBytes(3, info.toBytes());
					stmt.executeUpdate();
				}
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public void remove(final RepositoryKey repo, final CachedPackKey key,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (repo != null && key != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(DELETE_FROM_CACH_PACK);
				stmt.setString(1, encodeRowKey(repo));
				stmt.setString(2, encodeRowKey(key));
				stmt.executeUpdate();
				// TODO check result
				return;
			}
			throw new DhtException("Invalid parameters"); // TODO externalize
		} catch (UnsupportedEncodingException e) {
			throw new DhtException(e);
		} catch (SQLException e) {
			throw new DhtException(e);
		} finally {
			closeConnection(conn);
		}
	}

	@Override
	public RepositoryKey nextKey() throws DhtException {
		return RepositoryKey.create(nextId.incrementAndGet());
	}
}
