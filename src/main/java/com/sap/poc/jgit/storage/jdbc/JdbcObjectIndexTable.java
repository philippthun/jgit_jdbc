/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeRowKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeRowKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeShortChunkKey;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

public class JdbcObjectIndexTable extends JdbcSqlHelper implements
		ObjectIndexTable {
	private JdbcDatabase db;

	public JdbcObjectIndexTable(final JdbcDatabase db) {
		this.db = db;
	}

	@Override
	public void get(
			final Context options,
			final Set<ObjectIndexKey> objects,
			final AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
		db.getExecutorService().submit(new Runnable() {
			@Override
			public void run() {
				try {
					callback.onSuccess(getObjectInfos(options, objects));
				} catch (DhtException e) {
					callback.onFailure(e);
				}
			}
		});
	}

	private Map<ObjectIndexKey, Collection<ObjectInfo>> getObjectInfos(
			Context options, final Set<ObjectIndexKey> objects)
			throws DhtException {
		// TODO use options
		Connection conn = null;

		try {
			final Map<ObjectIndexKey, Collection<ObjectInfo>> objInfoMap = new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
			if (objects != null) {
				for (final ObjectIndexKey objIdxKey : objects) {
					conn = db.getConnection();
					final PreparedStatement stmt = conn
							.prepareStatement(SELECT_M_FROM_OBJ_IDX);
					stmt.setString(1, encodeRowKey(objIdxKey));
					stmt.execute();
					final ResultSet resSet = stmt.getResultSet();
					if (resSet != null) {
						final Collection<ObjectInfo> objInfoList = new ArrayList<ObjectInfo>();
						while (resSet.next()) {
							final String sShortChunkKey = resSet.getString(1);
							final byte[] objInfo = resSet.getBytes(2);
							if (sShortChunkKey != null
									&& sShortChunkKey.length() > 0
									&& objInfo != null && objInfo.length > 0)
								objInfoList.add(ObjectInfo.fromBytes(ChunkKey
										.fromShortBytes(objIdxKey,
												decodeRowKey(sShortChunkKey)),
										objInfo, -1));
						}
						objInfoMap.put(objIdxKey, objInfoList);
					}
				}
				return objInfoMap;
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
	public void add(final ObjectIndexKey objId, final ObjectInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (objId != null && info != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(INSERT_INTO_OBJ_IDX);
				stmt.setString(1, encodeRowKey(objId));
				stmt.setString(2, encodeShortChunkKey(info.getChunkKey()));
				stmt.setBytes(3, info.toBytes());
				stmt.execute();
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
	public void remove(final ObjectIndexKey objId, final ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (objId != null && chunk != null) {
				conn = db.getConnection();
				final PreparedStatement stmt = conn
						.prepareStatement(DELETE_FROM_OBJ_IDX);
				stmt.setString(1, encodeRowKey(objId));
				stmt.setString(2, encodeShortChunkKey(chunk));
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
}
