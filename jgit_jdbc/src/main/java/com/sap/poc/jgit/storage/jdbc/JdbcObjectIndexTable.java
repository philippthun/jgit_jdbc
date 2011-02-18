/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeChunkKeyShort;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.decodeObjInfo;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeChunkKeyShort;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeObjIdxKey;
import static com.sap.poc.jgit.storage.jdbc.JdbcEnDecoder.encodeObjInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
					final Statement stmt = conn.createStatement();
					stmt.execute(SELECT_M_FROM_OBJ_IDX(encodeObjIdxKey(objIdxKey)));
					final ResultSet resSet = stmt.getResultSet();
					if (resSet != null) {
						final Collection<ObjectInfo> objInfoList = new ArrayList<ObjectInfo>();
						while (resSet.next()) {
							final String sChunkKeyShort = resSet.getString(1);
							final String sObjInfo = resSet.getString(2);
							if (sChunkKeyShort != null
									&& sChunkKeyShort.length() > 0
									&& sObjInfo != null
									&& sObjInfo.length() > 0)
								objInfoList
										.add(ObjectInfo.fromBytes(
												ChunkKey.fromShortBytes(
														objIdxKey,
														decodeChunkKeyShort(sChunkKeyShort)),
												decodeObjInfo(sObjInfo), -1));
						}
						objInfoMap.put(objIdxKey, objInfoList);
					}
				}
				return objInfoMap;
			}
			throw new DhtException("Invalid parameter"); // TODO externalize
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
				conn.createStatement().executeUpdate(
						INSERT_INTO_OBJ_IDX(encodeObjIdxKey(objId),
								encodeChunkKeyShort(info.getChunkKey()),
								encodeObjInfo(info)));
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

	@Override
	public void remove(final ObjectIndexKey objId, final ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		Connection conn = null;

		try {
			if (objId != null && chunk != null) {
				conn = db.getConnection();
				conn.createStatement().executeUpdate(
						DELETE_FROM_OBJ_IDX(encodeObjIdxKey(objId),
								encodeChunkKey(chunk)));
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
