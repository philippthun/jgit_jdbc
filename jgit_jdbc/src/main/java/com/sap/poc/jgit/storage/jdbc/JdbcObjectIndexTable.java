/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

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
import org.eclipse.jgit.util.Base64;

public class JdbcObjectIndexTable implements ObjectIndexTable {
	private JdbcDatabase database;

	public JdbcObjectIndexTable(JdbcDatabase jdbcDatabase) {
		database = jdbcDatabase;
	}

	@Override
	public void get(
			Context options,
			final Set<ObjectIndexKey> objects,
			final AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
		final Map<ObjectIndexKey, Collection<ObjectInfo>> objectMap = new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
		try {
			if (objects != null)
				for (ObjectIndexKey object : objects) {
					final String dbKey = Base64.encodeBytes(object.toBytes());

					final Statement statement = database.getConnection()
							.createStatement();
					final String sql = "SELECT oi_object_key, oi_object_info FROM object_index WHERE oi_key = '"
							+ dbKey + "'";
					System.out.println("-----");
					System.out.println(sql);
					statement.execute(sql);
					final ResultSet resultSet = statement.getResultSet();
					if (resultSet != null) {
						final Collection<ObjectInfo> infoList = new ArrayList<ObjectInfo>();
						while (resultSet.next()) {
							final String objectKey = resultSet.getString(1);
							final String objectInfo = resultSet.getString(2);

							if (objectKey != null && objectKey.length() > 0
									&& objectInfo != null
									&& objectInfo.length() > 0)
								infoList.add(ObjectInfo.fromBytes(ChunkKey
										.fromBytes(Base64.decode(objectKey)),
										Base64.decode(objectInfo), -1));
						}
						objectMap.put(object, infoList);
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
				callback.onSuccess(objectMap);
			}
		});
	}

	@Override
	public void add(final ObjectIndexKey objId, final ObjectInfo info,
			WriteBuffer buffer) throws DhtException {
		// TODO use buffer
		try {
			if (objId != null && info != null) {
				final String dbKey = Base64.encodeBytes(objId.toBytes());
				final String dbObjectKey = Base64.encodeBytes(info
						.getChunkKey().toBytes());
				final String dbObjectInfo = Base64.encodeBytes(info.toBytes());

				final Statement statement = database.getConnection()
						.createStatement();
				final String sql = "INSERT INTO object_index (oi_key, oi_object_key, oi_object_info) VALUES ('"
						+ dbKey
						+ "', '"
						+ dbObjectKey
						+ "', '"
						+ dbObjectInfo
						+ "')";
				System.out.println("-----");
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		}
	}

	@Override
	public void remove(final ObjectIndexKey objId, ChunkKey chunk,
			WriteBuffer buffer) throws DhtException {
		// TODO use chunk
		// TODO use buffer
		try {
			if (objId != null) {
				final String dbKey = Base64.encodeBytes(objId.toBytes());

				final Statement statement = database.getConnection()
						.createStatement();
				final String sql = "DELETE FROM object_index WHERE oi_key = '"
						+ dbKey + "'";
				System.out.println("-----");
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			throw new DhtException(e);
		}
	}
}
