/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

class JdbcSqlHelper implements JdbcSqlConstants {
	static void closeConnection(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			// Ignore
		}
	}

	static String SELECT_EXISTS_FROM_REPO_IDX(final String sRepoName) {
		// TODO select single
		return SELECT_REPO_KEY_FROM_REPO_IDX(sRepoName);
	}

	static String SELECT_REPO_KEY_FROM_REPO_IDX(final String sRepoName) {
		return "SELECT " + REPO_KEY + " FROM " + REPO_IDX_TAB + " WHERE "
				+ REPO_NAME + " = '" + sRepoName + "';";
	}

	static String INSERT_INTO_REPO_IDX(final String sRepoKey,
			final String sRepoName) {
		return "INSERT INTO " + REPO_IDX_TAB + " (" + REPO_KEY + ", "
				+ REPO_NAME + ") VALUES ('" + sRepoKey + "', '" + sRepoName
				+ "');";
	}

	static String SELECT_EXISTS_FROM_CHUNK_INFO(final String sRepoKey,
			final String sChunkKey) {
		// TODO select single
		return "SELECT " + CHUNK_KEY + " FROM " + CHUNK_INFO_TAB + " WHERE "
				+ REPO_KEY + " = '" + sRepoKey + "' AND " + CHUNK_KEY + " = '"
				+ sChunkKey + "';";
	}

	static String INSERT_INTO_CHUNK_INFO(final String sRepoKey,
			final String sChunkKey, final String sChunkInfo) {
		return "INSERT INTO " + CHUNK_INFO_TAB + " (" + REPO_KEY + ", "
				+ CHUNK_KEY + ", " + CHUNK_INFO + ") VALUES " + "('" + sRepoKey
				+ "', '" + sChunkKey + "', '" + sChunkInfo + "');";
	}

	static String UPDATE_IN_CHUNK_INFO(final String sRepoKey,
			final String sChunkKey, final String sChunkInfo) {
		return "UPDATE " + CHUNK_INFO_TAB + " SET " + CHUNK_INFO + " = '"
				+ sChunkInfo + "' WHERE " + REPO_KEY + " = '" + sRepoKey
				+ "' AND " + CHUNK_KEY + " = '" + sChunkKey + "';";
	}

	static String DELETE_FROM_CHUNK_INFO(final String sRepoKey,
			final String sChunkKey) {
		return "DELETE FROM " + CHUNK_INFO_TAB + " WHERE " + REPO_KEY + " = '"
				+ sRepoKey + "' AND " + CHUNK_KEY + " = '" + sChunkKey + "';";
	}

	static String SELECT_EXISTS_FROM_CACH_PACK(final String sRepoKey,
			final String sCachPackKey) {
		// TODO select single
		return "SELECT " + CACH_PACK_KEY + " FROM " + CACH_PACK_TAB + " WHERE "
				+ REPO_KEY + " = '" + sRepoKey + "' AND " + CACH_PACK_KEY
				+ " = '" + sCachPackKey + "';";
	}

	static String SELECT_M_FROM_CACH_PACK(final String sRepoKey) {
		return "SELECT " + CACH_PACK_INFO + " FROM " + CACH_PACK_TAB
				+ " WHERE " + REPO_KEY + " = '" + sRepoKey + "' AND NOT "
				+ CACH_PACK_KEY + " = '';";
	}

	static String INSERT_INTO_CACH_PACK(final String sRepoKey,
			final String sCachPackKey, final String sCachPackInfo) {
		return "INSERT INTO " + CACH_PACK_TAB + " (" + REPO_KEY + ", "
				+ CACH_PACK_KEY + ", " + CACH_PACK_INFO + ") VALUES " + "('"
				+ sRepoKey + "', '" + sCachPackKey + "', '" + sCachPackInfo
				+ "');";
	}

	static String UPDATE_IN_CACH_PACK(final String sRepoKey,
			final String sCachPackKey, final String sCachPackInfo) {
		return "UPDATE " + CACH_PACK_TAB + " SET " + CACH_PACK_INFO + " = '"
				+ sCachPackInfo + "' WHERE " + REPO_KEY + " = '" + sRepoKey
				+ "' AND " + CACH_PACK_KEY + " = '" + sCachPackKey + "';";
	}

	static String DELETE_FROM_CACH_PACK(final String sRepoKey,
			final String sCachPackKey) {
		return "DELETE FROM " + CACH_PACK_TAB + " WHERE " + REPO_KEY + " = '"
				+ sRepoKey + "' AND " + CACH_PACK_KEY + " = '" + sCachPackKey
				+ "';";
	}

	static String SELECT_EXISTS_FROM_REF(final String sRepoKey,
			final String sRefKey) {
		// TODO select single
		return "SELECT " + REF_KEY + " FROM " + REF_TAB + " WHERE " + REPO_KEY
				+ " = '" + sRepoKey + "' AND " + REF_KEY + " = '" + sRefKey
				+ "';";
	}

	static String SELECT_FROM_REF(final String sRepoKey, final String sRefKey) {
		return "SELECT " + REF_DATA + " FROM " + REF_TAB + " WHERE " + REPO_KEY
				+ " = '" + sRepoKey + "' AND " + REF_KEY + " = '" + sRefKey
				+ "';";
	}

	static String SELECT_M_FROM_REF(final String sRepoKey) {
		return "SELECT " + REF_KEY + ", " + REF_DATA + " FROM " + REF_TAB
				+ " WHERE " + REPO_KEY + " = '" + sRepoKey + "';";
	}

	static String INSERT_INTO_REF(final String sRepoKey, final String sRefKey,
			final String sRefData) {
		return "INSERT INTO " + REF_TAB + " (" + REPO_KEY + ", " + REF_KEY
				+ ", " + REF_DATA + ") VALUES " + "('" + sRepoKey + "', '"
				+ sRefKey + "', '" + sRefData + "');";
	}

	static String UPDATE_IN_REF(final String sRepoKey, final String sRefKey,
			final String sRefData) {
		return "UPDATE " + REF_TAB + " SET " + REF_DATA + " = '" + sRefData
				+ "' WHERE " + REPO_KEY + " = '" + sRepoKey + "' AND "
				+ REF_KEY + " = '" + sRefKey + "';";
	}

	static String DELETE_FROM_REF(final String sRepoKey, final String sRefKey) {
		return "DELETE FROM " + REF_TAB + " WHERE " + REPO_KEY + " = '"
				+ sRepoKey + "' AND " + REF_KEY + " = '" + sRefKey + "';";
	}

	static String SELECT_FROM_CHUNK(final String chunkKey) {
		return "SELECT " + CHUNK_DATA + ", " + CHUNK_IDX + ", " + CHUNK_META
				+ " FROM " + CHUNK_TAB + " WHERE " + CHUNK_KEY + " = '"
				+ chunkKey + "';";
	}

	static String INSERT_INTO_CHUNK(final String chunkKey,
			final String chunkData) {
		return "INSERT INTO " + CHUNK_TAB + " (" + CHUNK_KEY + ", "
				+ CHUNK_DATA + ", " + CHUNK_IDX + ", " + CHUNK_META
				+ ") VALUES ('" + chunkKey + "', '" + chunkData + "', '', '');";
	}

	static String UPDATE_IN_CHUNK(final String chunkKey, final String chunkIdx,
			final String chunkMeta) {
		return "UPDATE " + CHUNK_TAB + " SET " + CHUNK_IDX + " = '" + chunkIdx
				+ "', " + CHUNK_META + " = '" + chunkMeta + "' WHERE "
				+ CHUNK_KEY + " = '" + chunkKey + "';";
	}

	static String DELETE_FROM_CHUNK(final String chunkKey) {
		return "DELETE FROM " + CHUNK_TAB + " WHERE " + CHUNK_KEY + " = '"
				+ chunkKey + "';";
	}

	static String SELECT_M_FROM_OBJ_IDX(final String sObjIdxKey) {
		return "SELECT " + CHUNK_KEY + ", " + OBJ_INFO + " FROM " + OBJ_IDX_TAB
				+ " WHERE " + OBJ_IDX_KEY + " = '" + sObjIdxKey + "';";
	}

	static String INSERT_INTO_OBJ_IDX(final String sObjIdxKey,
			final String sChunkKey, final String sObjInfo) {
		return "INSERT INTO " + OBJ_IDX_TAB + " (" + OBJ_IDX_KEY + ", "
				+ CHUNK_KEY + ", " + OBJ_INFO + ") VALUES ('" + sObjIdxKey
				+ "', '" + sChunkKey + "', '" + sObjInfo + "');";
	}

	static String DELETE_FROM_OBJ_IDX(final String sObjIdxKey,
			final String sChunkKey) {
		return "DELETE FROM " + OBJ_IDX_TAB + " WHERE " + OBJ_IDX_KEY + " = '"
				+ sObjIdxKey + "' AND " + CHUNK_KEY + " = '" + sChunkKey + "';";
	}
}
