/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

public interface JdbcSqlConstants {
	static final String CACH_PACK_INFO = "cach_pack_info";
	static final String CACH_PACK_KEY = "cach_pack_key";
	static final String CACH_PACK_TAB = "cach_pack";
	static final String CHUNK_DATA = "chunk_data";
	static final String CHUNK_IDX = "chunk_idx";
	static final String CHUNK_INFO = "chunk_info";
	static final String CHUNK_INFO_TAB = "chunk_info";
	static final String CHUNK_KEY = "chunk_key";
	static final String CHUNK_META = "chunk_meta";
	static final String CHUNK_TAB = "chunk";
	static final String OBJ_IDX_KEY = "obj_idx_key";
	static final String OBJ_IDX_TAB = "obj_idx";
	static final String OBJ_INFO = "obj_info";
	static final String REF_DATA = "ref_data";
	static final String REF_KEY = "ref_key";
	static final String REF_TAB = "ref";
	static final String REPO_IDX_TAB = "repo_idx";
	static final String REPO_KEY = "repo_key";
	static final String REPO_NAME = "repo_name";

	static final String CREATE_REPO_IDX_TAB = "CREATE TABLE " + REPO_IDX_TAB
			+ " (" + REPO_KEY + " CHAR(128), " + REPO_NAME + " TEXT);";

	static final String CREATE_CHUNK_INFO_TAB = "CREATE TABLE "
			+ CHUNK_INFO_TAB + " (" + REPO_KEY + " CHAR(128), " + CHUNK_KEY
			+ " CHAR(128), " + CHUNK_INFO + " TEXT);";

	static final String CREATE_CACH_PACK_TAB = "CREATE TABLE " + CACH_PACK_TAB
			+ " (" + REPO_KEY + " CHAR(128), " + CACH_PACK_KEY + " CHAR(128), "
			+ CACH_PACK_INFO + " TEXT);";

	static final String CREATE_REF_TAB = "CREATE TABLE " + REF_TAB + " ("
			+ REPO_KEY + " CHAR(128), " + REF_KEY + " CHAR(128), " + REF_DATA
			+ " TEXT);";

	static final String CREATE_CHUNK_TAB = "CREATE TABLE " + CHUNK_TAB + " ("
			+ CHUNK_KEY + " CHAR(128), " + CHUNK_DATA + " TEXT, " + CHUNK_IDX
			+ " TEXT, " + CHUNK_META + " TEXT);";

	static final String CREATE_OBJ_IDX_TAB = "CREATE TABLE " + OBJ_IDX_TAB
			+ " (" + OBJ_IDX_KEY + " CHAR(128), " + CHUNK_KEY + " CHAR(128), "
			+ OBJ_INFO + " TEXT);";
}
