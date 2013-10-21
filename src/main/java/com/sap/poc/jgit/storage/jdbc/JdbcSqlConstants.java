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

	/*
	 * REPO_KEY: 8 bytes, (US-ASCII) characters
	 * 
	 * REPO_NAME: characters, variable length, maps to a directory on file
	 * system, assume max. filename length is 256 (ext4), plus ".git" ending =>
	 * 256 + 4 = 260
	 */
	static final String CREATE_REPO_IDX_TAB = "CREATE TABLE " + REPO_IDX_TAB
			+ " (" + REPO_KEY + " CHAR(8), " + REPO_NAME + " VARCHAR(260));";

	/*
	 * REPO_KEY: 8 bytes, (US-ASCII) characters
	 * 
	 * CHUNK_KEY: 59 bytes, (US-ASCII) characters
	 * 
	 * CHUNK_INFO: byte array, variable length, max. length ? (TODO 128?)
	 */
	static final String CREATE_CHUNK_INFO_TAB = "CREATE TABLE "
			+ CHUNK_INFO_TAB + " (" + REPO_KEY + " CHAR(8), " + CHUNK_KEY
			+ " CHAR(59), " + CHUNK_INFO + " BYTEA);";

	/*
	 * REPO_KEY: 8 bytes, (US-ASCII) characters
	 * 
	 * CACH_PACK_KEY: 81 bytes, (US-ASCII) characters
	 * 
	 * CACH_PACK_INFO: byte array, variable length, max. length ? (TODO 1024?)
	 */
	static final String CREATE_CACH_PACK_TAB = "CREATE TABLE " + CACH_PACK_TAB
			+ " (" + REPO_KEY + " CHAR(8), " + CACH_PACK_KEY + " CHAR(81), "
			+ CACH_PACK_INFO + " BYTEA);";

	/*
	 * REPO_KEY: 8 bytes, (US-ASCII) characters
	 * 
	 * REF_KEY: characters, variable length, limit to 1000? (TODO ?)
	 * 
	 * REF_DATA: byte array, variable length, max. length ? (TODO ?)
	 */
	static final String CREATE_REF_TAB = "CREATE TABLE " + REF_TAB + " ("
			+ REPO_KEY + " CHAR(8), " + REF_KEY + " VARCHAR(1000), " + REF_DATA
			+ " BYTEA);";

	/*
	 * CHUNK_KEY: 59 bytes, (US-ASCII) characters
	 * 
	 * CHUNK_DATA: binary, variable length, large
	 * 
	 * CHUNK_IDX: binary, variable length, large
	 * 
	 * CHUNK_META: byte array, variable length, max. length ? (TODO 256?)
	 */
	static final String CREATE_CHUNK_TAB = "CREATE TABLE " + CHUNK_TAB + " ("
			+ CHUNK_KEY + " CHAR(59), " + CHUNK_DATA + " BYTEA, " + CHUNK_IDX
			+ " BYTEA, " + CHUNK_META + " BYTEA);";

	/*
	 * OBJ_IDX_KEY: 54 bytes, (US-ASCII) characters
	 * 
	 * CHUNK_KEY: short form, 45 bytes, (US-ASCII) characters
	 * 
	 * OBJ_INFO: byte array, variable length, max. length 34
	 */
	static final String CREATE_OBJ_IDX_TAB = "CREATE TABLE " + OBJ_IDX_TAB
			+ " (" + OBJ_IDX_KEY + " CHAR(54), " + CHUNK_KEY + " CHAR(45), "
			+ OBJ_INFO + " BYTEA);";

	static final String SELECT_EXISTS_FROM_REPO_IDX = "SELECT 1 FROM "
			+ REPO_IDX_TAB + " WHERE " + REPO_NAME + " = ?;";

	static final String SELECT_REPO_KEY_FROM_REPO_IDX = "SELECT " + REPO_KEY
			+ " FROM " + REPO_IDX_TAB + " WHERE " + REPO_NAME + " = ?;";

	static final String INSERT_INTO_REPO_IDX = "INSERT INTO " + REPO_IDX_TAB
			+ " (" + REPO_KEY + ", " + REPO_NAME + ") VALUES (?, ?);";

	static final String SELECT_EXISTS_FROM_CHUNK_INFO = "SELECT 1 FROM "
			+ CHUNK_INFO_TAB + " WHERE " + REPO_KEY + " = ? AND " + CHUNK_KEY
			+ " = ?;";

	static final String INSERT_INTO_CHUNK_INFO = "INSERT INTO "
			+ CHUNK_INFO_TAB + " (" + REPO_KEY + ", " + CHUNK_KEY + ", "
			+ CHUNK_INFO + ") VALUES " + "(?, ?, ?);";

	static final String UPDATE_IN_CHUNK_INFO = "UPDATE " + CHUNK_INFO_TAB
			+ " SET " + CHUNK_INFO + " = ? WHERE " + REPO_KEY + " = ? AND "
			+ CHUNK_KEY + " = ?;";

	static final String DELETE_FROM_CHUNK_INFO = "DELETE FROM "
			+ CHUNK_INFO_TAB + " WHERE " + REPO_KEY + " = ? AND " + CHUNK_KEY
			+ " = ?;";

	static final String SELECT_EXISTS_FROM_CACH_PACK = "SELECT 1 FROM "
			+ CACH_PACK_TAB + " WHERE " + REPO_KEY + " = ? AND "
			+ CACH_PACK_KEY + " = ?;";

	static final String SELECT_M_FROM_CACH_PACK = "SELECT " + CACH_PACK_INFO
			+ " FROM " + CACH_PACK_TAB + " WHERE " + REPO_KEY + " = ?;";

	static final String INSERT_INTO_CACH_PACK = "INSERT INTO " + CACH_PACK_TAB
			+ " (" + REPO_KEY + ", " + CACH_PACK_KEY + ", " + CACH_PACK_INFO
			+ ") VALUES " + "(?, ?, ?);";

	static final String UPDATE_IN_CACH_PACK = "UPDATE " + CACH_PACK_TAB
			+ " SET " + CACH_PACK_INFO + " = ? WHERE " + REPO_KEY + " = ? AND "
			+ CACH_PACK_KEY + " = ?;";

	static final String DELETE_FROM_CACH_PACK = "DELETE FROM " + CACH_PACK_TAB
			+ " WHERE " + REPO_KEY + " = ? AND " + CACH_PACK_KEY + " = ?;";

	static final String SELECT_EXISTS_FROM_REF = "SELECT 1 FROM " + REF_TAB
			+ " WHERE " + REPO_KEY + " = ? AND " + REF_KEY + " = ?;";

	static final String SELECT_FROM_REF = "SELECT " + REF_DATA + " FROM "
			+ REF_TAB + " WHERE " + REPO_KEY + " = ? AND " + REF_KEY + " = ?;";

	static final String SELECT_M_FROM_REF = "SELECT " + REF_KEY + ", "
			+ REF_DATA + " FROM " + REF_TAB + " WHERE " + REPO_KEY + " = ?;";

	static final String INSERT_INTO_REF = "INSERT INTO " + REF_TAB + " ("
			+ REPO_KEY + ", " + REF_KEY + ", " + REF_DATA + ") VALUES "
			+ "(?, ?, ?);";

	static final String UPDATE_IN_REF = "UPDATE " + REF_TAB + " SET "
			+ REF_DATA + " = ? WHERE " + REPO_KEY + " = ? AND " + REF_KEY
			+ " = ?;";

	static final String DELETE_FROM_REF = "DELETE FROM " + REF_TAB + " WHERE "
			+ REPO_KEY + " = ? AND " + REF_KEY + " = ?;";

	static final String SELECT_FROM_CHUNK = "SELECT " + CHUNK_DATA + ", "
			+ CHUNK_IDX + ", " + CHUNK_META + " FROM " + CHUNK_TAB + " WHERE "
			+ CHUNK_KEY + " = ?;";

	static final String INSERT_INTO_CHUNK = "INSERT INTO " + CHUNK_TAB + " ("
			+ CHUNK_KEY + ", " + CHUNK_DATA + ") VALUES (?, ?);";

	static final String UPDATE_IN_CHUNK = "UPDATE " + CHUNK_TAB + " SET "
			+ CHUNK_IDX + " = ?, " + CHUNK_META + " = ? WHERE " + CHUNK_KEY
			+ " = ?;";

	static final String DELETE_FROM_CHUNK = "DELETE FROM " + CHUNK_TAB
			+ " WHERE " + CHUNK_KEY + " = ?;";

	static final String SELECT_M_FROM_OBJ_IDX = "SELECT " + CHUNK_KEY + ", "
			+ OBJ_INFO + " FROM " + OBJ_IDX_TAB + " WHERE " + OBJ_IDX_KEY
			+ " = ?;";

	static final String INSERT_INTO_OBJ_IDX = "INSERT INTO " + OBJ_IDX_TAB
			+ " (" + OBJ_IDX_KEY + ", " + CHUNK_KEY + ", " + OBJ_INFO
			+ ") VALUES (?, ?, ?);";

	static final String DELETE_FROM_OBJ_IDX = "DELETE FROM " + OBJ_IDX_TAB
			+ " WHERE " + OBJ_IDX_KEY + " = ? AND " + CHUNK_KEY + " = ?;";
}
