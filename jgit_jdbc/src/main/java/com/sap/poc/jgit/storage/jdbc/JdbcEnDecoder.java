/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import org.eclipse.jgit.storage.dht.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.ChunkMeta;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.RefData;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.util.Base64;

class JdbcEnDecoder {
	static String encodeRepoKey(final RepositoryKey repoKey) {
		if (repoKey != null)
			return Base64.encodeBytes(repoKey.toBytes());
		else
			return null;
	}

	static byte[] decodeRepoKey(final String sRepoKey) {
		if (sRepoKey != null)
			return Base64.decode(sRepoKey);
		else
			return null;
	}

	static String encodeRepoName(final RepositoryName repoName) {
		if (repoName != null)
			return repoName.asString();
		else
			return null;
	}

	static String encodeChunkKey(final ChunkKey chunkKey) {
		if (chunkKey != null)
			return Base64.encodeBytes(chunkKey.toBytes());
		else
			return null;
	}

	static String encodeChunkKeyShort(final ChunkKey chunkKey) {
		if (chunkKey != null)
			return Base64.encodeBytes(chunkKey.toShortBytes());
		else
			return null;
	}

	static byte[] decodeChunkKey(final String sChunkKey) {
		if (sChunkKey != null)
			return Base64.decode(sChunkKey);
		else
			return null;
	}

	static byte[] decodeChunkKeyShort(final String sChunkKeyShort) {
		if (sChunkKeyShort != null)
			return Base64.decode(sChunkKeyShort);
		else
			return null;
	}

	static String encodeChunkInfo(final ChunkInfo chunkInfo) {
		if (chunkInfo != null)
			return Base64.encodeBytes(chunkInfo.toBytes());
		else
			return null;
	}

	static String encodeCachPackKey(final CachedPackKey cachPackKey) {
		if (cachPackKey != null)
			return Base64.encodeBytes(cachPackKey.toBytes());
		else
			return null;
	}

	static String encodeCachPackInfo(final CachedPackInfo cachPackInfo) {
		if (cachPackInfo != null)
			return Base64.encodeBytes(cachPackInfo.toBytes());
		else
			return null;
	}

	static byte[] decodeCachPackInfo(final String sCachPackInfo) {
		if (sCachPackInfo != null)
			return Base64.decode(sCachPackInfo);
		else
			return null;
	}

	static String encodeRefKey(final RefKey refKey) {
		if (refKey != null)
			return Base64.encodeBytes(refKey.toBytes());
		else
			return null;
	}

	static byte[] decodeRefKey(final String sRefKey) {
		if (sRefKey != null)
			return Base64.decode(sRefKey);
		else
			return null;
	}

	static String encodeRefData(final RefData refData) {
		if (refData != null)
			return Base64.encodeBytes(refData.toBytes());
		else
			return null;
	}

	static byte[] decodeRefData(final String sRefData) {
		if (sRefData != null)
			return Base64.decode(sRefData);
		else
			return null;
	}

	static String encodeChunkData(final byte[] chunkData) {
		if (chunkData != null)
			return Base64.encodeBytes(chunkData);
		else
			return null;
	}

	static byte[] decodeChunkData(final String sChunkData) {
		if (sChunkData != null)
			return Base64.decode(sChunkData);
		else
			return null;
	}

	static String encodeChunkIdx(final byte[] chunkidx) {
		if (chunkidx != null)
			return Base64.encodeBytes(chunkidx);
		else
			return null;
	}

	static byte[] decodeChunkIdx(final String sChunkIdx) {
		if (sChunkIdx != null)
			return Base64.decode(sChunkIdx);
		else
			return null;
	}

	static String encodeChunkMeta(final ChunkMeta chunkMeta) {
		if (chunkMeta != null)
			return Base64.encodeBytes(chunkMeta.toBytes());
		else
			return null;
	}

	static byte[] decodeChunkMeta(final String sChunkMeta) {
		if (sChunkMeta != null)
			return Base64.decode(sChunkMeta);
		else
			return null;
	}

	static String encodeObjIdxKey(final ObjectIndexKey objIdxKey) {
		if (objIdxKey != null)
			return Base64.encodeBytes(objIdxKey.toBytes());
		else
			return null;
	}

	static String encodeObjInfo(final ObjectInfo objInfo) {
		if (objInfo != null)
			return Base64.encodeBytes(objInfo.toBytes());
		else
			return null;
	}

	static byte[] decodeObjInfo(final String sObjInfo) {
		if (sObjInfo != null)
			return Base64.decode(sObjInfo);
		else
			return null;
	}
}
