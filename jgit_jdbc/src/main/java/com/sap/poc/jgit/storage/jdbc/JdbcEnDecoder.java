/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.io.UnsupportedEncodingException;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.RowKey;

class JdbcEnDecoder {
	static String encodeRowKey(final RowKey rowKey)
			throws UnsupportedEncodingException {
		if (rowKey != null)
			return new String(rowKey.toBytes(), "US-ASCII");
		else
			return null;
	}

	static byte[] decodeRowKey(final String sRowKey) {
		if (sRowKey != null)
			return Constants.encodeASCII(sRowKey);
		else
			return null;
	}

	static String encodeShortChunkKey(final ChunkKey chunkKey)
			throws UnsupportedEncodingException {
		if (chunkKey != null)
			return new String(chunkKey.toShortBytes(), "US-ASCII");
		else
			return null;
	}
}
