/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc;

import java.util.concurrent.ExecutorService;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.spi.util.AbstractWriteBuffer;

public class JdbcWriteBuffer extends AbstractWriteBuffer {
	JdbcWriteBuffer(ExecutorService executor, int bufferSize) {
		super(executor, bufferSize);
	}

	@Override
	protected void startQueuedOperations(int bufferedByteCount)
			throws DhtException {
	}
}
