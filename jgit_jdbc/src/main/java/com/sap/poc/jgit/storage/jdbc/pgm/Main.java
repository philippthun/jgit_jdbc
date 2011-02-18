/*
 * Copyright (C) 2011, Philipp Thun <philipp.thun@sap.com>
 */
package com.sap.poc.jgit.storage.jdbc.pgm;

import java.io.IOException;
import java.net.URISyntaxException;

import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.pgm.Die;

import com.sap.poc.jgit.storage.jdbc.JdbcRepositoryBuilder;

public class Main extends org.eclipse.jgit.pgm.Main {
	public static final String GIT_JDBC_PREFIX = "git+jdbc+";

	public static void main(final String[] argv) {
		new Main().run(argv);
	}

	@Override
	protected Repository openGitDir(final String gitdir) throws IOException {
		if (gitdir != null && gitdir.startsWith(GIT_JDBC_PREFIX)) {
			try {
				return new JdbcRepositoryBuilder().setURI(gitdir).build();
			} catch (URISyntaxException e) {
				throw new Die("Invalid URI " + gitdir);
			}
		}
		return super.openGitDir(gitdir);
	}
}
