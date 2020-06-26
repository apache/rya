package org.apache.rya.accumulo.mr;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.spark.graphx.Edge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map.Entry;

/**
 * Subclass of {@link AbstractInputFormat} for reading
 * {@link RyaStatementWritable}s directly from a running Rya instance.
 */
@SuppressWarnings("rawtypes")
public class GraphXEdgeInputFormat extends InputFormatBase<Object, Edge> {
	/**
	 * Instantiates a RecordReader for this InputFormat and a given task and
	 * input split.
	 *
	 * @param split
	 *            Defines the portion of the input this RecordReader is
	 *            responsible for.
	 * @param context
	 *            The context of the task.
	 * @return A RecordReader that can be used to fetch RyaStatementWritables.
	 */
	@Override
	public RecordReader<Object, Edge> createRecordReader(final InputSplit split,
			final TaskAttemptContext context) {
		return new RyaStatementRecordReader();
	}

	/**
	 * Sets the table layout to use.
	 *
	 * @param conf
	 *            Configuration to set the layout in.
	 * @param layout
	 *            Statements will be read from the Rya table associated with
	 *            this layout.
	 */
	public static void setTableLayout(final Job conf, final TABLE_LAYOUT layout) {
		conf.getConfiguration().set(MRUtils.TABLE_LAYOUT_PROP, layout.name());
	}

	/**
	 * Retrieves RyaStatementWritable objects from Accumulo tables.
	 */
	public class RyaStatementRecordReader extends
			AbstractRecordReader<Object, Edge> {
		private RyaTripleContext ryaContext;
		private TABLE_LAYOUT tableLayout;

		protected void setupIterators(final TaskAttemptContext context,
				final Scanner scanner, final String tableName, final RangeInputSplit split) {
		}

		/**
		 * Initializes the RecordReader.
		 *
		 * @param inSplit
		 *            Defines the portion of data to read.
		 * @param attempt
		 *            Context for this task attempt.
		 * @throws IOException
		 *             if thrown by the superclass's initialize method.
		 */
		@Override
		public void initialize(final InputSplit inSplit, final TaskAttemptContext attempt)
				throws IOException {
			super.initialize(inSplit, attempt);
			this.tableLayout = MRUtils.getTableLayout(
					attempt.getConfiguration(), TABLE_LAYOUT.SPO);
			// TODO verify that this is correct
			this.ryaContext = RyaTripleContext
					.getInstance(new AccumuloRdfConfiguration(attempt
							.getConfiguration()));
		}

		/**
		 * Load the next statement by converting the next Accumulo row to a
		 * statement, and make the new (key,value) pair available for retrieval.
		 *
		 * @return true if another (key,value) pair was fetched and is ready to
		 *         be retrieved, false if there was none.
		 * @throws IOException
		 *             if a row was loaded but could not be converted to a
		 *             statement.
		 */
		@Override
		public boolean nextKeyValue() throws IOException {
			if (!scannerIterator.hasNext()) {
                return false;
            }
			final Entry<Key, Value> entry = scannerIterator.next();
			++numKeysRead;
			currentKey = entry.getKey();
			try {
				currentK = currentKey.getRow();
				final RyaTypeWritable rtw = new RyaTypeWritable();
				final RyaStatement stmt = this.ryaContext.deserializeTriple(
						this.tableLayout, new TripleRow(entry.getKey().getRow()
								.getBytes(), entry.getKey().getColumnFamily()
								.getBytes(), entry.getKey()
								.getColumnQualifier().getBytes(), entry
								.getKey().getTimestamp(), entry.getKey()
								.getColumnVisibility().getBytes(), entry
								.getValue().get()));

				final long subHash = getVertexId(stmt.getSubject());
				final long objHash = getVertexId(stmt.getObject());
				rtw.setRyaType(stmt.getPredicate());

				final Edge<RyaTypeWritable> writable = new Edge<RyaTypeWritable>(
						subHash, objHash, rtw);
				currentV = writable;
			} catch (final TripleRowResolverException e) {
				throw new IOException(e);
			}
			return true;
		}

		protected List<IteratorSetting> contextIterators(
				final TaskAttemptContext context, final String tableName) {
			return getIterators(context);
		}

		@Override
		protected void setupIterators(final TaskAttemptContext context,
				final Scanner scanner, final String tableName,
				final org.apache.accumulo.core.client.mapreduce.RangeInputSplit split) {
			List<IteratorSetting> iterators = null;

			if (null == split) {
				iterators = contextIterators(context, tableName);
			} else {
				iterators = split.getIterators();
				if (null == iterators) {
					iterators = contextIterators(context, tableName);
				}
			}

			for (final IteratorSetting iterator : iterators) {
                scanner.addScanIterator(iterator);
            }
		}

	}

	public static long getVertexId(final RyaValue resource) throws IOException {
		String iri = "";
		if (resource != null) {
			iri = resource.getData();
		}
		try {
			// SHA-256 the string value and then generate a hashcode from
			// the digested string, the collision ratio is less than 0.0001%
			// using custom hash function should significantly reduce the
			// collision ratio
			final MessageDigest messageDigest = MessageDigest
					.getInstance("SHA-256");
			messageDigest.update(iri.getBytes(StandardCharsets.UTF_8));
			final String encryptedString = new String(messageDigest.digest(), StandardCharsets.UTF_8);
			return hash(encryptedString);
		}
		catch (final NoSuchAlgorithmException e) {
			throw new IOException(e);
		}
	}

	public static long hash(final String string) {
		long h = 1125899906842597L; // prime
		final int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}
}
