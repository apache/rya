package mvm.rya.accumulo.mr;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.indexing.accumulo.entity.EntityCentricIndex;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class GraphXInputFormat extends InputFormatBase<Object, RyaTypeWritable> {

	private static final int WHOLE_ROW_ITERATOR_PRIORITY = 23;

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
	public RecordReader<Object, RyaTypeWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new RyaStatementRecordReader();
	}

	
	
	/**
	 * Retrieves RyaStatementWritable objects from Accumulo tables.
	 */
	public class RyaStatementRecordReader extends
			AbstractRecordReader<Object, RyaTypeWritable> {
		protected void setupIterators(TaskAttemptContext context,
				Scanner scanner, String tableName,
				@SuppressWarnings("deprecation") RangeInputSplit split) {
			IteratorSetting iteratorSetting = new IteratorSetting(
					WHOLE_ROW_ITERATOR_PRIORITY, WholeRowIterator.class);
			scanner.addScanIterator(iteratorSetting);
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
		public void initialize(InputSplit inSplit, TaskAttemptContext attempt)
				throws IOException {
			super.initialize(inSplit, attempt);
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
			if (!scannerIterator.hasNext())
				return false;
			Entry<Key, Value> entry = scannerIterator.next();
			++numKeysRead;
			currentKey = entry.getKey();

			try {
				currentK = currentKey.getRow();
				SortedMap<Key, Value> wholeRow = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
				Key key = wholeRow.firstKey();
				Value value = wholeRow.get(key);
				RyaType type = EntityCentricIndex.getRyaType(key, value);
				RyaTypeWritable writable = new RyaTypeWritable();
				writable.setRyaType(type);
				currentV = writable;
			} catch (RyaTypeResolverException e) {
				throw new IOException();
			}
			return true;
		}

		protected List<IteratorSetting> contextIterators(
				TaskAttemptContext context, String tableName) {
			return getIterators(context);
		}

		@Override
		protected void setupIterators(TaskAttemptContext context,
				Scanner scanner, String tableName,
				org.apache.accumulo.core.client.mapreduce.RangeInputSplit split) {

			List<IteratorSetting> iterators = null;

			if (null == split) {
				iterators = contextIterators(context, tableName);
			} else {
				iterators = split.getIterators();
				if (null == iterators) {
					iterators = contextIterators(context, tableName);
				}
			}

			for (IteratorSetting iterator : iterators)
				scanner.addScanIterator(iterator);
		}
	}
}
