/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.run.mapred.hbase2tsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Simple input format for HFiles.
 */
public class HFileInputFormat_mr2 extends FileInputFormat<NullWritable, KeyValue> {

	private static final Logger LOG = LoggerFactory.getLogger(HFileInputFormat_mr2.class);
	static final String START_ROW_KEY = "crunch.hbase.hfile.input.format.start.row";
	static final String STOP_ROW_KEY = "crunch.hbase.hfile.input.format.stop.row";

	/**
	 * File filter that removes all "hidden" files. This might be something worth removing from
	 * a more general purpose utility; it accounts for the presence of metadata files created
	 * in the way we're doing exports.
	 */
	static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};
	private static byte[] decodeHexOrDie(String s) {
		try {
			return Hex.decodeHex(s.toCharArray());
		} catch (DecoderException e) {
			throw new AssertionError("Failed to decode hex string: " + s);
		}
	}
	/**
	 * Record reader for HFiles.
	 */
	private static class HFileRecordReader implements RecordReader<NullWritable, KeyValue> {
		private Reader in;
		protected Configuration conf;
		/**
		 * A private cache of the key value so it doesn't need to be loaded twice from the scanner.
		 */
		private KeyValue value = null;
		private boolean reachedStopRow = false;
		private long count;
		private boolean seeked = false;
		private HFileScanner scanner;

		private byte[] startRow = null;
		private byte[] stopRow = null;
		public HFileRecordReader() {
			// TODO Auto-generated constructor stub
		}
		public HFileRecordReader(Reader in, HFileScanner scanner, byte[] startRow, byte[] stopRow) {
			this.scanner = scanner;
			this.in = in;
			this.startRow = startRow;
			this.stopRow = startRow;
		}

		@Override
		public boolean next(NullWritable key, KeyValue value) throws IOException {
			if (reachedStopRow) {
				return false;
			}
			boolean hasNext;
			if (!seeked) {
				if (startRow != null) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Seeking to start row {}", Bytes.toStringBinary(startRow));
					}
					KeyValue kv = KeyValue.createFirstOnRow(startRow);
					hasNext = seekAtOrAfter(scanner, kv);
				} else {
					LOG.info("Seeking to start");
					hasNext = scanner.seekTo();
				}
				seeked = true;
			} else {
				hasNext = scanner.next();
			}
			if (!hasNext) {
				return false;
			}
			value = KeyValue.cloneAndAddTags(scanner.getKeyValue(), ImmutableList.<Tag> of());
			if (stopRow != null && Bytes.compareTo(value.getRowArray(), value.getRowOffset(), value.getRowLength(), stopRow, 0, stopRow.length) >= 0) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Reached stop row {}", Bytes.toStringBinary(stopRow));
				}
				reachedStopRow = true;
				value = null;
				return false;
			}
			count++;
			return true;
		}
		// This method is copied from o.a.h.hbase.regionserver.StoreFileScanner,
		// as we don't want
		// to depend on it.
		private static boolean seekAtOrAfter(HFileScanner s, KeyValue k) throws IOException {
			int result = s.seekTo(k);
			if (result < 0) {
				// Passed KV is smaller than first KV in file, work from start
				// of file
				return s.seekTo();
			} else if (result > 0) {
				// Passed KV is larger than current KV in file, if there is a
				// next
				// it is the "after", if not then this scanner is done.
				return s.next();
			}
			// Seeked to the exact key
			return true;
		}
		@Override
		public NullWritable createKey() {
			return NullWritable.get();
		}

		@Override
		public KeyValue createValue() {
			return value;
		}

		@Override
		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float getProgress() {
			// This would be inaccurate if KVs are not uniformly-sized or we
			// have performed a seek to
			// the start row, but better than nothing anyway.
			return 1.0f * count / in.getEntries();
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
				in = null;
			}
		}
	}
	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();

		// Explode out directories that match the original FileInputFormat
		// filters since HFiles are written to directories where the
		// directory name is the column name
		for (FileStatus status : super.listStatus(job)) {
			if (status.isDirectory()) {
				FileSystem fs = status.getPath().getFileSystem(job);
				for (FileStatus match : fs.listStatus(status.getPath(), HIDDEN_FILE_FILTER)) {
					result.add(match);
				}
			} else {
				result.add(status);
			}
		}

		return (FileStatus[]) result.toArray();
	}
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
	@Override
	public RecordReader<NullWritable, KeyValue> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		FileSplit fileSplit = (FileSplit) split;
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(job);
		LOG.info("Initialize HFileRecordReader for {}", path);
		Reader in = HFile.createReader(fs, path, new CacheConfig(job), job);

		// The file info must be loaded before the scanner can be used.
		// This seems like a bug in HBase, but it's easily worked around.
		in.loadFileInfo();
		HFileScanner scanner = in.getScanner(false, false);

		String startRowStr = job.get(START_ROW_KEY);
		byte[] startRow = null;
		if (startRowStr != null) {
			startRow = decodeHexOrDie(startRowStr);
		}
		String stopRowStr = job.get(STOP_ROW_KEY);
		byte[] stopRow = null;
		if (stopRowStr != null) {
			stopRow = decodeHexOrDie(stopRowStr);
		}
		return new HFileRecordReader(in, scanner, startRow, stopRow);
	}
}