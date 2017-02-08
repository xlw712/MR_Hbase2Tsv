package com.run.mapred.hbase2tsv;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import S003.RWA_BASIC_Z002_1111_;
import S003.RWA_BASIC_Z002_2222_;
import S003.RWA_BASIC_Z002_3333_;
import S003.RWA_BASIC_Z002_4444_;

public class Hbase2UsvTest {
	MapDriver<NullWritable, KeyValue, Text, Text> mapDriver;
	Mapper<NullWritable, KeyValue, Text, Text> mapper;
	public static final String TABLE_NAME = "tableName";
	public static String FILE_PATH = "F:\\4c8bdac6e9864a1984968a908d7176e6";
	public static Configuration cfg = new Configuration();
	@Before
	public void SetUp() {
		mapper = new Hbase2UsvTest.Hbase2UsvMap();
		mapDriver = MapDriver.newMapDriver(mapper);
		Configuration conf = mapDriver.getConfiguration();
		conf.setStrings("io.serializations", conf.get("io.serializations"), KeyValueSerialization.class.getName());
		conf.setStrings(TABLE_NAME, "qqginfo");
	}
	@Test
	public void testMap()
			throws IOException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		List<Pair<NullWritable, KeyValue>> inputs = readScan();
		mapDriver.withAll(inputs).run(true);
	}
	public static class Hbase2UsvMap extends Mapper<NullWritable, KeyValue, Text, Text> {
		private static Logger logger = LoggerFactory.getLogger(Hbase2UsvMap.class);
		private SimpleDateFormat simpleDateFormat;
		public Hbase2UsvMap() {
		}
		@Override
		protected void setup(Mapper<NullWritable, KeyValue, Text, Text>.Context context) throws IOException, InterruptedException {
			simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		@Override
		protected void map(NullWritable key, KeyValue value, Mapper<NullWritable, KeyValue, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (Bytes.toString(value.getQualifier()).equals("pbc")) {
				StringBuffer sbf = new StringBuffer();
				switch (context.getConfiguration().get(TABLE_NAME)) {
					case "qqfriend" :
						// RB040002,RB060002,RB060003,RB060006
						RWA_BASIC_Z002_1111_.RWA_BASIC_Z002_1111 qqfriend = RWA_BASIC_Z002_1111_.RWA_BASIC_Z002_1111.parseFrom(value.getValue());
						sbf.append(qqfriend.getRB040002()).append((char) 1);
						sbf.append(qqfriend.getRB060002()).append((char) 1);
						sbf.append(qqfriend.getRB060003()).append((char) 1);
						sbf.append(qqfriend.getRB060006()).append((char) 1);
						context.write(new Text(), new Text(sbf.toString()));
						break;
					case "qqginfo" :
						logger.info(Bytes.toString(value.getValue()));
						// RD010010,RD020007,RD010007,RD010009,RD0100175
						RWA_BASIC_Z002_2222_.RWA_BASIC_Z002_2222 qqginfo = RWA_BASIC_Z002_2222_.RWA_BASIC_Z002_2222.parseFrom(value.getValue());
						sbf.append(qqginfo.getRD010010()).append((char) 1);
						sbf.append(qqginfo.getRD020007()).append((char) 1);
						sbf.append(simpleDateFormat.format(new Date(qqginfo.getRD010007()))).append((char) 1);
						sbf.append(qqginfo.getRD010009()).append((char) 1);
						sbf.append(qqginfo.getRD010017()).append((char) 1);
						// context.write(new Text(), new Text(sbf.toString()));
						break;
					case "qqgmember" :
						// RD010010,RB040002,RB060003
						RWA_BASIC_Z002_3333_.RWA_BASIC_Z002_3333 qqgmember = RWA_BASIC_Z002_3333_.RWA_BASIC_Z002_3333.parseFrom(value.getValue());
						sbf.append(qqgmember.getRD010010()).append((char) 1);
						sbf.append(qqgmember.getRB040002()).append((char) 1);
						sbf.append(qqgmember.getRB060003()).append((char) 1);
						context.write(new Text(), new Text(sbf.toString()));
						break;
					case "qqcontact" :
						// RB040002,RB060002,RB010001,RZ002517,RB030101,RB010013,RB010014,RB010011
						RWA_BASIC_Z002_4444_.RWA_BASIC_Z002_4444 qqcontact = RWA_BASIC_Z002_4444_.RWA_BASIC_Z002_4444.parseFrom(value.getValue());
						sbf.append(qqcontact.getRB040002()).append((char) 1);
						sbf.append(qqcontact.getRB060002()).append((char) 1);
						sbf.append(qqcontact.getRB010001()).append((char) 1);
						sbf.append(qqcontact.getRZ002517()).append((char) 1);
						sbf.append(qqcontact.getRB030101()).append((char) 1);
						sbf.append(qqcontact.getRB010013()).append((char) 1);
						sbf.append(qqcontact.getRB010014()).append((char) 1);
						sbf.append(qqcontact.getRB010011()).append((char) 1);
						context.write(new Text(), new Text(sbf.toString()));
						break;
				}
			}
		}
	}
	/** 
	 * 使用Scanner读取数据块内容 
	 */
	@SuppressWarnings("unchecked")
	public static List<Pair<NullWritable, KeyValue>> readScan()
			throws IOException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		List<Pair<NullWritable, KeyValue>> list = new ArrayList<Pair<NullWritable, KeyValue>>();
		// 创建读取路径，本地文件系统，两个读取流
		Path path = new Path(FILE_PATH);
		FileSystem fs = FileSystem.getLocal(cfg);
		CacheConfig config = new CacheConfig(cfg);
		FSDataInputStream fsdis = fs.open(path);
		FSDataInputStreamWrapper fsdisw = new FSDataInputStreamWrapper(fsdis);
		FSDataInputStream fsdisNoFsChecksum = fsdis;
		HFileSystem hfs = new HFileSystem(fs);
		long size = fs.getFileStatus(path).getLen();
		// 由读FS读取流，文件长度，就可以读取到尾文件块
		FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, size);
		HFileReaderV3 v3 = new HFileReaderV3(path, trailer, fsdisw, size, config, hfs, cfg);
		// 读取FileInfo中的内容
		Method method = v3.getClass().getMethod("loadFileInfo", new Class[]{});
		Map<byte[], byte[]> fileInfo = (Map<byte[], byte[]>) method.invoke(v3, new Object[]{});
		Iterator<Entry<byte[], byte[]>> iter = fileInfo.entrySet().iterator();
		HFileScanner scanner = v3.getScanner(false, false);
		scanner.seekTo();
		list.add(new Pair<NullWritable, KeyValue>(NullWritable.get(), (KeyValue) scanner.getKeyValue()));
		while (scanner.next()) {
			list.add(new Pair<NullWritable, KeyValue>(NullWritable.get(), (KeyValue) scanner.getKeyValue()));
		}
		v3.close();
		return list;
	}
}
