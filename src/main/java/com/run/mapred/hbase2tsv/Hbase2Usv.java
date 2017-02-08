package com.run.mapred.hbase2tsv;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.run.mapred.hbase2tsv.constant.MapReduceConstant;

import S003.RWA_BASIC_Z002_1111_;
import S003.RWA_BASIC_Z002_2222_;
import S003.RWA_BASIC_Z002_3333_;
import S003.RWA_BASIC_Z002_4444_;

public class Hbase2Usv implements MapReduceConstant {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Hbase2Usv <in> <out> <tableName>");
			System.exit(1);
		}
		switch (otherArgs[2]) {
			case "qqfriend" :
				conf.set(TABLE_NAME, otherArgs[2]);
				break;
			case "qqginfo" :
				conf.set(TABLE_NAME, otherArgs[2]);
				break;
			case "qqgmember" :
				conf.set(TABLE_NAME, otherArgs[2]);
				break;
			case "qqcontact" :
				conf.set(TABLE_NAME, otherArgs[2]);
				break;
			default :
				System.err.println("args[2] parmas is must qqfriend|qqginfo|qqgmember|qqcontact}");
				System.exit(1);
				break;
		}
		Job job = new Job(conf, "Hbase2Usv_" + otherArgs[2]);
		job.setJarByClass(Hbase2Usv.class);
		job.setMapperClass(Hbase2UsvMap.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(HFileInputFormat_mr1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		HFileInputFormat_mr1.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setCompressOutput(job, false);
		// TextOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class Hbase2UsvMap extends Mapper<NullWritable, KeyValue, NullWritable, Text> {
		private MultipleOutputs outputs;
		private SimpleDateFormat sdf;
		@Override
		protected void setup(Mapper<NullWritable, KeyValue, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			outputs = new MultipleOutputs(context);
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		public Hbase2UsvMap() {
		}
		@Override
		protected void map(NullWritable key, KeyValue value, Mapper<NullWritable, KeyValue, NullWritable, Text>.Context context)
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
						sbf.append(qqfriend.getRB060006());
						outputs.write(NullWritable.get(), new Text(sbf.toString()), "qqfriend");
						break;
					case "qqginfo" :
						// RD010010,RD020007,RD010007,RD010009,RD010017
						RWA_BASIC_Z002_2222_.RWA_BASIC_Z002_2222 qqginfo = RWA_BASIC_Z002_2222_.RWA_BASIC_Z002_2222.parseFrom(value.getValue());
						sbf.append(qqginfo.getRD010010()).append((char) 1);
						sbf.append(qqginfo.getRD020007()).append((char) 1);
						sbf.append(sdf.format(new Date(qqginfo.getRD010007()))).append((char) 1);
						sbf.append(qqginfo.getRD010009()).append((char) 1);
						sbf.append(qqginfo.getRD010017());
						outputs.write(NullWritable.get(), new Text(sbf.toString()), "qqginfo");
						break;
					case "qqgmember" :
						// RD010010,RB040002,RB060003
						RWA_BASIC_Z002_3333_.RWA_BASIC_Z002_3333 qqgmember = RWA_BASIC_Z002_3333_.RWA_BASIC_Z002_3333.parseFrom(value.getValue());
						sbf.append(qqgmember.getRD010010()).append((char) 1);
						sbf.append(qqgmember.getRB040002()).append((char) 1);
						sbf.append(qqgmember.getRB060003());
						outputs.write(NullWritable.get(), new Text(sbf.toString()), "qqgmember");
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
						sbf.append(qqcontact.getRB010011());
						outputs.write(NullWritable.get(), new Text(sbf.toString()), "qqcontact");
						break;
				}
			}
		}
	}
}
