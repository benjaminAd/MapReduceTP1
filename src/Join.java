import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Join {
	private static final String INPUT_PATH = "input-join";
	private static final String OUTPUT_PATH = "output/join-";
	private static final Logger LOG = Logger.getLogger(Join.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			String[] Champs = value.toString().split("\\|");
			if (fileName.equals("customers.tbl")) {
				String custKey = Champs[0];
				String name = "cust," + Champs[1];
				context.write(new Text(custKey), new Text(name));
			}
			if (fileName.equals("orders.tbl")) {
				String custKey = Champs[1];
				String price = "Price," + Champs[3];
				context.write(new Text(custKey), new Text(price));
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String[]> Val = new ArrayList<>();
			for (Text t : values) {
				Val.add(t.toString().split(","));
			}
//			for (String[] c1 : Val) {
//				if (c1[0].equals("cust")) {
//					double TotalSum = 0.0;
//					for (String[] c2 : Val) {
//						if (c2[0].equals("Price")) {
//							double price = Double.parseDouble(c2[1].toString());
//							TotalSum += price;
//							// context.write(new Text(c1[1]), new Text(c2[1]));
//						}
//						context.write(new Text(c1[1]), new DoubleWritable(TotalSum));
//					}
//				}
//			}
			String name = "";
			double TotalSum = 0.0;
			for (String[] c1 : Val) {
				if (c1[0].equals("cust"))
					name = c1[1];
				else if (c1[0].equals("Price"))
					TotalSum += Double.parseDouble(c1[1]);
			}
//			double TotalSum = 0.0;
//			for (String[] c2 : Val) {
//				if (c2[0].equals("Price"))
//					TotalSum += Double.parseDouble(c2[1]);
//			}
			if (!name.equals(""))
				context.write(new Text(name), new DoubleWritable(TotalSum));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		Job job = new Job(conf, "Join");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}

}
