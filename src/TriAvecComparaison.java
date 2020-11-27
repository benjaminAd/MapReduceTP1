
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// =========================================================================
// COMPARATEURS
// =========================================================================

/**
 * Comparateur qui inverse la méthode de comparaison d'un sous-type T de
 * WritableComparable (ie. une clé).
 */
@SuppressWarnings("rawtypes")
class InverseComparator<T extends WritableComparable> extends WritableComparator {

	public InverseComparator(Class<T> parameterClass) {
		super(parameterClass, true);
	}

	/**
	 * Cette fonction définit l'ordre de comparaison entre 2 objets de type T. Dans
	 * notre cas nous voulons simplement inverser la valeur de retour de la méthode
	 * T.compareTo.
	 * 
	 * @return 0 si a = b <br>
	 *         x > 0 si a > b <br>
	 *         x < 0 sinon
	 */
	@SuppressWarnings("unchecked")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// On inverse le retour de la méthode de comparaison du type
		return -a.compareTo(b);

	}
}

/**
 * Inverseur de la comparaison du type Text.
 */
class TextInverseComparator extends InverseComparator<Text> {

	public TextInverseComparator() {
		super(Text.class);
	}
}

// =========================================================================
// CLASSE MAIN
// =========================================================================

public class TriAvecComparaison {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/9-TriAvecComparaison-";
	private static final Logger LOG = Logger.getLogger(TriAvecComparaison.class.getName());

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

	// =========================================================================
	// MAPPER
	// =========================================================================

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] ValuesArray = line.split(",");
			if (ValuesArray[0].equals("Row ID"))
				return;
			String Key = "";
			String Values = "";
			for (int i = 0; i < ValuesArray.length; i++) {
				if (i == 3)
					Key = ValuesArray[i];
				else {
					if (i == 0)
						Values += ValuesArray[i];
					else
						Values += ValuesArray[i];
				}
			}
			String[] KeyArray = Key.split("/");
			//System.out.println(Key);
			Key = KeyArray[2] + "/" + KeyArray[0] + "/" + KeyArray[1];
			context.write(new Text(Key), new Text(Values));
		}
	}

	// =========================================================================
	// REDUCER
	// =========================================================================

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
			String s = "";
			for (Text t : values) {
				s += t.toString();
			}
//			String[] Date = key.toString().split("/");
//			String c = "";
//			for (int i = Date.length - 1; i >= 0; i--) {
//				if (i == 0)
//					c += Date[i];
//				else
//					c += Date[i] + "/";
//			}
			context.write(key, new Text(s));
		}
	}

	// =========================================================================
	// MAIN
	// =========================================================================

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		Job job = new Job(conf, "9-Sort");

		/*
		 * Affectation de la classe du comparateur au job. Celui-ci sera appelé durant
		 * la phase de shuffle.
		 */
		job.setSortComparatorClass(TextInverseComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}