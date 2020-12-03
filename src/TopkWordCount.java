
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini Map et Reduce en dehors de notre classe principale.
 * Il se pose alors le problème du passage du paramètre 'k' dans notre reducer, car il n'est en effet plus possible de déclarer un paramètre k dans notre classe principale qui serait partagé avec ses classes internes ; c'est la que la Configuration du Job entre en jeu.
 */

// =========================================================================
// MAPPER
// =========================================================================

class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String[] data = line.split(",");

		if (Arrays.equals(data, emptyWords))
			return;
//		System.out.println(data.length);
		context.write(new DoubleWritable(Double.parseDouble(data[2])), new Text(data[0] + "/" + data[1]));
		/*
		 * for (String word : words) context.write(new Text(word), one);
		 */
	}
}

// =========================================================================
// REDUCER
// =========================================================================

class Reduce extends Reducer<DoubleWritable, Text, Text, Text> {
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la
	 * fréquence d'un ou plusieurs mots). Utilisé pour conserver les k mots les
	 * plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	// private TreeMap<Text, List<Text>> sortedWords = new TreeMap<>();
	private TreeMap<DoubleWritable, Text> LigneByProfits = new TreeMap<>();
	// private int nbsortedWords = 0;
	private int nbLigne = 0;
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * int sum = 0;
		 * 
		 * for (IntWritable val : values) sum += val.get();
		 * 
		 * // On copie car l'objet key reste le même entre chaque appel du reducer Text
		 * keyCopy = new Text(key);
		 * 
		 * // Fréquence déjà présente if (sortedWords.containsKey(sum))
		 * sortedWords.get(sum).add(keyCopy); else { List<Text> words = new
		 * ArrayList<>(); words.add(keyCopy); sortedWords.put(sum, words); }
		 * 
		 * // Nombre de mots enregistrés atteint : on supprime le mot le moins
		 * fréquent // (le premier dans sortedWords) if (nbsortedWords == k) { Integer
		 * firstKey = sortedWords.firstKey(); List<Text> words =
		 * sortedWords.get(firstKey); words.remove(words.size() - 1); if
		 * (words.isEmpty()) sortedWords.remove(firstKey); } else nbsortedWords++;
		 */
		String res = "";
		for (Text t : values) {

			res += t.toString();
		}
		if (nbLigne < k) {
//			System.out.println("key = " + key.toString() + " res = " + res);
			LigneByProfits.put(new DoubleWritable(key.get()), new Text(res));
			nbLigne += 1;
//			System.out.println(nbLigne);
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
//		Set<DoubleWritable> SetDouble = LigneByProfits.keySet();
//		System.out.println("SetDouble = " + SetDouble.size());
		System.out.println("TreeMap = " + LigneByProfits);
		List<Double> Profits = new ArrayList<>();
		for (Entry<DoubleWritable, Text> entry : LigneByProfits.entrySet()) {
			Profits.add(entry.getKey().get());
		}
//		int l = 0;
//		for (DoubleWritable t : SetDouble) {
//			Profits[l] = t.get();
//			l++;
//		}
		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = Profits.size();
		System.out.println("i = " + i);
		while (i-- != 0) {
			double profit = Profits.get(i);
			Text t = LigneByProfits.get(new DoubleWritable(profit));
			String[] data = t.toString().split(",");
			int j = 0;
			for (String d : data) {
				String[] va = d.split("/");
				context.write(new Text(va[0]), new Text(va[1]));
			}
			// context.write(, new IntWritable(nbof));
		}
	}
}

public class TopkWordCount {
	private static final String INPUT_PATH = "output/Profits-1606939167";
	private static final String OUTPUT_PATH = "output/TopkWordCountGroupBy-";
	private static final Logger LOG = Logger.getLogger(TopkWordCount.class.getName());

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

	/**
	 * Ce programme permet le passage d'une valeur k en argument de la ligne de
	 * commande.
	 */
	public static void main(String[] args) throws Exception {
		// Borne 'k' du topk
		int k = 10;

		try {
			// Passage du k en argument ?
			if (args.length > 0) {
				k = Integer.parseInt(args[0]);

				// On contraint k à valoir au moins 1
				if (k <= 0) {
					LOG.warning("k must be at least 1, " + k + " given");
					k = 1;
				}
			}
		} catch (NumberFormatException e) {
			LOG.severe("Error for the k argument: " + e.getMessage());
			System.exit(1);
		}

		Configuration conf = new Configuration();
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		conf.setInt("k", k);

		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}