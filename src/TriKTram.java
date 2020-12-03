import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class MapB extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	}
}

// =========================================================================
// REDUCER
// =========================================================================

class ReduceB extends Reducer<DoubleWritable, Text, Text, Text> {
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

public class TriKTram {
	private static final String INPUT_PATH = "output/Profits-1606939167";
	private static final String OUTPUT_PATH = "output/TopkWordCountGroupBy-";
	private static final Logger LOG = Logger.getLogger(TriKTram.class.getName());

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

	public static class MapA extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] Values = value.toString().split(";");
			if (Values[0].equals("course"))
				return;
//			String NumLigne = Values[4];
//			String[] time = Values[Values.length - 2].split(":");
//			String station = Values[3];
//			if (station.equals("OCCITANIE"))
//				context.write(new Text(NumLigne + "," + time[0]), new IntWritable(1));

//			Pour chaque station, donner le nombre de trams et bus par jour.
//			String station = Values[3];
//			context.write(new Text(station), new IntWritable(1));

//			Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, avec X_tram="faible" si au plus 8 trams sont pr�vus (noter qu'une ligne de circulation a deux sens, donc au plus 4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont pr�vus, et X="fort" pour toute autre valeur. Afficher la m�me information pour les bus. Pour les stations o� il a seulement des trams (ou des bus) il faut afficher une seule information	
			String station = Values[3];
			String trajet = Values[3] + "," + Values[5];
			String[] time = Values[Values.length - 2].split(":");
			context.write(new Text(station + "," + time[0]), new IntWritable(1));
		}

	}

	public static class ReduceA extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
//			String[] dataKeys = key.toString().split(",");
//			String ligne = "Ligne " + dataKeys[0];
//			String time = dataKeys[1] + "h";
//			int acc = 0;
//			for (IntWritable i : values) {
//				acc += i.get();
//			}
//			context.write(new Text(ligne), new Text(time + ", " + acc));

// 			Pour chaque station, donner le nombre de trams et bus par jour.
//			int acc = 0;
//			for (IntWritable i : values) {
//				acc += i.get();
//			}
//			context.write(key, new Text("" + acc));

//			Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, avec X_tram="faible" si au plus 8 trams sont pr�vus (noter qu'une ligne de circulation a deux sens, donc au plus 4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont pr�vus, et X="fort" pour toute autre valeur. Afficher la m�me information pour les bus. Pour les stations o� il a seulement des trams (ou des bus) il faut afficher une seule information
			int acc = 0;
			for (IntWritable t : values) {
				acc += t.get();
			}

			String X_tram = "";
			if (acc <= 8)
				X_tram = "faible";
			else if ((acc >= 9) && (acc <= 18))
				X_tram = "moyen";
			else
				X_tram = "fort";
			String[] datas = key.toString().split(",");
			String station = datas[0];
			String time = datas[1];
			context.write(new Text(station + "," + time + "," + X_tram), new Text());
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
		Configuration confA = new Configuration();
		confA.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		Job jobA = new Job(confA, "RecupTram");

		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);

		jobA.setMapperClass(MapA.class);
		jobA.setReducerClass(ReduceA.class);

		jobA.setInputFormatClass(TextInputFormat.class);
		jobA.setOutputFormatClass(TextOutputFormat.class);

		long instant = Instant.now().getEpochSecond();
		FileInputFormat.addInputPath(jobA, new Path("TAM_MMM_OffreJour"));
		FileOutputFormat.setOutputPath(jobA, new Path("output/TamLastExo-" + instant));

		jobA.waitForCompletion(true);

		Configuration conf = new Configuration();
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		conf.setInt("k", k);

		Job job = new Job(conf, "Tritram");

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapB.class);
		job.setReducerClass(ReduceB.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + instant));

		job.waitForCompletion(true);
	}
}
