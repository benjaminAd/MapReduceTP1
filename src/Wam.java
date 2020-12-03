import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.sun.jdi.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.swing.text.html.parser.Parser;
class InvComparator<T extends WritableComparable> extends WritableComparator {

    public InvComparator(Class<T> parameterClass) {
        super(parameterClass, true);
    }

    /**
     * Cette fonction définit l'ordre de comparaison entre 2 objets de type T.
     * Dans notre cas nous voulons simplement inverser la valeur de retour de la méthode T.compareTo.
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
class TextInvComparator extends InvComparator<IntWritable> {

    public TextInvComparator() {
        super(IntWritable.class);
    }
}

public class Wam {
    private static final String INPUT_PATH = "TAM_MMM_OffreJour/";
    private static final String OUTPUT_PATH = "output/TAM-count";
    private static final Logger LOG = Logger.getLogger(TAM.class.getName());

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
            String NumLigne = Values[4];
            String station = Values[3];


//            if (Integer.parseInt(String.valueOf(NumLigne)) < 5) 
//            if (Integer.parseInt(String.valueOf(NumLigne)) > 4) 
            //les 10 stastions pour les bus et les tram
                context.write( new Text(station) , new IntWritable(1));

//			Pour chaque station, donner le nombre de trams et bus par jour.
//			String station = Values[3];
//			context.write(new Text(station), new IntWritable(1));

//			Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, avec X_tram="faible" si au plus 8 trams sont pr�vus (noter qu'une ligne de circulation a deux sens, donc au plus 4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont pr�vus, et X="fort" pour toute autre valeur. Afficher la m�me information pour les bus. Pour les stations o� il a seulement des trams (ou des bus) il faut afficher une seule information	
//			String station = Values[3];
//			String trajet = Values[3] + "," + Values[5];
//			String[] time = Values[Values.length - 2].split(":");
//			context.write(new Text(station + "," + time[0]), new IntWritable(1));
        }

    }

    public static class ReduceA extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {

                count += i.get();
            }

            context.write(new Text(key.toString() + ","), new IntWritable(count));
        }
    }

        public static class MapB extends Mapper<LongWritable, Text, IntWritable, Text> {
            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] Values = value.toString().split(",");

                String station = Values[0];
                String nbLigne =Values[1];
              int num = Integer.parseInt(nbLigne.replaceAll("\\s", ""));


                    context.write( new IntWritable(num),new Text(station) );

//			Pour chaque station, donner le nombre de trams et bus par jour.
//			String station = Values[3];
//			context.write(new Text(station), new IntWritable(1));

//			Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, avec X_tram="faible" si au plus 8 trams sont pr�vus (noter qu'une ligne de circulation a deux sens, donc au plus 4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont pr�vus, et X="fort" pour toute autre valeur. Afficher la m�me information pour les bus. Pour les stations o� il a seulement des trams (ou des bus) il faut afficher une seule information
//			String station = Values[3];
//			String trajet = Values[3] + "," + Values[5];
//			String[] time = Values[Values.length - 2].split(":");
//			context.write(new Text(station + "," + time[0]), new IntWritable(1));
            }

        }

        public static class ReduceB extends Reducer<IntWritable, Text, Text, IntWritable> {
            int i = 0;
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)

                    throws IOException, InterruptedException {
                Map<Text , IntWritable> map = new HashMap<>();


String s = "";
     for (Text t :values) {

         if (i<10)
         {
             context.write(new Text(t) , key);
             i++;
         }


     }



            }


// 			Pour chaque station, donner le nombre de trams et bus par jour.
//			int acc = 0;
//			for (IntWritable i : values) {
//				acc += i.get();
//			}
//			context.write(key, new Text("" + acc));

//			Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, avec X_tram="faible" si au plus 8 trams sont pr�vus (noter qu'une ligne de circulation a deux sens, donc au plus 4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont pr�vus, et X="fort" pour toute autre valeur. Afficher la m�me information pour les bus. Pour les stations o� il a seulement des trams (ou des bus) il faut afficher une seule information
//			int acc = 0;
//			for (IntWritable t : values) {
//				acc += t.get();
//			}
//
//			String X_tram = "";
//			if (acc <= 8)
//				X_tram = "faible";
//			else if ((acc >= 9) && (acc <= 18))
//				X_tram = "moyen";
//			else
//				X_tram = "fort";
//			String[] datas = key.toString().split(",");
//			String station = datas[0];
//			String time = datas[1];
//			context.write(new Text(station + " " + time), new Text(X_tram));
    }


    public static void main(String[] args) throws Exception {
        Configuration confA = new Configuration();
        confA.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
        Job jobA = new Job(confA, "wam");
        //job.setSortComparatorClass(TextInvComparator.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(MapA.class);
        jobA.setReducerClass(ReduceA.class);

        jobA.setInputFormatClass(TextInputFormat.class);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        long instant = Instant.now().getEpochSecond();
        FileInputFormat.addInputPath(jobA, new Path(INPUT_PATH));

        FileOutputFormat.setOutputPath(jobA, new Path(OUTPUT_PATH + instant));

        jobA.waitForCompletion(true);

        Configuration confB = new Configuration();
        confB.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
        Job jobB = new Job(confB, "wamB");
        jobB.setSortComparatorClass(TextInvComparator.class);
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapperClass(MapB.class);
        jobB.setReducerClass(ReduceB.class);

        jobB.setInputFormatClass(TextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobB, new Path("output/TAM-count" + instant));
        FileOutputFormat.setOutputPath(jobB, new Path("output/Tam-countTri" + instant));

        jobB.waitForCompletion(true);
    }

}