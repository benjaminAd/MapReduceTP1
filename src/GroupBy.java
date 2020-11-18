import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GroupBy {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupBy-";
    private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

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

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String ValueToString = value.toString();

            String[] ValuesArray = ValueToString.split(",");
            if (ValuesArray[0].equals("Row ID")) return;
			/* GroupBy Customer_Id
			String Customer_ID = ValuesArray[5];
			Double profit = Double.valueOf(ValuesArray[ValuesArray.length-1]);
			context.write(new Text(Customer_ID), new DoubleWritable(profit));
			*/

           /* GroupBy Order Date and State
           String DateAndState = ValuesArray[2] + " " + ValuesArray[10];
            Double profit = Double.valueOf(ValuesArray[ValuesArray.length - 1]);
            context.write(new Text(DateAndState), new DoubleWritable(profit));*/

           /* GroupBy Order Date and Category
           String DateAndCategory = ValuesArray[2] + " " + ValuesArray[14];
            double profit = Double.parseDouble(ValuesArray[ValuesArray.length - 1]);
            context.write(new Text(DateAndCategory), new DoubleWritable(profit));*/

            /* Nombre de produits distincts par commande
            String Order_ID = ValuesArray[1];
            context.write(new Text(Order_ID), new DoubleWritable((double) 1));*/

            //Nombre total d'exemplaire par commande
            String Order_ID = ValuesArray[1];
            double quantity = Double.parseDouble(ValuesArray[ValuesArray.length - 3]);
            context.write(new Text(Order_ID), new DoubleWritable(quantity));

        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
           /* Calcul de profit
           double totalProfit = (double) 0;
            for (DoubleWritable val : values) {
                if (val.get() < 0) totalProfit -= val.get();
                else totalProfit += val.get();
            }
            context.write(key, new DoubleWritable(totalProfit));*/

            //Calcul de produits et utile aussi pour le calcul d'exemplaire
            double NbProduits = 0;
            for (DoubleWritable val : values) {
                NbProduits += val.get();
            }
            context.write(key, new DoubleWritable(NbProduits));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}