import java.io.IOException;
import java.util.Scanner;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockChangeAnalysis {

    public static class StockChangeAnalysisMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            MarketIndex marketIndex = new MarketIndex();
            String toParse = value.toString();
            String[] tokens = toParse.split("\n");
            if (tokens[1].contains("tdv-var")) {
                System.out.println("TESTTEST" + " " + tokens[2] + "\n\n\n" +
                        "\n\n");
                int i = 2;
                while (!tokens[i].contains("</tr>")) {
                    System.out.println(tokens[i]);
                    String[] lineTokens = tokens[i].split(">");
                    parseLine(lineTokens, marketIndex);
                    i++;
                }
            }
            System.out.println(marketIndex);
            context.write(new IntWritable(1), new IntWritable(2));
        }

        private void parseLine(String[] lineTokens, MarketIndex marketIndex) {
            String pattern = "(tdv-[A-Za-z_]+)";
            String[] localTokens;
            Scanner scan = new Scanner(lineTokens[0]);
            if (scan.findInLine(pattern) != null) {
                MatchResult matchResult = scan.match();
                switch (matchResult.group(0)) {
                    case "tdv-libelle":
                        localTokens = lineTokens[4].split("<");
                        System.out.println("libelle " + localTokens[0] + "\n");
                        marketIndex.setName(localTokens[0]);
                        break;
                    case "tdv-last":
                        localTokens = lineTokens[3].split("\\s");
                        System.out.println("last " + Float.parseFloat(localTokens[0]) + "\n");
                        marketIndex.setClosingValue(Float.parseFloat(localTokens[0]));
                        break;
                    case "tdv-var":
                        localTokens = lineTokens[2].split("%");
                        System.out.println("var " + Float.parseFloat(localTokens[0]) + "\n");
                        marketIndex.setDailyVariation(Float.parseFloat(localTokens[0]));
                        break;
                    case "tdv-open":
                        localTokens = lineTokens[3].split("<");
                        if(!localTokens[0].equals("ND")) {
                            System.out.println("open " + Float.parseFloat(localTokens[0]) + "\n");
                            marketIndex.setOpeningValue(Float.parseFloat(localTokens[0]));
                        }
                        break;
                    case "tdv-high":
                        localTokens = lineTokens[3].split("<");
                        if(!localTokens[0].equals("ND")) {
                            System.out.println("high " + Float.parseFloat(localTokens[0]) + "\n");
                            marketIndex.setHigherValue(Float.parseFloat(localTokens[0]));
                        }
                        break;
                    case "tdv-low":
                        localTokens = lineTokens[3].split("<");
                        if(!localTokens[0].equals("ND")) {
                            System.out.println("low " + Float.parseFloat(localTokens[0]) + "\n");
                            marketIndex.setLowerValue(Float.parseFloat(localTokens[0]));
                        }
                        break;
                    case "tdv-var_an":
                        localTokens = lineTokens[2].split("%");
                        System.out.println("var_an " + Float.parseFloat(localTokens[0]) + "\n");
                        marketIndex.setAnnualVariation(Float.parseFloat(localTokens[0]));
                        break;
                    case "tdv-tot_volume":
                        localTokens = lineTokens[2].split("<");
                        int volumeTotal = Integer.parseInt(localTokens[0].replaceAll("\\s+",""));
                        System.out.println("tot_volume " + volumeTotal + "\n");
                        marketIndex.setCapitalization(volumeTotal);
                        break;
                    default:
                        System.out.println(matchResult.group(0) + "yspasserien\n");
                        break;
                }
                scan.close();
            }
        }

    }

    /*public static class StockChangeAnalysisCombiner extends Reducer<IntWritable, Writable, IntWritable, Writable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }*/

    public static class StockChangeAnalysisReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("htmlinput.start", "<tr");
        conf.set("htmlinput.end", "</tr>");

        Job job = Job.getInstance(conf, "MonProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(StockChangeAnalysis.class);
        job.setMapperClass(StockChangeAnalysisMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //job.setCombinerClass(StockChangeAnalysisCombiner.class);
        job.setReducerClass(StockChangeAnalysisReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setInputFormatClass(HtmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
