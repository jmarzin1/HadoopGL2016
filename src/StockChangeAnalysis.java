import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.MatchResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockChangeAnalysis {


/*             PARSING                   */


    public static class StockChangeAnalysisMapper
            extends Mapper<Object, Text, IntWritable, MarketIndex> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date.getTime());
            String toParse = value.toString();
            String[] tokens = toParse.split("\n");
            if ((tokens.length >= 2) && tokens[1].contains("tdv-var")) {
                int i = 2;
                while (!tokens[i].contains("</tr>")) {
                    String[] lineTokens = tokens[i].split(">");
                    parseLine(lineTokens, marketIndex);
                    i++;
                }
            } else {
                return;
            }
            context.write(new IntWritable(), marketIndex);

        }
    }


    public static class StockChangeAnalysisReducer
            extends Reducer<IntWritable, MarketIndex, IntWritable, MarketIndex> {
        public void reduce(IntWritable key, Iterable<MarketIndex> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (MarketIndex value : values) {
                context.write(key, value);
            }
        }
    }



    /*             TOPK (WITHOUT PRE-PARSING)                   */

    public static class StockChangeTopKMapper
            extends Mapper<Object, Text, NullWritable, MarketIndex> {
        public int k = 10;
        private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date.getTime());
            String toParse = value.toString();
            String[] tokens = toParse.split("\n");
            if ((tokens.length >= 2) && tokens[1].contains("tdv-var")) {
                int i = 2;
                while (!tokens[i].contains("</tr>")) {
                    String[] lineTokens = tokens[i].split(">");
                    parseLine(lineTokens, marketIndex);
                    i++;
                }
            } else {
                return;
            }
            topKMarketIndexes.put(marketIndex.getCapitalization(), marketIndex);
            if (topKMarketIndexes.size() > k) {
                topKMarketIndexes.remove(topKMarketIndexes.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // Output our ten records to the reducers with a null key
            for (MarketIndex marketIndex : topKMarketIndexes.values()) {
                context.write(NullWritable.get(), marketIndex);
            }
        }

    }



    /*                  TOP K  (WITH PREPARSED)             */



    public static class StockChangeTopKMapperB
    extends Mapper<Object, Text, NullWritable, MarketIndex> {
    	private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<>();
    	private HashMap<String, Integer> converter = new HashMap<>();
    	
    	private int k = 10;
    	
    	public void map(Object key, Text value, Context context
    			) throws IOException, InterruptedException {
    		
    		MarketIndex marketIndex = new MarketIndex();
    		String toParse = value.toString();
    		String[] tokens = toParse.split(",");
    		if (tokens[0].contains("0")) {
    			fillMarketIndexData(tokens, marketIndex);
    		} else {
    			return;
    		}
            if (converter.get(marketIndex.getName()) == null) {
                topKMarketIndexes.put(marketIndex.getCapitalization(), marketIndex);
                converter.put(marketIndex.getName(), marketIndex.getCapitalization());
                if (topKMarketIndexes.size() > k) {
                    topKMarketIndexes.remove(topKMarketIndexes.firstKey());
                }
            } else {
                Integer tmpCapitalization = converter.get(marketIndex.getName());
                if (tmpCapitalization < marketIndex.getCapitalization()) {
                    topKMarketIndexes.remove(converter.get(marketIndex.getName()));
                    topKMarketIndexes.put(marketIndex.getCapitalization(), marketIndex);
                    converter.put(marketIndex.getName(), marketIndex.getCapitalization());
                }
            }
    	}

    	@Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // Output our ten records to the reducers with a null key
            for (MarketIndex marketIndex : topKMarketIndexes.values()) {
                context.write(NullWritable.get(), marketIndex);
            }
        }

    }

    public static class StockChangeTopKReducer extends Reducer<NullWritable, MarketIndex, NullWritable, Text> {
        public int k = 10;
        private TreeMap<Integer, Text> topKMarketIndexes = new TreeMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<MarketIndex> values, Context context) throws IOException, InterruptedException {
            for (MarketIndex value : values) {
                // Former bug here: need to copy the 'value' instance
                Text marketIndexN = new Text(value.getName());
                if (!topKMarketIndexes.containsValue(marketIndexN)) {
                    topKMarketIndexes.put(value.getCapitalization(), new Text(marketIndexN));
                    if (topKMarketIndexes.size() > k) {
                        topKMarketIndexes.remove(topKMarketIndexes.firstKey());
                    }
                } else {
                    int tmpCapitalization = 0;
                    for (Map.Entry<Integer, Text> entry : topKMarketIndexes.entrySet()) {
                        if (entry.getValue().equals(marketIndexN)) {
                            tmpCapitalization = entry.getKey();
                        }
                    }
                    if (tmpCapitalization < value.getCapitalization()) {
                        topKMarketIndexes.remove(tmpCapitalization);
                        topKMarketIndexes.put(value.getCapitalization(), marketIndexN);
                    }

                }
            }
            for (Text marketIndexName : topKMarketIndexes.descendingMap().values()) {
                context.write(key, marketIndexName);
            }

        }
    }


/*       MINMAX                       */


    public static class MinMaxMapper extends Mapper<Object, Text, Text, MinMaxTuple> {
        private Text outUserId = new Text();
        private MinMaxTuple outminMaxTuple = new MinMaxTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date.getTime());
            String toParse = value.toString();
            String[] tokens = toParse.split("\n");
            if ((tokens.length >= 2) && tokens[1].contains("tdv-var")) {
                int i = 2;
                while (!tokens[i].contains("</tr>")) {
                    String[] lineTokens = tokens[i].split(">");
                    parseLine(lineTokens, marketIndex);
                    i++;
                }
            } else {
                return;
            }
            outminMaxTuple.setMinVar(marketIndex.getDailyVariation());
            outminMaxTuple.setMaxVar(marketIndex.getDailyVariation());
            outUserId.set(marketIndex.getName());
            context.write(outUserId, outminMaxTuple);
        }
    }

    public static class MinMaxReducer extends Reducer<Text, MinMaxTuple, Text, MinMaxTuple> {
        private MinMaxTuple result = new MinMaxTuple();

        public void reduce(Text key, Iterable<MinMaxTuple> values,
                           Context context) throws IOException, InterruptedException {
            result.setMinVar(0);
            result.setMaxVar(0);
            for (MinMaxTuple val : values) {
                if (val.getMinVar() < (result.getMinVar())) {
                    result.setMinVar(val.getMinVar());
                }
                if (val.getMaxVar() > result.getMaxVar()) {
                    result.setMaxVar(val.getMaxVar());
                }
            }
            System.out.println(key + " " + result);
            context.write(key, result);
        }
    }


    /*
    public static class CorrelationMapper extends Mapper<Object, Text, Text, MarketIndex> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date);
            String toParse = value.toString();
            String[] tokens = toParse.split("\n");
            if ((tokens.length >= 2) && tokens[1].contains("tdv-var")) {
                int i = 2;
                while (!tokens[i].contains("</tr>")) {
                    String[] lineTokens = tokens[i].split(">");
                    parseLine(lineTokens, marketIndex);
                    i++;
                }
            } else {
                return;
            }
            context.write(new Text(marketIndex.getDate().toString()), marketIndex);
        }
    }

    public static class CorrelationMapper2 extends Mapper<Text, MarketIndex, Text, MarketIndex> {
        public void reduce(IntWritable key, Iterable<MarketIndex> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
*/


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("htmlinput.start", "<tr");
        conf.set("htmlinput.end", "</tr>");

        Job job = Job.getInstance(conf, "MonProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(StockChangeAnalysis.class);
        //job.setInputFormatClass(HtmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String commande = "";
        if (args.length > 2) {
            commande = args[0];
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        }
        int returnCode = 0;
        switch (commande) {
            case "parsing":
            	job.setInputFormatClass(HtmlInputFormat.class);

                job.setMapperClass(StockChangeAnalysisMapper.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(MarketIndex.class);

                job.setReducerClass(StockChangeAnalysisReducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(MarketIndex.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
            case "topK":
            	job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(StockChangeTopKMapperB.class);
                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(MarketIndex.class);

                job.setReducerClass(StockChangeTopKReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
            case "topKWithoutParsing":
                job.setInputFormatClass(HtmlInputFormat.class);

                job.setMapperClass(StockChangeTopKMapper.class);
                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(MarketIndex.class);

                job.setReducerClass(StockChangeTopKReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;

            case "minMax":
                job.setInputFormatClass(HtmlInputFormat.class);

                job.setMapperClass(MinMaxMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(MinMaxTuple.class);

                job.setReducerClass(MinMaxReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(MinMaxTuple.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
            default:
                System.out.println("Usage: commands args");
                System.out.println("commands:");
                System.out.println(" - parsing [inputURI] [outputURI]");
                System.out.println(" - topK [inputURI] [outputURI]");
                returnCode = 1;
        }

        System.exit(returnCode);
    }

    public static void parseLine(String[] lineTokens, MarketIndex marketIndex) {
        String pattern = "(tdv-[A-Za-z_]+)";
        String[] localTokens;
        Scanner scan = new Scanner(lineTokens[0]);
        if (scan.findInLine(pattern) != null) {
            MatchResult matchResult = scan.match();
            switch (matchResult.group(0)) {
                case "tdv-libelle":
                    localTokens = lineTokens[4].split("<");
                    marketIndex.setName(localTokens[0]);
                    break;
                case "tdv-last":
                    localTokens = lineTokens[3].split("\\s");
                    marketIndex.setClosingValue(Float.parseFloat(localTokens[0]));
                    break;
                case "tdv-var":
                    localTokens = lineTokens[2].split("%");
                    marketIndex.setDailyVariation(Float.parseFloat(localTokens[0]));
                    break;
                case "tdv-open":
                    localTokens = lineTokens[3].split("<");
                    if (!localTokens[0].equals("ND")) {
                        marketIndex.setOpeningValue(Float.parseFloat(localTokens[0].replaceAll("\\s+", "")));
                    }
                    break;
                case "tdv-high":
                    localTokens = lineTokens[3].split("<");
                    if (!localTokens[0].equals("ND")) {
                        marketIndex.setHigherValue(Float.parseFloat(localTokens[0].replaceAll("\\s+", "")));
                    }
                    break;
                case "tdv-low":
                    localTokens = lineTokens[3].split("<");
                    if (!localTokens[0].equals("ND")) {
                        marketIndex.setLowerValue(Float.parseFloat(localTokens[0].replaceAll("\\s+", "")));
                    }
                    break;
                case "tdv-var_an":
                    localTokens = lineTokens[2].split("%");
                    marketIndex.setAnnualVariation(Float.parseFloat(localTokens[0]));
                    break;
                case "tdv-tot_volume":
                    localTokens = lineTokens[2].split("<");
                    int volumeTotal = Integer.parseInt(localTokens[0].replaceAll("\\s+", ""));
                    marketIndex.setCapitalization(volumeTotal);
                    break;
                default:
                    break;
            }
            scan.close();
        }
    }

    public static void fillMarketIndexData(String[] tokens, MarketIndex marketIndex) {

        String[] tokenName = tokens[0].split("'");
        marketIndex.setName(tokenName[1]);
        String[] tokenNumber = tokens[1].split("=");
        marketIndex.setDate(Long.parseLong(tokenNumber[1]));
        tokenNumber = tokens[2].split("=");
        marketIndex.setClosingValue(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[3].split("=");
        marketIndex.setDailyVariation(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[4].split("=");
        marketIndex.setOpeningValue(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[5].split("=");
        marketIndex.setHigherValue(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[6].split("=");
        marketIndex.setLowerValue(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[7].split("=");
        marketIndex.setAnnualVariation(Float.parseFloat(tokenNumber[1]));
        tokenNumber = tokens[8].split("=");
        String[] tokenCapitalization = tokenNumber[1].split("}");
        marketIndex.setCapitalization(Integer.parseInt(tokenCapitalization[0]));
    }

    public static Date convertDate(String fileName) {
        String pattern = "([0-9]+)";
        long timestamp = 0;
        Scanner scan = new Scanner(fileName);
        if (scan.findInLine(pattern) != null) {
            MatchResult matchResult = scan.match();
            timestamp = Long.parseLong(matchResult.group(0)) * 1000;
            scan.close();
        }
        return new Date(timestamp);
    }

}
