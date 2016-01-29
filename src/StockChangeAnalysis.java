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
import org.apache.hadoop.io.LongWritable;
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
            String rawString = value.toString();
            String[] tokens = rawString.split("\n");
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
        public int k = 0;
        private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<>();

        @Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("k", 10);
		}

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date.getTime());
            String rawString = value.toString();
            String[] tokens = rawString.split("\n");
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
            topKMarketIndexes.put(marketIndex.getDailyExchangeVolume(), marketIndex);
            if (topKMarketIndexes.size() > k) {
                topKMarketIndexes.remove(topKMarketIndexes.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (MarketIndex marketIndex : topKMarketIndexes.values()) {
                context.write(NullWritable.get(), marketIndex);
            }
        }

    }



    /*                  TOP K  (WITH PREPARSING)             */


    public static class StockChangeTopKMapperFromCleanFile
            extends Mapper<Object, Text, NullWritable, MarketIndex> {
        private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<>();
        private HashMap<String, Integer> converter = new HashMap<>();

        public int k = 0;

        @Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("k", 10);
		}

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            MarketIndex marketIndex = new MarketIndex();
            String rawString = value.toString();
            String[] tokens = rawString.split(",");
            if (tokens[0].contains("0")) {
                fillMarketIndexData(tokens, marketIndex);
            } else {
                return;
            }
            if (converter.get(marketIndex.getName()) == null) {
                topKMarketIndexes.put(marketIndex.getDailyExchangeVolume(), marketIndex);
                converter.put(marketIndex.getName(), marketIndex.getDailyExchangeVolume());
                if (topKMarketIndexes.size() > k) {
                    topKMarketIndexes.remove(topKMarketIndexes.firstKey());
                }
            } else {
                Integer tmpDailyExchangeVolume = converter.get(marketIndex.getName());
                if (tmpDailyExchangeVolume < marketIndex.getDailyExchangeVolume()) {
                    topKMarketIndexes.remove(converter.get(marketIndex.getName()));
                    topKMarketIndexes.put(marketIndex.getDailyExchangeVolume(), marketIndex);
                    converter.put(marketIndex.getName(), marketIndex.getDailyExchangeVolume());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (MarketIndex marketIndex : topKMarketIndexes.values()) {
                context.write(NullWritable.get(), marketIndex);
            }
        }

    }

    public static class StockChangeTopKReducer extends Reducer<NullWritable, MarketIndex, NullWritable, Text> {
        public int k = 0;
        private TreeMap<Integer, Text> topKMarketIndexes = new TreeMap<>();

        @Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<MarketIndex> values, Context context) throws IOException, InterruptedException {
            for (MarketIndex value : values) {
                Text marketIndexN = new Text(value.getName());
                if (!topKMarketIndexes.containsValue(marketIndexN)) {
                    topKMarketIndexes.put(value.getDailyExchangeVolume(), new Text(marketIndexN));
                    if (topKMarketIndexes.size() > k) {
                        topKMarketIndexes.remove(topKMarketIndexes.firstKey());
                    }
                } else {
                    //Removing if index name already present
                    int tmpDailyExchangeVolume = 0;
                    for (Map.Entry<Integer, Text> entry : topKMarketIndexes.entrySet()) {
                        if (entry.getValue().equals(marketIndexN)) {
                            tmpDailyExchangeVolume = entry.getKey();
                        }
                    }
                    if (tmpDailyExchangeVolume < value.getDailyExchangeVolume()) {
                        topKMarketIndexes.remove(tmpDailyExchangeVolume);
                        topKMarketIndexes.put(value.getDailyExchangeVolume(), marketIndexN);
                    }

                }
            }
            for (Text marketIndexName : topKMarketIndexes.descendingMap().values()) {
                context.write(key, marketIndexName);
            }

        }
    }


/*       MINMAX  (Without Pre-parsing)                     */


    public static class MinMaxMapper extends Mapper<Object, Text, Text, MinMaxTuple> {
        private Text marketIndexName = new Text();
        private MinMaxTuple minMaxTuple = new MinMaxTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Date date = convertDate(fileName);
            MarketIndex marketIndex = new MarketIndex();
            marketIndex.setDate(date.getTime());
            String rawString = value.toString();
            String[] tokens = rawString.split("\n");
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
            minMaxTuple.setMinVar(marketIndex.getDailyVariation());
            minMaxTuple.setMaxVar(marketIndex.getDailyVariation());
            marketIndexName.set(marketIndex.getName());
            context.write(marketIndexName, minMaxTuple);
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
            context.write(key, result);
        }
    }

/*                Best and Worst                              */


    public static class BestAndWorstMapper extends Mapper<Object, Text, LongWritable, MarketIndex> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            MarketIndex marketIndex = new MarketIndex();
            String rawString = value.toString();
            String[] tokens = rawString.split(",");
            if (tokens[0].contains("0")) {
                fillMarketIndexData(tokens, marketIndex);
            } else {
                return;
            }
            context.write(new LongWritable(marketIndex.getDate()), marketIndex);
        }
    }


    public static class BestAndWorstReducer extends Reducer<LongWritable, MarketIndex, Text, Text> {
        public void reduce(LongWritable key, Iterable<MarketIndex> values, Context context) throws IOException, InterruptedException {
            Extremas extremas = new Extremas(3);
            for (MarketIndex val : values) {
                extremas.addBest(val.getDailyVariation(), val.getName());
                extremas.addWorst(val.getDailyVariation(), val.getName());
            }
            for (Map.Entry<Float,String>entry : extremas.getBest().entrySet()){
                for (Map.Entry<Float,String>entry2 : extremas.getWorst().entrySet()){
                    context.write(new Text(entry.getValue() + "# "), new Text(" #" + entry2.getValue()));
                }

            }
        }
    }

    /*               BestAndWorstCount                              */


    public static class BestAndWorstCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String rawString = value.toString();
            String[] tokens = rawString.split("#");
            String result="";
            if (tokens.length > 2) {
            	if (tokens[0].compareTo(tokens[2]) > 0) {
            		result= tokens[0] + " " + tokens[2];
            	}
            	else {
            		result= tokens[2] + " " + tokens[0];
            	}
            }
            context.write(new Text(result), new IntWritable(1));
        }
    }

    public static class BestAndWorstCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	Integer count = 0;
            for (IntWritable val : values) {
            	count+=val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("htmlinput.start", "<tr");
        conf.set("htmlinput.end", "</tr>");

        if (args.length > 3) {
            conf.setInt("k", Integer.parseInt(args[3]));
        }

        Job job = Job.getInstance(conf, "StockChangeAnalysis");
        job.setNumReduceTasks(1);
        job.setJarByClass(StockChangeAnalysis.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String commande = "";
        if (args.length > 2) {
            commande = args[0];
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        }
        int returnCode;
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

                job.setMapperClass(StockChangeTopKMapperFromCleanFile.class);
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
                break;
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
            case "bestAndWorst":
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(BestAndWorstMapper.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(MarketIndex.class);

                job.setReducerClass(BestAndWorstReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
            case "count":
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(BestAndWorstCountMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);

                job.setReducerClass(BestAndWorstCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
            default:
                System.out.println("Usage: commands args");
                System.out.println("commands:");
                System.out.println("With Raw Data :");
                System.out.println(" - parsing [inputURI] [outputURI]");
                System.out.println(" - minMax [inputURI] [outputURI]");
                System.out.println(" - topKWithoutParsing [inputURI] [outputURI] {k}");
                System.out.println("With Clean Data :");
                System.out.println(" - topK [inputURI] [outputURI] {k}");
                System.out.println(" - BestAndWorst [inputURI] [outputURI]");
                System.out.println("With result of Best and Worth :");
                System.out.println(" - count [inputURI] [outputURI]");
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
                    marketIndex.setDailyExchangeVolume(volumeTotal);
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
        String[] tokenDailyExchangeVolume = tokenNumber[1].split("}");
        marketIndex.setDailyExchangeVolume(Integer.parseInt(tokenDailyExchangeVolume[0]));
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
