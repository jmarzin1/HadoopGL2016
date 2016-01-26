import java.io.IOException;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockChangeAnalysis {

    public static class StockChangeAnalysisMapper
            extends Mapper<Object, Text, NullWritable, MarketIndex> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
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
            }
            else {
                return;
            }
            context.write(NullWritable.get(), marketIndex);

        }
    }


    public static class StockChangeTopKMapper
            extends Mapper<Object, Text, NullWritable, MarketIndex> {
        public int k = 10;
        private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
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
            }
            else {
            	return;
        	}
            topKMarketIndexes.put(marketIndex.getCapitalization(), marketIndex);
            if (topKMarketIndexes.size() > k){
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
            extends Reducer<IntWritable, MarketIndex, IntWritable, MarketIndex> {
    	public void reduce(IntWritable key, Iterable<MarketIndex> values,
                           Context context
        ) throws IOException, InterruptedException {
    		for (MarketIndex value : values) {
    			context.write(key, value);
    		}
        }
    }
    
    public static class StockChangeTopKReducer extends Reducer<NullWritable, MarketIndex, NullWritable, MarketIndex> {
		public int k = 10;
		private TreeMap<Integer, MarketIndex> topKMarketIndexes = new TreeMap<Integer, MarketIndex>();
		@Override
		public void reduce(NullWritable key, Iterable<MarketIndex> values,	Context context) throws IOException, InterruptedException {
			for (MarketIndex value : values) {
				// Former bug here: need to copy the 'value' instance
				topKMarketIndexes.put(value.getCapitalization(), new MarketIndex(value));
				if (topKMarketIndexes.size() > k) {
					topKMarketIndexes.remove(topKMarketIndexes.firstKey());
				}
			}
			for (MarketIndex mi : topKMarketIndexes.descendingMap().values()) {
                System.out.println(mi);
				context.write(key, mi);
			}
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("htmlinput.start", "<tr");
        conf.set("htmlinput.end", "</tr>");

        Job job = Job.getInstance(conf, "MonProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(StockChangeAnalysis.class);
        job.setMapperClass(StockChangeTopKMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MarketIndex.class);
        job.setReducerClass(StockChangeTopKReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MarketIndex.class);
        job.setInputFormatClass(HtmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
                    if(!localTokens[0].equals("ND")) {
                        marketIndex.setOpeningValue(Float.parseFloat(localTokens[0].replaceAll("\\s+","")));
                    }
                    break;
                case "tdv-high":
                    localTokens = lineTokens[3].split("<");
                    if(!localTokens[0].equals("ND")) {
                        marketIndex.setHigherValue(Float.parseFloat(localTokens[0].replaceAll("\\s+","")));
                    }
                    break;
                case "tdv-low":
                    localTokens = lineTokens[3].split("<");
                    if(!localTokens[0].equals("ND")) {
                        marketIndex.setLowerValue(Float.parseFloat(localTokens[0].replaceAll("\\s+","")));
                    }
                    break;
                case "tdv-var_an":
                    localTokens = lineTokens[2].split("%");
                    marketIndex.setAnnualVariation(Float.parseFloat(localTokens[0]));
                    break;
                case "tdv-tot_volume":
                    localTokens = lineTokens[2].split("<");
                    int volumeTotal = Integer.parseInt(localTokens[0].replaceAll("\\s+",""));
                    marketIndex.setCapitalization(volumeTotal);
                    break;
                default:
                    break;
            }
            scan.close();
        }
    }

    public static Date convertDate(String fileName) {
        String pattern = "([0-9]+)";
        long timestamp = 0;
        Scanner scan = new Scanner(fileName);
        if (scan.findInLine(pattern) != null) {
            MatchResult matchResult = scan.match();
            timestamp = Long.parseLong(matchResult.group(0))*1000;
            scan.close();
        }
        return new Date(timestamp);
    }

}
