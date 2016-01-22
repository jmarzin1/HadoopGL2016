import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockChangeAnalysis {

    public static class StockChangeAnalysisMapper
            extends Mapper<Object, Text, IntWritable, Writable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            if (tokens.length < 7 || tokens[4].length()==0) return;
            String name;
            int region;
            int population;
            double latitude;
            double longitude;
            try {
                name = tokens[2];
                region = Integer.parseInt(tokens[3]);
                population = Integer.parseInt(tokens[4]);
                latitude = Double.parseDouble(tokens[5]);
                longitude = Double.parseDouble(tokens[6]);
            }
            catch(Exception e) {
                return;
            }
            context.write(new IntWritable(region) , new MarketIndex());
        }

    }

    public static class StockChangeAnalysisCombiner extends Reducer<IntWritable, Writable, IntWritable, Writable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class StockChangeAnalysisReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MonProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(StockChangeAnalysis.class);
        job.setMapperClass(StockChangeAnalysisMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Writable.class);
        job.setCombinerClass(StockChangeAnalysisCombiner.class);
        job.setReducerClass(StockChangeAnalysisReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
