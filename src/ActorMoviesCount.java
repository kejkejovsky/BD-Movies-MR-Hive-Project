import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ActorMoviesCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ActorMoviesCount.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ActorMoviesCount(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "actormoviescount");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ActorMoviesCountMapper.class);
        job.setReducerClass(ActorMoviesCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static class ActorMoviesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text tconst = new Text();
        private Text category = new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try{
                if(key.get() == 0)
                    return;
                else{
                    String line = value.toString();
                    int i = 0;
                    for(String word : line.split(",(?=([^\"]*\"[^\"])*[^\"]*$)")){
                        if(i == 0) {
                            tconst.set(word);
                        }
                        if(i == 3){
                            category.set(word);
                        }
                        i++;
                    }
                    if(category.equals("actor") || category.equals("actress") ) {
                        context.write(tconst, one);
                    }
                }
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    public static class ActorMoviesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int sum;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}