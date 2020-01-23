import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount {

    public static class MappingWords extends Mapper<Object, Text,Text, IntWritable>{
        private final IntWritable intialValue=new IntWritable(1);
        private Text keyWord=new Text();

        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            StringTokenizer stringTokenizer=new StringTokenizer(value.toString());

            while(stringTokenizer.hasMoreTokens())
            {
                keyWord.set(stringTokenizer.nextToken());
                context.write(keyWord,intialValue);
            }
        }

    }
    public static class CombineAndReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable res=new IntWritable();

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
        {
            int tot=0;
            for(IntWritable val:values)
            {
                tot += val.get();
            }
            res.set(tot);
            context.write(key,res);
        }

    }

    public static void main(String args[]) throws Exception
    {
        Configuration config = new Configuration();
        Job job=Job.getInstance(config);
        job.setJobName("Word Counter");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MappingWords.class);
        job.setCombinerClass(CombineAndReduce.class);
        job.setReducerClass(CombineAndReduce.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Path outputPath=new Path(args[1]);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        if(!job.waitForCompletion(true))
            System.exit(0);
        else
            System.exit(1);


    }
}
