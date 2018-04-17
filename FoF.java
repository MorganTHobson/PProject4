import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FoF {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text triple = new Text();

    //Outputs key: Every possible triple formed with the designated user. value: 1
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String sorted;
      Integer user = Integer.parseInt(itr.nextToken()); //Gathers specified user
      Integer[] arr = new Integer[3];
      int size;
      ArrayList<Integer> friends = new ArrayList<Integer>();

      //Gathers list of friends
      while (itr.hasMoreTokens()) {
        friends.add(Integer.parseInt(itr.nextToken()));
      }

      //Outpur friend triples
      size = friends.size();
      for (int i = 0; i < size; i++) {
        for (int j = i+1; j < size; j++) {
          arr[0] = user;
          arr[1] = friends.get(i);
          arr[2] = friends.get(j);
          Arrays.sort(arr);
          //Sorts triple numerically
          sorted = arr[0].toString() + " " + arr[1].toString() + " " + arr[2].toString();
          triple.set(sorted);
          //Output
          context.write(triple, one);
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
    private Text keyOut = new Text();

    //Outputs key: FoF triangle value: NullWritable
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      //Count occurences of possible triangle
      for (IntWritable val : values) {
        sum += val.get();
      }
      //If key occurs 3 times, triangle exists, output
      if (sum >= 3) {
        StringTokenizer itr = new StringTokenizer(key.toString());
        String[] triple = new String[3];
        triple[0] = itr.nextToken();
        triple[1] = itr.nextToken();
        triple[2] = itr.nextToken();

        //Output all appropriate orderings of triangle
        keyOut.set(triple[0] + " " + triple[1] + " " + triple[2]);
        context.write(keyOut, NullWritable.get());
        keyOut.set(triple[1] + " " + triple[0] + " " + triple[2]);
        context.write(keyOut, NullWritable.get());
        keyOut.set(triple[2] + " " + triple[0] + " " + triple[1]);
        context.write(keyOut, NullWritable.get());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf= new Configuration();
    Job job = new Job(conf,"fof");

    job.setJarByClass(FoF.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outputPath = new Path(args[1]);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Remove need to manually remove output directory
    outputPath.getFileSystem(conf).delete(outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
