import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class FoF {
        
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text valueOut = new Text();
    private Text keyOut = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String user;
      String friend;
      List<String> friends = new ArrayList<String>();
      ListIterator<String> li;

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      //Assign user to first value in line
      user = tokenizer.nextToken();

      //Parse through user's friends
      while (tokenizer.hasMoreTokens()) {
        friend = tokenizer.nextToken();
        friends.add(friend);

        if (tokenizer.hasMoreTokens()) {
          friend = friend + " ";
        }

        valueOut.set(valueOut.toString() + friend);
      }

      //Send key[user,friend](ordered) value[friends]
      li = friends.listIterator();
      while (li.hasNext()) {
        friend = li.next();

        if (Integer.parseInt(user) < Integer.parseInt(friend)) {
          keyOut.set(user + " " + friend);
        } else {
          keyOut.set(friend + " " + user);
        }

        context.write(keyOut, valueOut);
      }
    }
  } 
        
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private Text keyOut = new Text();

    public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
      boolean first = true;
      boolean write = false;
      List<String> shared = new ArrayList<String>();
      List<String> friends = new ArrayList<String>();
      ListIterator<String> li;
      String friend;

      //Iterate through values
      while (values.hasNext()) {
        String value = values.next().toString();
        StringTokenizer friendsTokens = new StringTokenizer(value);
        friends.clear();

        if (first) {
          //Initialize shared list
          while (friendsTokens.hasMoreTokens()) {
            shared.add(friendsTokens.nextToken());
          }
          first = false;
        } else {
          //Interset shared list with new values
          while (friendsTokens.hasMoreTokens()) {
            friends.add(friendsTokens.nextToken());
          }
          shared.retainAll(friends);
          write = true;
        }
      }

      //Write if intersection was made
      if (write) {
        li = shared.listIterator();
        while (li.hasNext()) {
          friend = li.next();
          keyOut.set(friend);
          context.write(keyOut, key);
        }
      }
    }
  }
        
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "fof");
    job.setJarByClass(FoF.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outputPath = new Path(args[1]);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    outputPath.getFileSystem(conf).delete(outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
