/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;


public class StripesPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int WORD_TO_SEE = 40;

  //First mapper to emit (A, 1)
  public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      //generate a set of unique words
      Set<String> wordOccur = new HashSet<>();
      int wordCount = 0;
      for (String word : tokens) {
        wordCount++;
        if (!wordOccur.contains(word)) {
          wordOccur.add(word);
          WORD.set(word);
          context.write(WORD,ONE);
        }
        if (wordCount >= WORD_TO_SEE) {
          break;
        }
      }
      //to count the number of lines in the file
      WORD.set("a_line_counter");
      context.write(WORD, ONE);
      }
    }

  //first reducer to emit (A, sum)
  public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private static final IntWritable SUM = new IntWritable();

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {

        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        SUM.set(sum);
        context.write(key, SUM);
      }
   }

 //second mapper to emit (A, {B:1, C:1, D:1...})
 private static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HMapStFW> {
   //remember HMapStFW is a string-float key-value pair
    private static final Text KEY = new Text();
    private static final HMapStFW MAP = new HMapStFW();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      List<String> tokens = Tokenizer.tokenize(value.toString());
      ArrayList<String> wordOccur = new ArrayList<>();
      int wordCount = 0;
      for (String word : tokens) {
        wordCount++;
        if (!wordOccur.contains(word)) {
          wordOccur.add(word);
        }
        if (wordCount >= WORD_TO_SEE) {
          break;
        }
      }
      for (int i = 0; i < wordOccur.size(); i++) {
        MAP.clear();
        KEY.set(wordOccur.get(i));
        for (int j = 0; j < wordOccur.size(); j++) {
          //don't want A A
          if (i == j) {
            continue;
          }
          //A {B 1}
          MAP.put(wordOccur.get(j), 1f);
        }
        //TEXT {STRING-FLOAT pairs}
        context.write(KEY,MAP);
      }
    }
  }



  //combiner is to sum all the values(floats) in the map for the same key
  //i.e: A {B 15, C 21, D 11...}
  private static final class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static final class MyReducer2 extends Reducer<Text, HMapStFW, Text, HashMapWritable> {
    private static final Text KEY = new Text(); //(left part of the final output)
    private static final HashMapWritable MAP = new HashMapWritable(); //(right part of the final output)
    //output of first mr job will be stored in this variable
    private static final Map<String, Integer> word_count_output = new HashMap<String, Integer>();

    //same as what I did in PairsPMI
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      //array of output files for the first mapReduce job
      FileStatus[] mr_outputs = fs.globStatus(new Path("tmp_for_stripes/part-r-*"));


      for (FileStatus file : mr_outputs) {
        FSDataInputStream fsdis = fs.open(file.getPath());
        InputStreamReader isr = new InputStreamReader(fsdis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String eachLine = br.readLine();

        LOG.info("Start reading file.");
        while (eachLine != null) {

          String[] mr_data = eachLine.split("\\s+");
          //store pairs like (A, sum) into a variable
          word_count_output.put(mr_data[0], Integer.parseInt(mr_data[1]));
        }
        LOG.info("Finish reading file.");
        br.close();
      }
    }

    //emit A {B (pmi for AB, count for AB), ...}
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }
      //map now looks like {B:3, C:5} and key is A

      int threshold = context.getConfiguration().getInt("threshold",0);

      String eachKey = key.toString();//A
      //number of occurence of the key i.e:A
      Integer eachKeySum = word_count_output.get(eachKey);
      Integer total = word_count_output.get("a_line_counter");


      //co_occur is B C ... of the same key A
      for (String co_occur: map.keySet()) {
        //number of occurence of the co_occurence word (i.e:B)
        Integer co_occurSum = word_count_output.get(co_occur);
        if (map.get(co_occur) >= threshold) {
          //convert the co_occur to writable type
          Text co_occurWritable = new Text(co_occur);
          //number of i.e:(A, B)
          sum = map.get(co_occur);

          PairOfFloatInt pmi_count_pair = new PairOfFloatInt();

          if (total != null && eachKeySum != null && co_occurSum != null) {
            float pmi = (float) Math.log10(1.0f * sum * total / (eachKeySum * co_occurSum));

            pmi_count_pair.set(pmi, (int)sum);
            MAP.put(co_occurWritable, pmi_count_pair);

          }
        }
      }
      if (!MAP.isEmpty()) {
        context.write(key, MAP);
        MAP.clear();
      }
  }
}

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    boolean textOutput = true;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence pairs")
    int threshold = 10;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    //Job 1
    String intermediatePath = "tmp_for_stripes/";
    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.setInt("threshold", args.threshold);
    conf.set("intermediatePath", intermediatePath);

    Job job = Job.getInstance(getConf());
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(intermediatePath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path intermediateDir = new Path(intermediatePath);
    FileSystem.get(getConf()).delete(intermediateDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job one Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    //Job 2
    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - num threshold: " + args.threshold);

    Job jobTwo = Job.getInstance(getConf());
    jobTwo.setJobName(StripesPMI.class.getSimpleName());
    jobTwo.setJarByClass(StripesPMI.class);

    jobTwo.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(jobTwo, new Path(args.input));
    FileOutputFormat.setOutputPath(jobTwo, new Path(args.output));

    jobTwo.setMapOutputKeyClass(Text.class);
    jobTwo.setMapOutputValueClass(HMapStFW.class);
    jobTwo.setOutputKeyClass(Text.class);
    jobTwo.setOutputValueClass(HashMapWritable.class);
    if (args.textOutput) {
      jobTwo.setOutputFormatClass(TextOutputFormat.class);
    } else {
      jobTwo.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    jobTwo.setMapperClass(MyMapper2.class);
    jobTwo.setCombinerClass(MyCombiner.class);
    jobTwo.setReducerClass(MyReducer2.class);

    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    //long startTime = System.currentTimeMillis();
    jobTwo.waitForCompletion(true);
    System.out.println("Job two Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
