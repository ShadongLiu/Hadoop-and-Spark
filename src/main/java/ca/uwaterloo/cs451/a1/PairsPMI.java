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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class PairsPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static final int WORD_LIMIT = 40;

  // First mapper to emit (A, 1)
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
        if (wordCount >= WORD_LIMIT) {
          break;
        }
      }
      //to count the number of lines in the file
      WORD.set("a_line_counter");
      context.write(WORD, ONE);
      }
    }

  //first reducer to emit(A, sum)
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


  //second mapper to emit(A, B) 1
  private static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings Pair = new PairOfStrings();

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
        if (wordCount >= WORD_LIMIT) {
          break;
        }
      }

      for (int i = 0; i < wordOccur.size(); i++) {
        for (int j = i + 1; j < wordOccur.size(); j++) {
          //get(A, B) 1
          Pair.set(wordOccur.get(i), wordOccur.get(j));
          context.write(Pair, ONE);
          //get(B, A) 1
          Pair.set(wordOccur.get(j), wordOccur.get(i));
          context.write(Pair, ONE);
        }
      }
    }
  }


  //first combiner to emit (A, B) sum of this pair
  private static final class MyCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }


  private static final class MyReducer2 extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt VALUE = new PairOfFloatInt();
    private static HashMap<String, Integer> word_count_output = new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      //array of output files for the first mapReduce job
      FileStatus[] mr_outputs = fs.globStatus(new Path("tmp_for_paris/part-r-*"));


      for (FileStatus file : mr_outputs) {
        FSDataInputStream fsdis = fs.open(file.getPath());
        InputStreamReader isr = new InputStreamReader(fsdis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String eachLine = br.readLine();

        while (eachLine != null) {

          String[] mr_data = line.split("\\s+");

          //store pairs like (A, sum) into a variable
          word_count_output.put(mr_data[0], Integer.parseInt(mr_data[1]));

          //read next line
          eachLine = br.readLine();
        }
        br.close();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold",0);

      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (sum >= threshold) {
        String x = key.getLeftElement();//A
        String y = key.getRightElement();//B
        Integer total = word_count_output.get("a_line_counter");
        Integer xVal = word_count_output.get(x);
        Integer yVal = word_count_output.get(y);

        if (total != null && xVal != null && yVal != null) {
          float pmi = (float) Math.log10(1.0f * sum * total / (xVal * yVal));

          VALUE.set(pmi, (int)sum);
          context.write(key, VALUE);
        }
      }
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

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
    String intermediatePath = "tmp_for_paris/";
    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - threshold: " + args.threshold);



    Configuration conf = getConf();
    conf.set("threshold", Integer.toString(args.threshold));
    conf.set("intermediatePath", intermediatePath);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

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
    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - num threshold: " + args.threshold);

    Job jobTwo = Job.getInstance(getConf());
    jobTwo.setJobName(PairsPMI.class.getSimpleName());
    jobTwo.setJarByClass(PairsPMI.class);

    jobTwo.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(jobTwo, new Path(args.input));
    FileOutputFormat.setOutputPath(jobTwo, new Path(args.output));

    jobTwo.setMapOutputKeyClass(PairOfStrings.class);
    jobTwo.setMapOutputValueClass(FloatWritable.class);
    jobTwo.setOutputKeyClass(PairOfStrings.class);
    jobTwo.setOutputValueClass(PairOfFloatInt.class);
    if (args.textOutput) {
      jobTwo.setOutputFormatClass(TextOutputFormat.class);
    } else {
      jobTwo.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    jobTwo.setMapperClass(MyMapper2.class);
    jobTwo.setCombinerClass(MyCombiner.class);
    jobTwo.setReducerClass(MyReducer2.class);
    jobTwo.setPartitionerClass(MyPartitioner.class);

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
    ToolRunner.run(new PairsPMI(), args);
  }
}
