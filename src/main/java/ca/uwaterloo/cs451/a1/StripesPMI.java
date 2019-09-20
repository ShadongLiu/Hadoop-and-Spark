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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class StripesPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int WORD_LIMIT = 40;

  //First mapper to emit (Word, 1)
  public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> wordOccur = new HashSet<>();
      int numWords = 0;
      for (String word : tokens) {
        numWords++;
        if (!wordOccur.contains(word)) {
          wordOccur.add(word);
          WORD.set(word);
          context.write(WORD,ONE);
        }
        if (numWords >= WORD_LIMIT) {
          break;
        }
      }
      //to count the number of lines in the file
      WORD.set("abcdef");
      context.write(WORD, ONE);
      }
    }

  //first reducer to emit (word, sum)
  public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private static final IntWritable SUM = new IntWritable();

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
          // Sum up values.
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        SUM.set(sum);
        context.write(key, SUM);
      }
   }

 //second mapper to emit (word, {word1:1, word2:1, word3:1...})
 private static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HMapStFW> {
   //remember HMapStFW is a string-float key-value pair
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      Map<String, HMapStFW> stripes = new HashMap<>();
      List<String> tokens = Tokenizer.tokenize(value.toString());

      if (tokens.size() < 2) return;
      for (int i = 1; i < tokens.size(); i++) {
        String prev = tokens.get(i-1);
        String cur = tokens.get(i);
        if (stripes.containsKey(prev)) {
          HMapStFW stripe = stripes.get(prev);
          if (stripe.containsKey(cur)) {
            stripe.put(cur, stripe.get(cur)+1.0f);
          } else {
            stripe.put(cur, 1.0f);
          }
        } else {
          HMapStFW stripe = new HMapStFW();
          stripe.put(cur, 1.0f);
          stripes.put(prev, stripe);
        }
      }

      for (String t : stripes.keySet()) {
        KEY.set(t);
        context.write(KEY, stripes.get(t));
      }
    }
  }


  //combiner is to sum the all the values(floats) in the map for the same key
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

    private static final Map<String, Integer> wordCounts = new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FileStatus[] status = fs.globStatus(new Path("tmp2/part-r-*"));
      for (FileStatus file : status) {
        FSDataInputStream is = fs.open(file.getPath());
        InputStreamReader isr = new InputStreamReader(is, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();
        while (line != null) {
          String[] data = line.split("\\s+");
          if (data.length == 2) {
            wordCounts.put(data[0], Integer.parseInt(data[1]));
          }
          line = br.readLine();
        }
        br.close();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold",0);

      String eachKey = key.toString();
      KEY.set(eachKey);
      Integer eachKeySum = wordCounts.get(eachKey);
      Integer total = wordCounts.get("abcdef");
      for (String term: map.keySet()) {
        Integer termSum = wordCounts.get(term);
        if (map.get(term) >= threshold) {
          Text termWritable = new Text(term);
          sum = map.get(term);
          if (total != null && eachKeySum != null && termSum != null) {
            float pmi = (float) Math.log10(1.0f * sum * total / (eachKeySum * termSum));
            PairOfFloatInt pmi_count_pair = new PairOfFloatInt();
            pmi_count_pair.set(pmi, (int)sum);
            MAP.put(termWritable, pmi_count_pair);
        }
      }
      context.write(KEY, MAP);
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
    boolean textOutput = false;

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
    String intermediatePath = "tmp2/";
    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.set("threshold", Integer.toString(args.threshold));
    conf.set("intermediatePath", intermediatePath);

    Job job = Job.getInstance(getConf());
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(intermediatePath);
    FileSystem.get(getConf()).delete(outputDir, true);

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

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
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
