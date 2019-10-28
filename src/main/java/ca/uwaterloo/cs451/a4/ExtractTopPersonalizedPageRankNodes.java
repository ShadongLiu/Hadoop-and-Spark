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

package ca.uwaterloo.cs451.a4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
  private static final String SOURCE_NODES = "node.src";

  private static class MyMapper extends Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private ArrayList<TopScoredObjects<Integer>> queue;
    private ArrayList<Integer> sources;
    private static int num_source_nodes = 0;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      String[] sourceNodes = context.getConfiguration().getStrings(SOURCE_NODES, "");
      num_source_nodes = sourceNodes.length;
      //sources = new ArrayList<Integer>();
      for (String sn : sourceNodes) {
        sources.add(Integer.valueOf(sn));
      }
      queue = new ArrayList<TopScoredObjects<Integer>>();
      for (int i = 0; i < num_source_nodes; i++) {
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException {
      for (int i = 0; i < num_source_nodes; i++) {
        TopScoredObjects<Integer> q = queue.get(i);
        q.add(node.getNodeId(), node.getPageRank().get(i));
        queue.set(i, q);
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      PairOfInts key = new PairOfInts();
      FloatWritable value = new FloatWritable();

      for (int i = 0; i < num_source_nodes; i++) {
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(i, pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  private static class MyReducer extends Reducer<PairOfInts, FloatWritable, Text, Text> {
    private ArrayList<TopScoredObjects<Integer>> queue;
    private ArrayList<Integer> sources;
    private static int num_source_nodes = 0;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      String[] sourceNodes = context.getConfiguration().getStrings(SOURCE_NODES, "");
      num_source_nodes = sourceNodes.length;
      //sources = new ArrayList<Integer>();
      for (String sn : sourceNodes) {
        sources.add(Integer.valueOf(sn));
      }
      queue = new ArrayList<TopScoredObjects<Integer>>();
      for (int i = 0; i < num_source_nodes; i++) {
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context) throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      TopScoredObjects<Integer> q = queue.get(nid.getLeftElement());
      q.add((int)nid.getRightElement(), iter.next().get());
      queue.set(nid.getLeftElement(), q);

      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      FloatWritable key = new FloatWritable();
      IntWritable value = new IntWritable();

      
      for (int i = 0; i < num_source_nodes; i++) {
        context.write(new Text("Source: " + sources.get(i)), new Text(""));
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
        value.set(pair.getLeftElement());
        key.set((float)StrictMath.exp(pair.getRightElement()));
        context.write(new Text(String.format("%.5f", key.get())), new Text(String.valueOf(value)));
        }
        if (i < queue.size() - 1) {
          context.write(new Text(""), new Text(""));
        }
      }
      
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg().withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)
    || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String srcStr = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + srcStr);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings(SOURCE_NODES, srcStr);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // Text instead of FloatWritable so we can control formatting

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    Path p = new Path(outputPath + "/part-r-00000");
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
    try{
      String line;
      line = br.readLine();
      while (line != null) {
        System.out.println(line);
        line = br.readLine();
      }
    }finally {
      br.close();
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
