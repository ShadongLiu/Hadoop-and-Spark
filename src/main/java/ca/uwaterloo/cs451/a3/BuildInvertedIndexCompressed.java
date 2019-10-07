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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final IntWritable TF = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      for (PairOfObjectInt<String> e : COUNTS) {
        //TF represents the time each term appear in the current doc
        TF.set(e.getRightElement());
        //emit(one, 1) 1  (fish, 1) 2... for each doc the 1 inside represents the docno
        context.write(new PairOfStringInt(e.getLeftElement(), (int)docno.get()),TF);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    //private static final IntWritable DF = new IntWritable();
    int df = 0;
    int prevDoc = 0;
    String prevWord = "";
    private static final Text TERM = new Text();
    private final static ByteArrayOutputStream postingByte = new ByteArrayOutputStream();
    private final static DataOutputStream postingData = new DataOutputStream(postingByte);


    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      //ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

      if (prevWord != null && !key.getLeftElement().equals(prevWord)) {
        //flush and write out the postings to the disk
        postingData.flush();

        TERM.set(prevWord);
        //create the buffer we need for the final postings (including the df)
        ByteArrayOutputStream allByteBuffer = new ByteArrayOutputStream();
        DataOutputStream allDataBuffer = new DataOutputStream(allByteBuffer);
        //write df first since we want df comes first
        WritableUtils.writeVInt(allDataBuffer, df);
        //write postings bytes behind df byte
        allDataBuffer.write(postingByte.toByteArray());

        context.write(TERM, new BytesWritable(allByteBuffer.toByteArray()));
        // prepare for next different word
        postingByte.reset();
        allByteBuffer.reset();
        prevDoc = 0;
        df = 0;
      }


      while (iter.hasNext()) {
        df++;
        //postings.add(iter.next().clone());
        int tf = iter.next().get();
        int gap = key.getRightElement() - prevDoc;
        prevDoc = key.getRightElement();
        //Here use gap compression(only encode gaps)
        //Write gaps first since postings look like (docno(+gap), tf)
        WritableUtils.writeVInt(postingData, gap);
        WritableUtils.writeVInt(postingData, tf);
      }
      //finished the postings for one word (no df included)
      prevWord = key.getLeftElement();
    }


  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    postingData.flush();
    TERM.set(prevWord);

    ByteArrayOutputStream allByteBuffer = new ByteArrayOutputStream();
    DataOutputStream allDataBuffer = new DataOutputStream(allByteBuffer);
    WritableUtils.writeVInt(allDataBuffer, df);
    allDataBuffer.write(postingByte.toByteArray());

    context.write(TERM, new BytesWritable(allByteBuffer.toByteArray()));

    postingByte.close();
    postingData.close();
  }
}

private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
  @Override
  public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
    return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
