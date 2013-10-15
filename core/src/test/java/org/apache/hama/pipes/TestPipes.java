/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hama.pipes;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.junit.Test;

/**
 * Test case for {@link PipesBSP}
 * 
 */
public class TestPipes extends HamaCluster {
  private static final Log LOG = LogFactory.getLog(TestPipes.class);

  private static Path cppExamples = new Path(
      System.getProperty("install.c++.examples"));

  static Path summation = new Path(cppExamples, "bin/summation");
  static Path piestimator = new Path(cppExamples, "bin/piestimator");
  static Path matrixmultiplication = new Path(cppExamples,
      "bin/matrixmultiplication");

  public static String TMP_OUTPUT = "/tmp/test-example/";
  public static final String TMP_OUTPUT_PATH = "/tmp/messageQueue";
  public static Path OUTPUT_PATH = new Path(TMP_OUTPUT + "serialout");

  final static String[] summation_input = new String[] {
      "`and\t1\na\t1\nand\t1\nbeginning\t1\nbook\t1\nbut\t1\nby\t1\n"
          + "conversation?'\t1\ndo:\t1\nhad\t2\nhaving\t1\nher\t2\nin\t1\nit\t1\n"
          + "it,\t1\nno\t1\nnothing\t1\nof\t3\non\t1\nonce\t1\nor\t3\npeeped\t1\n"
          + "pictures\t2\nthe\t3\nthought\t1\nto\t2\nuse\t1\nwas\t2\n",

      "Alice\t2\n`without\t1\nbank,\t1\nbook,'\t1\nconversations\t1\nget\t1\n"
          + "into\t1\nis\t1\nreading,\t1\nshe\t1\nsister\t2\nsitting\t1\ntired\t1\n"
          + "twice\t1\nvery\t1\nwhat\t1\n" };

  protected HamaConfiguration configuration;

  public TestPipes() {
    configuration = new HamaConfiguration();
    configuration.set("bsp.master.address", "localhost");
    configuration.set("hama.child.redirect.log.console", "true");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", "/tmp/hama-test");
    configuration.set(DiskQueue.DISK_QUEUE_PATH_KEY, TMP_OUTPUT_PATH);
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testPipes() throws Exception {
    if (System.getProperty("compile.c++") == null) {
      LOG.info("compile.c++ is not defined, so skipping TestPipes");
      return;
    }

    Path inputPath = new Path("testing/in");
    Path outputPath = new Path("testing/out");

    // Test Summation example
    writeInputFile(FileSystem.get(configuration), inputPath);

    runProgram(configuration, summation, inputPath, outputPath, 3,
        this.numOfGroom, summation_input);

    cleanup(FileSystem.get(configuration), outputPath);

    // Test MatrixMultiply example

  }

  static void writeInputFile(FileSystem fs, Path dir) throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    out.writeBytes("Alice was beginning to get very tired of sitting by her\n");
    out.writeBytes("sister on the bank, and of having nothing to do: once\n");
    out.writeBytes("or twice she had peeped into the book her sister was\n");
    out.writeBytes("reading, but it had no pictures or conversations in\n");
    out.writeBytes("it, `and what is the use of a book,' thought Alice\n");
    out.writeBytes("`without pictures or conversation?'\n");
    out.close();
  }

  static void cleanup(FileSystem fs, Path p) throws IOException {
    fs.delete(p, true);
    assertFalse("output not cleaned up", fs.exists(p));
  }

  static void runProgram(HamaConfiguration conf, Path program, Path inputPath,
      Path outputPath, int numBspTasks, int numOfGroom, String[] expectedResults)
      throws IOException {

    BSPJob bsp = new BSPJob(conf);
    bsp.setJobName("Test Hama Pipes " + program);
    bsp.setBspClass(PipesBSP.class);

    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(Text.class);

    bsp.setOutputPath(OUTPUT_PATH);

    BSPJobClient jobClient = new BSPJobClient(conf);
    conf.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 6000);

    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(numOfGroom, cluster.getGroomServers());
    bsp.setNumBspTask(numBspTasks);

    Path testExec = new Path("testing/bin/application");

    FileSystem fs = FileSystem.get(conf);
    fs.delete(testExec.getParent(), true);
    fs.copyFromLocalFile(program, testExec);

    Submitter.setExecutable(conf, fs.makeQualified(testExec).toString());

    Submitter.setIsJavaRecordReader(conf, true);
    Submitter.setIsJavaRecordWriter(conf, true);

    FileInputFormat.setInputPaths(bsp, inputPath);
    FileOutputFormat.setOutputPath(bsp, outputPath);

    Submitter.runJob(bsp);

    // wait for job completion
    while (!bsp.isComplete()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    assertTrue("pipes job failed", bsp.isSuccessful());

    LOG.info("Client finishes execution job.");

    // check output
    FileStatus[] listStatus = fs.listStatus(OUTPUT_PATH);
    assertEquals(listStatus.length, numBspTasks);

    for (FileStatus status : listStatus) {
      if (!status.isDir()) {

        /*
         * SequenceFile.Reader reader = new SequenceFile.Reader(fileSys,
         * status.getPath(), conf); IntWritable key = new IntWritable(); Text
         * value = new Text(); while (reader.next(key, value)) {
         * assertEquals(superStep, key.get()); } reader.close();
         */
        if (status.getLen() > 0) {
          System.out.println("Output File: " + status.getPath());
          FSDataInputStream in = fs.open(status.getPath());
          IOUtils.copyBytes(in, System.out, conf, false);
          in.close();
        }

        // TODO
        // assertEquals("pipes program " + program + " output " + i + " wrong",
        // expectedResults[i], results.get(i));

      }
    }
    // delete output_path
    // fs.delete(OUTPUT_PATH, true);
  }
}
