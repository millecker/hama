/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

public class TestPartitioning extends HamaCluster {

  public static final Log LOG = LogFactory.getLog(TestPartitioning.class);

  public static String TMP_OUTPUT = "/tmp/test-example/";
  public static final String TMP_OUTPUT_PATH = "/tmp/test-example/output.txt";
  public static Path OUTPUT_PATH = new Path(TMP_OUTPUT);

  protected HamaConfiguration configuration;

  // these variables are preventing from rebooting the whole stuff again since
  // setup and teardown are called per method.

  public TestPartitioning() {
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

  public void testPartitioner() throws Exception {

    Configuration conf = new Configuration();
    conf.set("bsp.local.dir", "/tmp/hama-test/partitioning");
    conf.setBoolean("bsp.input.runtime.partitioning", true);
    BSPJob bsp = new BSPJob(new HamaConfiguration(conf));
    bsp.setJobName("Test partitioning with input");
    bsp.setBspClass(PartionedBSP.class);
    bsp.setNumBspTask(2);
    conf.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 600);
    bsp.setInputFormat(TextInputFormat.class);
    bsp.setOutputFormat(NullOutputFormat.class);
    FileInputFormat.setInputPaths(bsp, "../CHANGES.txt,../README.txt");
    bsp.setPartitioner(HashPartitioner.class);
    assertTrue(bsp.waitForCompletion(true));

    FileSystem fs = FileSystem.get(conf);
    fs.delete(OUTPUT_PATH, true);
    
    getMergeProcessorID();
  }

  public void getMergeProcessorID() {
    int peerNum = 6;
    for (int partitionID = 0; partitionID < 8; partitionID++) {
      assertTrue(PartitioningRunner.getMergeProcessorID(partitionID, peerNum) < peerNum);
    }
  }

  public static class PartionedBSP extends
      BSP<LongWritable, Text, NullWritable, NullWritable, NullWritable> {

    @Override
    public void bsp(
        BSPPeer<LongWritable, Text, NullWritable, NullWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      long numOfPairs = 0;
      KeyValuePair<LongWritable, Text> readNext = null;
      while ((readNext = peer.readNext()) != null) {
        LOG.debug(readNext.getKey().get() + " / "
            + readNext.getValue().toString());
        numOfPairs++;
      }

      assertTrue(numOfPairs > 2);
    }
  }

}
