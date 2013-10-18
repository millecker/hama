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
package org.apache.hama.bsp.gpu;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.pipes.PipesApplicable;
import org.apache.hama.pipes.PipesApplication;

import edu.syr.pcpratts.rootbeer.runtime.Rootbeer;

/**
 * This class provides an abstract implementation of the {@link BSP} and
 * {@link BSPGpuInterface}.
 */
public abstract class HybridBSP<K1, V1, K2, V2, M extends Writable> extends
    BSP<K1, V1, K2, V2, M> implements BSPGpuInterface<K1, V1, K2, V2, M>,
    PipesApplicable {

  protected PipesApplication<K1, V1, K2, V2, M> pipesApplication;

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract void bspGpu(BSPPeer<K1, V1, K2, V2, M> peer, Rootbeer rootbeer)
      throws IOException, SyncException, InterruptedException;

  /**
   * {@inheritDoc}
   */
  @Override
  public void setupGpu(BSPPeer<K1, V1, K2, V2, M> peer, Rootbeer rootbeer)
      throws IOException, SyncException, InterruptedException {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanupGpu(BSPPeer<K1, V1, K2, V2, M> peer, Rootbeer rootbeer)
      throws IOException {

  }

  @Override
  public void setApplication(
      PipesApplication<?, ?, ?, ?, ? extends Writable> pipesApplication) {
    this.pipesApplication = (PipesApplication<K1, V1, K2, V2, M>) pipesApplication;
  }

  public Rootbeer start(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      InterruptedException {

    Map<String, String> env = pipesApplication.setupEnvironment(peer
        .getConfiguration());

    pipesApplication.startServer(peer);

    return new Rootbeer(env);
  }
}
