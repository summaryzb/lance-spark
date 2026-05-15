/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Driver-side singleton that uses consistent hashing to deterministically map fragment fingerprints
 * to executors. Unlike the RPC-based approach, this requires no runtime feedback — the same
 * fingerprint always maps to the same executor as long as the executor set is stable.
 *
 * <p>Executor lifecycle is tracked via a SparkListener (registered in {@code LanceScanBuilder}).
 * When executors are added/removed, the hash ring rebalances with minimal key redistribution.
 *
 * <p>The location format is {@code executor_{host}_{executorId}}, recognized by Spark's
 * DAGScheduler as {@code ExecutorCacheTaskLocation} for PROCESS_LOCAL scheduling.
 */
public final class LanceSoftAffinityManager {
  private static final Logger LOG = LoggerFactory.getLogger(LanceSoftAffinityManager.class);

  private static final int DEFAULT_VIRTUAL_NODES = 100;

  private static volatile LanceSoftAffinityManager instance;

  private final LanceConsistentHash<ExecutorNode> hashRing;
  private final java.util.concurrent.ConcurrentHashMap<String, String> executorHosts =
      new java.util.concurrent.ConcurrentHashMap<>();

  private LanceSoftAffinityManager(int virtualNodes) {
    this.hashRing = new LanceConsistentHash<>(virtualNodes);
  }

  public static LanceSoftAffinityManager getInstance() {
    LanceSoftAffinityManager local = instance;
    if (local == null) {
      synchronized (LanceSoftAffinityManager.class) {
        local = instance;
        if (local == null) {
          local = new LanceSoftAffinityManager(DEFAULT_VIRTUAL_NODES);
          instance = local;
        }
      }
    }
    return local;
  }

  static void resetForTesting() {
    synchronized (LanceSoftAffinityManager.class) {
      instance = null;
    }
  }

  public synchronized void onExecutorAdded(String executorId, String host) {
    ExecutorNode node = new ExecutorNode(executorId, host);
    hashRing.addNode(node);
    executorHosts.put(executorId, host);
    LOG.info("Executor added to affinity ring: {} (total: {})", node, hashRing.nodeCount());
  }

  public synchronized void onExecutorRemoved(String executorId, String host) {
    ExecutorNode node = new ExecutorNode(executorId, host);
    hashRing.removeNode(node);
    executorHosts.remove(executorId);
    LOG.info("Executor removed from affinity ring: {} (total: {})", node, hashRing.nodeCount());
  }

  public synchronized void onExecutorRemovedById(String executorId) {
    String host = executorHosts.remove(executorId);
    if (host != null) {
      hashRing.removeNode(new ExecutorNode(executorId, host));
      LOG.info(
          "Executor removed from affinity ring: {}@{} (total: {})",
          executorId,
          host,
          hashRing.nodeCount());
    } else {
      LOG.debug("Executor {} not found in affinity ring on removal", executorId);
    }
  }

  public String[] getPreferredLocations(String fragmentFingerprint) {
    if (hashRing.nodeCount() == 0) {
      return new String[0];
    }
    List<ExecutorNode> nodes = hashRing.allocateNodes(fragmentFingerprint, 1);
    if (nodes.isEmpty()) {
      return new String[0];
    }
    ExecutorNode n = nodes.get(0);
    return new String[] {"executor_" + n.host + "_" + n.executorId};
  }

  public int executorCount() {
    return hashRing.nodeCount();
  }

  static final class ExecutorNode {
    final String executorId;
    final String host;

    ExecutorNode(String executorId, String host) {
      this.executorId = executorId;
      this.host = host;
    }

    @Override
    public String toString() {
      return executorId + "@" + host;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ExecutorNode)) return false;
      ExecutorNode that = (ExecutorNode) o;
      return executorId.equals(that.executorId) && host.equals(that.host);
    }

    @Override
    public int hashCode() {
      return Objects.hash(executorId, host);
    }
  }
}
