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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe consistent hash ring for mapping fragment fingerprints to executors.
 *
 * <p>Each executor is replicated as {@code virtualNodes} virtual nodes on the ring. A key (fragment
 * fingerprint) is hashed to a slot, then the ring is walked clockwise to find the first executor.
 * When executors are added/removed, only keys that hash to affected slots are redistributed.
 *
 * @param <T> node type (typically an executor identifier)
 */
public final class LanceConsistentHash<T> {

  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final SortedMap<Long, T> ring = new TreeMap<>();
  private final Map<T, Set<Long>> nodeSlots = new HashMap<>();
  private final int virtualNodes;

  public LanceConsistentHash(int virtualNodes) {
    if (virtualNodes <= 0) {
      throw new IllegalArgumentException("virtualNodes must be positive, got " + virtualNodes);
    }
    this.virtualNodes = virtualNodes;
  }

  public void addNode(T node) {
    lock.writeLock().lock();
    try {
      if (nodeSlots.containsKey(node)) return;
      Set<Long> slots = new HashSet<>();
      for (int i = 0; i < virtualNodes; i++) {
        long slot = hash(node.toString() + "#" + i);
        ring.put(slot, node);
        slots.add(slot);
      }
      nodeSlots.put(node, slots);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void removeNode(T node) {
    lock.writeLock().lock();
    try {
      Set<Long> slots = nodeSlots.remove(node);
      if (slots != null) {
        for (Long slot : slots) {
          ring.remove(slot);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public List<T> allocateNodes(String key, int count) {
    lock.readLock().lock();
    try {
      List<T> result = new ArrayList<>();
      if (key == null || count <= 0 || ring.isEmpty()) return result;
      if (count >= nodeSlots.size()) {
        result.addAll(nodeSlots.keySet());
        return result;
      }
      long slot = hash(key);
      SortedMap<Long, T> tail = ring.tailMap(slot);
      SortedMap<Long, T> head = ring.headMap(slot);
      Iterator<T> it = new RingIterator<>(tail, head);
      Set<T> seen = new HashSet<>();
      while (it.hasNext() && seen.size() < count) {
        T node = it.next();
        if (seen.add(node)) {
          result.add(node);
        }
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  public int nodeCount() {
    lock.readLock().lock();
    try {
      return nodeSlots.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  private static final ThreadLocal<MessageDigest> DIGEST_TL =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException("SHA-256 not available", e);
            }
          });

  private static long hash(String key) {
    MessageDigest md = DIGEST_TL.get();
    md.reset();
    byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
    return ((long) (digest[0] & 0xFF))
        | ((long) (digest[1] & 0xFF) << 8)
        | ((long) (digest[2] & 0xFF) << 16)
        | ((long) (digest[3] & 0xFF) << 24)
        | ((long) (digest[4] & 0xFF) << 32)
        | ((long) (digest[5] & 0xFF) << 40)
        | ((long) (digest[6] & 0xFF) << 48)
        | ((long) (digest[7] & 0xFF) << 56);
  }

  private static class RingIterator<T> implements Iterator<T> {
    private final Iterator<T> tailIt;
    private final Iterator<T> fullIt;
    private boolean useFull = false;

    RingIterator(SortedMap<Long, T> tail, SortedMap<Long, T> full) {
      this.tailIt = tail.values().iterator();
      this.fullIt = full.values().iterator();
    }

    @Override
    public boolean hasNext() {
      return useFull ? fullIt.hasNext() : (tailIt.hasNext() || fullIt.hasNext());
    }

    @Override
    public T next() {
      if (!useFull && tailIt.hasNext()) {
        return tailIt.next();
      }
      useFull = true;
      return fullIt.next();
    }
  }
}
