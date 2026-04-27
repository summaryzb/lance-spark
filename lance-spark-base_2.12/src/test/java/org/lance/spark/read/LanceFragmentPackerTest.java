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
package org.lance.spark.read;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceFragmentPackerTest {

  private static LanceSplit single(int fragId) {
    return new LanceSplit(Collections.singletonList(fragId));
  }

  private static List<LanceSplit> singletons(int... fragIds) {
    List<LanceSplit> result = new ArrayList<>(fragIds.length);
    for (int id : fragIds) {
      result.add(single(id));
    }
    return result;
  }

  private static Map<Integer, Long> sizes(long... sizes) {
    Map<Integer, Long> result = new HashMap<>();
    for (int i = 0; i < sizes.length; i++) {
      result.put(i, sizes[i]);
    }
    return result;
  }

  @Test
  void emptyInputReturnsEmpty() {
    List<LanceSplit> packed =
        LanceFragmentPacker.packFragmentsIntoSplits(
            Collections.emptyList(), Collections.emptyMap(), 1024L, 16L);
    assertTrue(packed.isEmpty());
  }

  @Test
  void allSmallFragmentsMergeIntoOneSplit() {
    // 4 fragments of 100 bytes each; maxSplitBytes = 1000, openCost = 10.
    // Per fragment cost = 110, 4*110 = 440 < 1000 => single split.
    List<LanceSplit> input = singletons(0, 1, 2, 3);
    Map<Integer, Long> sizes = sizes(100L, 100L, 100L, 100L);
    List<LanceSplit> packed = LanceFragmentPacker.packFragmentsIntoSplits(input, sizes, 1000L, 10L);
    assertEquals(1, packed.size());
    assertEquals(4, packed.get(0).getFragments().size());
  }

  @Test
  void oversizedFragmentGetsOwnSplit() {
    // Fragment 0 is larger than maxSplitBytes. NFD still emits it as a singleton split.
    List<LanceSplit> input = singletons(0, 1, 2);
    Map<Integer, Long> sizes = sizes(5000L, 100L, 100L);
    List<LanceSplit> packed = LanceFragmentPacker.packFragmentsIntoSplits(input, sizes, 1000L, 10L);
    // First (largest) fragment alone in a split; smaller two fit in the next split.
    assertEquals(2, packed.size());
    assertEquals(Arrays.asList(0), packed.get(0).getFragments());
    assertTrue(packed.get(1).getFragments().containsAll(Arrays.asList(1, 2)));
    assertEquals(2, packed.get(1).getFragments().size());
  }

  @Test
  void nfdOrderingDescending() {
    // Sizes: 0=100, 1=900, 2=800, 3=50. After descending sort: 1, 2, 0, 3.
    // maxSplitBytes=1000, openCost=0.
    //   - f1 (900): current=0, 0+900<=1000, add -> [1], size 900.
    //   - f2 (800): current=900, 900+800>1000 -> close [1], new [2], size 800.
    //   - f0 (100): current=800, 800+100<=1000, add -> [2,0], size 900.
    //   - f3 (50):  current=900, 900+50<=1000, add -> [2,0,3], size 950.
    // final: [[1], [2,0,3]]
    List<LanceSplit> input = singletons(0, 1, 2, 3);
    Map<Integer, Long> sizes = sizes(100L, 900L, 800L, 50L);
    List<LanceSplit> packed = LanceFragmentPacker.packFragmentsIntoSplits(input, sizes, 1000L, 0L);
    assertEquals(2, packed.size());
    assertEquals(Arrays.asList(1), packed.get(0).getFragments());
    assertEquals(Arrays.asList(2, 0, 3), packed.get(1).getFragments());
  }

  @Test
  void openCostInBytesForcesCloseSplit() {
    // Walk with sizes=[200,200,200,200,200], max=1000, openCost=50:
    //   - f0 (200): 0+200<=1000, add; current = 250.
    //   - f1 (200): 250+200=450<=1000, add; current = 500.
    //   - f2 (200): 500+200=700<=1000, add; current = 750.
    //   - f3 (200): 750+200=950<=1000, add; current = 1000.
    //   - f4 (200): 1000+200=1200 > 1000 -> close [f0..f3], new [f4].
    // final: one 4-fragment split + one 1-fragment split.
    List<LanceSplit> input = singletons(0, 1, 2, 3, 4);
    Map<Integer, Long> sizes = sizes(200L, 200L, 200L, 200L, 200L);
    List<LanceSplit> packed = LanceFragmentPacker.packFragmentsIntoSplits(input, sizes, 1000L, 50L);
    assertEquals(2, packed.size());
    assertEquals(4, packed.get(0).getFragments().size());
    assertEquals(1, packed.get(1).getFragments().size());
  }
}
