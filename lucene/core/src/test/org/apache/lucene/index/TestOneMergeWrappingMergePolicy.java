/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

public class TestOneMergeWrappingMergePolicy extends BaseMergePolicyTestCase {

  public MergePolicy mergePolicy() {
    return new OneMergeWrappingMergePolicy(newMergePolicy(random()), this::wrapOneMerge);
  }

  public OneMerge wrapOneMerge(OneMerge merge) {
    return new WrappedOneMerge(merge.segments);
  }

  private static class WrappedOneMerge extends OneMerge {

    public WrappedOneMerge(List<SegmentCommitInfo> segments) {
      super(segments);
    }

  }

  public void testTestSegmentsAreWrapped() throws Exception {
    try (Directory dir = newDirectory()) {
      final MergeScheduler mergeScheduler = new SerialMergeScheduler() {
        @Override
        synchronized public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound)
            throws IOException {

          while(true) {
            OneMerge merge = writer.getNextMerge();
            if (merge == null)
              break;
            
            assertTrue("Wanted a wrapped merge", merge instanceof WrappedOneMerge);
            
            
            writer.merge(merge);
          }
          
        }
      };

      MergePolicy mp = mergePolicy();
      assumeFalse("this test cannot tolerate random forceMerges", mp.toString().contains("MockRandomMergePolicy"));
      mp.setNoCFSRatio(random().nextBoolean() ? 0 : 1);

      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergeScheduler(mergeScheduler);
      iwc.setMergePolicy(mp);

      IndexWriter writer = new IndexWriter(dir, iwc);
      final int numSegments = TestUtil.nextInt(random(), 20, 40);
      for (int i = 0; i < numSegments; ++i) {
        final int numDocs = TestUtil.nextInt(random(), 1, 5);
        for (int j = 0; j < numDocs; ++j) {
          writer.addDocument(new Document());
        }
        
        writer.getReader().close();
        
        if(i < numSegments-1) {
          writer.maybeMerge();
        }
        
      }
      
      for (int i = 5; i >= 0; --i) {
        final int segmentCount = writer.getSegmentCount();
        final int maxNumSegments = i == 0 ? 1 : TestUtil.nextInt(random(), 1, 10);
        if (VERBOSE) {
          System.out
              .println("TEST: now forceMerge(maxNumSegments=" + maxNumSegments + ") vs segmentCount=" + segmentCount);
        }
        writer.forceMerge(maxNumSegments);
        
      }
      writer.close();
    }
  }
}
