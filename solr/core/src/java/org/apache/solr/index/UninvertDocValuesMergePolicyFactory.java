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
package org.apache.solr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.uninverting.UninvertingReader;

public class UninvertDocValuesMergePolicyFactory extends WrapperMergePolicyFactory {
  
  public UninvertDocValuesMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were "+args+" but "+getClass().getSimpleName()+" takes no arguments.");
    }
  }

  @Override
  protected MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
    return new OneMergeWrappingMergePolicy(wrappedMP, (merge) -> new UninvertDocValuesOneMerge(merge.segments));
  }
  
  private boolean fieldNeedsUninvertedDocValues(FieldInfo fi) {
    SchemaField sf = schema.getFieldOrNull(fi.name);
    
    return null != sf && 
           sf.hasDocValues() &&
           fi.getDocValuesType() == DocValuesType.NONE && 
           fi.getIndexOptions() != IndexOptions.NONE;

  }
    
  private class UninvertDocValuesOneMerge extends OneMerge {

    public UninvertDocValuesOneMerge(List<SegmentCommitInfo> segments) {
      super(segments);
    }
    
    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
      // Wrap the reader with an uninverting reader if any of the fields have no docvalues but the 
      // Schema says there should be
      
      
      Map<String,UninvertingReader.Type> uninversionMap = new HashMap<>();
      
      for(FieldInfo fi: reader.getFieldInfos()) {
        if(fieldNeedsUninvertedDocValues(fi)) {
          SchemaField sf = schema.getFieldOrNull(fi.name);
          uninversionMap.put(fi.name, sf.getType().getUninversionType(sf));
        }
        
      }
      
      if(uninversionMap.isEmpty()) {
        return reader; // Default to normal reader if nothing to uninvert
      } else {
        return new UninvertingFilterCodecReader(reader, uninversionMap);
      }
      
    }
    
  }
  
  
  /**
   * Delegates to an Uninverting for fields with docvalues
   * 
   * This is going to blow up FieldCache, look into an alternative implementation that uninverts without
   * fieldcache
   */
  private class UninvertingFilterCodecReader extends FilterCodecReader {
    private final UninvertingReader uninvertingReader;
    
    private final DocValuesProducer docValuesProducer = new DocValuesProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {  
        return uninvertingReader.getNumericDocValues(field.name);
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return uninvertingReader.getBinaryDocValues(field.name);
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedDocValues(field.name);
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedNumericDocValues(field.name);
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedSetDocValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
    
    public UninvertingFilterCodecReader(CodecReader in, Map<String,UninvertingReader.Type> uninversionMap) {
      super(in);

      this.uninvertingReader = new UninvertingReader(in, uninversionMap);
    }
    
    @Override
    public DocValuesProducer getDocValuesReader() {
      return docValuesProducer;
    }
    
    @Override
    public FieldInfos getFieldInfos() {
      return uninvertingReader.getFieldInfos();
    }
    
  }

}
