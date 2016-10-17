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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.CorePropertiesLocator;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkAwareCoreLocator extends CorePropertiesLocator {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkAwareCoreLocator(Path coreDiscoveryRoot) {
    super(coreDiscoveryRoot);
  }
  
  @Override
  public List<CoreDescriptor> discover(final CoreContainer cc) {
    List<CoreDescriptor> localCores = super.discover(cc);
    List<CollectionSliceReplica> myRemoteCores = findMyCoresInZk(cc);
    List<CoreDescriptor> zkIsTruthCores = writeMissingLocalCoresAndGetTruth(cc, localCores, myRemoteCores);
    
    return zkIsTruthCores;
  }
 
  private List<CoreDescriptor> writeMissingLocalCoresAndGetTruth(final CoreContainer cc, List<CoreDescriptor> localCoreDescriptors, List<CollectionSliceReplica> myRemoteCores) {
    List<CoreDescriptor> zkIsTruthCores = Lists.newArrayList();
    
    for(CollectionSliceReplica csr: myRemoteCores) {
      //Find if there was already a local core descriptor
      //Stream by range so we can get the index to remove later **glorified for loop**
      OptionalInt localCoreDescriptorIndex = IntStream.range(0, localCoreDescriptors.size())
          .filter(i -> {
            return localCoreDescriptors.get(i)
                .getName()
                .equals(csr.replica.second().getCoreName());
          })
          .findFirst();
      
      //TODO: verify behavior -- if there was already a local core descriptor use it and remove it from original list
      //if one doesn't exist create it
      if(localCoreDescriptorIndex.isPresent()) {
        CoreDescriptor cd = localCoreDescriptors.remove(localCoreDescriptorIndex.getAsInt());
        logger.info("core '{}' already exists locally, using this configuration", cd.getName());
        zkIsTruthCores.add(cd);
      } else {
        zkIsTruthCores.add(writeCoreProperties(cc, csr));
      }
    }
    
    //Anything left in localCoreDescriptors wasn't found remotely so warn for these
    for(CoreDescriptor c: localCoreDescriptors) {
      logger.warn("Not starting core: '{}' because I found it locally but there is no configuration in ClusterState for my nodeName '{}'", 
          c.getName(), 
          cc.getZkController().getNodeName()
      );
    }
    
    return zkIsTruthCores;
  }
  
  private CoreDescriptor writeCoreProperties(CoreContainer cc, CollectionSliceReplica csr) {
    //TODO: we only write some properties into core.properties but what about other things like dataDir? -- how do we get this info -- do we need it?
    String coreName = csr.replica.second().getCoreName();
    
    logger.info("Found that I should be hosting core: '{}' in ClusterState but it did not exist locally. I will create it now", coreName);
    
    Path instancePath = cc.getCoreRootDirectory().resolve(coreName);
    
    Map<String, String> props = new HashMap<String, String>();
    props.put(CoreDescriptor.CORE_NAME, coreName);
    props.put(CoreDescriptor.CORE_NODE_NAME, csr.replica.first());
    props.put(CoreDescriptor.CORE_COLLECTION, csr.collection.first());
    props.put(CoreDescriptor.CORE_SHARD, csr.slice.first());
    //Is this property defined anywhere else?
    props.put(OverseerCollectionMessageHandler.COLL_CONF, cc.getZkStateReader().readConfigName(csr.collection.first()));
    
    CoreDescriptor cd = new CoreDescriptor(cc, coreName, instancePath, props);
    create(cc, cd);
    
    return cd;
  }
  

  private List<CollectionSliceReplica> findMyCoresInZk(final CoreContainer cc) {
    String myNodeName = cc.getZkController().getNodeName();
    ClusterState cs = cc.getZkStateReader().getClusterState();
    
    List<CollectionSliceReplica> remoteCores = cs.getCollectionsMap().entrySet()
      .stream()
      .flatMap(coll -> {
        return coll.getValue().getSlicesMap().entrySet()
            .stream()
            .flatMap(slice -> {
               return slice.getValue().getReplicasMap().entrySet()
                 .stream()
                 .filter(replica -> replica.getValue().getNodeName().equals(myNodeName))
                 .map(replica -> {
                   CollectionSliceReplica csr = new CollectionSliceReplica();
                   csr.collection = new Pair<>(coll.getKey(), coll.getValue());
                   csr.slice = new Pair<>(slice.getKey(), slice.getValue());
                   csr.replica = new Pair<>(replica.getKey(), replica.getValue());
                   return csr;
                 });
            });
      })
      .collect(Collectors.toList());
 
    return remoteCores;
  }
  
  private static class CollectionSliceReplica {
    public Pair<String, DocCollection> collection;
    public Pair<String, Slice> slice;
    public Pair<String, Replica> replica;
  }

}
