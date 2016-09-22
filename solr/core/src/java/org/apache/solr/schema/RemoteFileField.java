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
package org.apache.solr.schema;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.function.FileFloatSource;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteFileField extends ExternalFileField {
  private AtomicReference<List<SchemaField>> remoteFileFields = new AtomicReference<List<SchemaField>>(Collections.emptyList());
 
  @Override
  public void inform(IndexSchema schema) {
    super.inform(schema);
    // TODO does this get call on schema reload? -- verify this behavior
    
    remoteFileFields.set(rffs(schema));
    
    downloadAll();
  }
  
  private void downloadAll() {
    for (SchemaField field: remoteFileFields.get()) {
      try {
        downloadFile(schema, field);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to download remote file for '" + field.getName() + "'");
      }
    }
  }
  
  public static List<SchemaField> rffs(IndexSchema schema) {
    List<SchemaField> next = new ArrayList<SchemaField>();
    
    for (SchemaField field : schema.getFields().values()) {
      FieldType type = field.getType();
      if (type instanceof RemoteFileField) {
        next.add(field);
      }
    }
      
    return next;
  }
  
  private void downloadFile(IndexSchema schema, SchemaField sf) throws Exception {
    String surl = (String) sf.getArgs().get("url");
    
    if(null == surl) {
      throw new RuntimeException("No url specified for field '" + sf.getName() + "'");
    }
    
    URL url = new URL(surl);
    
    try(FileOutputStream os = new FileOutputStream(externalFieldFile(schema, sf));
        InputStream is = url.openStream();)
    {
      IOUtils.copy(is, os);
    }
  }
  
  private File externalFieldFile(IndexSchema schema, SchemaField sf) {
    return Paths.get(schema.getResourceLoader().getDataDir(), "external_" + sf.getName()).toFile();
  }

  public static class DownloadRemoteFilesRequestHandler extends RequestHandlerBase implements SolrCoreAware {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private String path;
    
    
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception {
      
      CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
      boolean zkEnabled = coreDesc.getCoreContainer().isZooKeeperAware();
      
      if( !zkEnabled || false == req.getParams().getBool(CommonParams.DISTRIB, true) ) {
        
        for(SchemaField sf: rffs(req.getSchema())) {
          ((RemoteFileField)sf.getType()).downloadAll();
        }
        
        return;
      }
      
      //TODO null check?
      List<Node> nodes = getCollectionUrls(req, coreDesc.getCloudDescriptor().getCollectionName());

      UpdateShardHandler handler = coreDesc.getCoreContainer().getUpdateShardHandler();
      
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.DISTRIB, false);
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          coreDesc.getCoreContainer().getZkController().getBaseUrl(), 
          req.getCore().getName()));
      
      // TODO make concurrent
      for(Node n: nodes) {
        submit(handler, n, params);
      }

      //And then reset the external field cache and commit -- DEFAULT NO
      if(req.getParams().getBool(UpdateRequestHandler.COMMIT, false)) {
        FileFloatSource.resetCache();
        log.debug("readerCache has been reset.");
  
        UpdateRequestProcessor processor =
          req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
        try{
          RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
        }
        finally{
          processor.finish();
        }
      }
      
    }

    @Override
    public String getDescription() {
      return "Download remote files request handler";
    }
    
    private void submit(UpdateShardHandler uhandler, Node node, ModifiableSolrParams params) {
      try (HttpSolrClient client = new HttpSolrClient.Builder(node.getUrl()).withHttpClient(uhandler.getHttpClient()).build()) {
        client.request(new GenericSolrRequest(METHOD.POST, path, params));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed synchronous update on shard " + node, e);
      }
    }
    
    private List<Node> getCollectionUrls(SolrQueryRequest req, String collection) {
      ClusterState clusterState = req.getCore().getCoreDescriptor()
          .getCoreContainer().getZkController().getClusterState();
      
      Map<String,Slice> slices = Optional.ofNullable(clusterState.getCollectionsMap().get(collection)).map(c -> c.getSlicesMap()).orElse(null);
      
      if (slices == null) {
        throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
            "Could not find collection in zk: " + clusterState);
      }
      
      final List<Node> urls = new ArrayList<>(slices.size());
      for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
        Slice replicas = slices.get(sliceEntry.getKey());

        Map<String,Replica> shardMap = replicas.getReplicasMap();
        
        for (Entry<String,Replica> entry : shardMap.entrySet()) {
          ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
          if (clusterState.liveNodesContain(nodeProps.getNodeName())) {
            urls.add(new StdNode(nodeProps, collection, replicas.getName()));
          }
        }
      }
      if (urls.isEmpty()) {
        return null;
      }
      return urls;
    }

    @Override
    public void inform(SolrCore core) {
      // Find the registered path of the handler
      path = null;
      for (Map.Entry<String, PluginBag.PluginHolder<SolrRequestHandler>> entry : core.getRequestHandlers().getRegistry().entrySet()) {
        if (core.getRequestHandlers().isLoaded(entry.getKey()) && entry.getValue().get() == this) {
          path = entry.getKey();
          break;
        }
      }
      if (path == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "The DownloadRemoteFilesRequestHandler is not registered with the current core.");
      }
      if (!path.startsWith("/")) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "The DownloadRemoteFilesRequestHandler needs to be registered to a path.");
      }
    }
  }

}
