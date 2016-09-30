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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.ext.servlet.ServerServlet;

public class RemoteFileFieldTest extends RestTestBase {
  public static class GetExternalFile extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {   
      final String testHome = SolrTestCaseJ4.getFile("solr/collection1").getParent();
      String filename = "external_eff";
      FileInputStream is = new FileInputStream(new File(testHome + "/" + filename));
      GZIPOutputStream gzos  = new GZIPOutputStream(resp.getOutputStream());
      IOUtils.copy(is, gzos);
      
      gzos.finish();
      is.close();
    }
  }
  
  private static File tmpSolrHome;
  private static File tmpConfDir;
  
  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";
  
  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    final ServletHolder getExternalFile = new ServletHolder("external", GetExternalFile.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'
    extraServlets.put(getExternalFile, "/rfftest/*");
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-eff.xml",
                          "/solr", true, extraServlets);

  }
  
  @After
  private void after() throws Exception {
    jetty.stop();
    jetty = null;
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
    
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }
  
  static void updateExternalFile() throws IOException {
    final String testHome = SolrTestCaseJ4.getFile("solr/collection1").getParent();
    String filename = "external_eff";
    FileUtils.copyFile(new File(testHome + "/" + filename),
        new File(jetty.getCoreContainer().getCores().iterator().next().getDataDir() + "/" + filename));
  }

  private void addDocuments() {
    for (int i = 1; i <= 10; i++) {
      String id = Integer.toString(i);
      assertU("add a test doc", adoc("id", id));
    }
    assertU("commit", commit());
  }

  @Test
  public void testSort() throws Exception {
    String updateMapping = "{ managedMap: { rff: { url:\"http://127.0.0.1:" + jetty.getLocalPort() + "/solr/rfftest\", gzipped: true } } }";
    assertJPut("/schema/remote-files/rff", updateMapping, "/responseHeader/status==0");
    
    //restTestHarness.reload();
    
    assertJQ("/update-rffs", "//responseHeader/status==0");
    
    addDocuments();
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.setSort("rff", ORDER.asc);
    assertQ("/select" + q.toQueryString(),
        "//result/doc[position()=1]/str[.='3']",
        "//result/doc[position()=2]/str[.='1']",
        "//result/doc[position()=10]/str[.='8']");
  }
  
}
