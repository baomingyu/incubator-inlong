/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.reco.util;

import org.apache.inlong.reco.pojo.Response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class HttpUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

    private static final Gson GSON = new GsonBuilder().registerTypeAdapterFactory(ResponseTypeAdaptor.FACTORY).create();

    public static Response<Boolean> checkAttaAuditFromLedger(String baseUrl, String attaId,
            String partition, String type) throws Exception {
        String url = baseUrl + "/" + attaId + "/audits";
        LOGGER.info("=== checkAttaAuditFromLedger url: " + url + ",partition=" + partition + ",type=" + type);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("partition", partition));
        params.add(new BasicNameValuePair("type", type));
        URI uri = new URIBuilder(url).addParameters(params).build();
        HttpGet httpGet = new HttpGet(uri);
        HttpClient httpClient = getHttpClient(url);
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new Exception("http result is null");
        }
        String infoStr = EntityUtils.toString(entity, "UTF-8");
        Response<Boolean> rp = GSON.fromJson(infoStr, Response.class);
        return rp;
    }

    public static HttpClient getHttpClient(String url) throws Exception {

        if (StringUtils.isNotEmpty(url) && url.toLowerCase().startsWith("https")) {
            SSLContext sslcontext = SSLContexts.custom().useSSL().build();
            sslcontext.init(null, new X509TrustManager[]{new HttpsTrustManager()}, new SecureRandom());
            SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslcontext,
                    SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
            return HttpClients.custom().setSSLSocketFactory(factory).build();
        }
        return HttpClients.createDefault();
    }
}
