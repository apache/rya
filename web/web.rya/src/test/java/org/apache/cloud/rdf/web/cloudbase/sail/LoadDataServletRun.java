package org.apache.cloud.rdf.web.cloudbase.sail;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

public class LoadDataServletRun {

    public static void main(String[] args) {
        try {
            final InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("namedgraphs.trig");
            URL url = new URL("http://localhost:8080/web.rya/loadrdf" +
                    "?format=Trig" +
                    "&cv=ROSH|ANDR");
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Content-Type", "text/plain");
            urlConnection.setDoOutput(true);

            final OutputStream os = urlConnection.getOutputStream();

            int read;
            while((read = resourceAsStream.read()) >= 0) {
                os.write(read);
            }
            resourceAsStream.close();
            os.flush();

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
