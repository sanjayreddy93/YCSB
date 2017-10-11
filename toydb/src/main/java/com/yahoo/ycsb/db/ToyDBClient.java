/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * ToyDB client binding for YCSB.
 *
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Set;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * YCSB binding for a toy database, ToyDB.
 *
 */
public class ToyDBClient extends DB {

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      String url = "http://localhost:8080/readLog?SvId=primary&type=log&table=User&key="+key+"&";
      if(fields!=null){
        for (String field: fields){
          url+="fields="+field+"&";
        }
      }
      url = url.substring(0, url.length()-1);     
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      ObjectMapper mapper=new ObjectMapper();
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        Map<String, String> object=new HashMap<String, String>();
        object=mapper.readValue(inputLine, new TypeReference<Map<String, String>>(){});
        for (Map.Entry<String, String> entry: object.entrySet()){
          object.put(entry.getKey(), entry.getValue());
        }
        StringByteIterator.putAllAsByteIterators(result, object);
      }
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;          
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> map = StringByteIterator.getStringMap(values);
      String url = "http://localhost:8080/insertLog?SvId=primary&type=log&table=User&key="+key+"&";
      for (Map.Entry<String, String> field : map.entrySet()){
        url+=field.getKey()+"="+URLEncoder.encode(field.getValue(), "UTF-8")+"&";
      }
      url = url.substring(0, url.length()-1);     
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      in.readLine();
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      String url = "http://localhost:8080/deleteLog?SvId=primary&type=log&table=User&key="+key;
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      in.readLine();
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> map = StringByteIterator.getStringMap(values);
      String url = "http://localhost:8080/updateLog?SvId=primary&type=log&table=User&key="+key+"&";
      for (Map.Entry<String, String> field : map.entrySet()){
        url+=field.getKey()+"="+URLEncoder.encode(field.getValue(), "UTF-8")+"&";
      }
      url = url.substring(0, url.length()-1);     
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      in.readLine();
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;          
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      String url = "http://localhost:8080/scanLog?SvId=primary&type=log&table=User&key=";
      url+=startkey+"&recordCount="+recordcount+"&";
      if (fields!=null){
        for (String field: fields){
          url+="fields="+field+"&";
        }
      }
      url = url.substring(0, url.length()-1);
      URL obj = new URL(url);
      HttpURLConnection con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");      
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      ObjectMapper mapper=new ObjectMapper();
      while ((inputLine = in.readLine()) != null) {
        List<Map<String, String>> object=new ArrayList<>();
        object=mapper.readValue(inputLine, new TypeReference<List<Map<String, String>>>(){});
        for (Map<String, String> entry2: object){
          Map<String, String> object2=new HashMap<String, String>();
          for (Map.Entry<String, String> entry: entry2.entrySet()){
            object2.put(entry.getKey(), entry.getValue());
          }
          result.addElement((HashMap<String, ByteIterator>) StringByteIterator.getByteIteratorMap(object2));
        }
      }
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }
}
