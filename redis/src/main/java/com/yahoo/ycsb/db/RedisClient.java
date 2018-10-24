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
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import redis.clients.jedis.Jedis;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import java.util.Map.Entry;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Protocol;
import java.util.Properties;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private Jedis jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";

  public static final String INDEX = "indices";
  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    jedis = new Jedis(host, port);
    jedis.connect();

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      jedis.auth(password);
    }
  }

  public void cleanup() throws DBException {
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

// XXX jedis.select(int index) to switch to `table`
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      String url = "https://us-central1-serverless-functions-217415.cloudfunctions.net/function-1?table=user&key="+key;
//      if(fields!=null){
//        for (String field: fields){
//          url+="fields="+field+"&";
//       }
//      url = url.substring(0, url.length()-1);     
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
        Map<String, Map<String, String>> object=new HashMap<String, Map<String, String>>();
        object=mapper.readValue(inputLine, new TypeReference<Map<String, Map<String, String>>>(){});
        System.out.println(object.get(key));
        Map<String, String> object2=new HashMap<String, String>();
        object2=object.get("Message");
        for (Map.Entry<String, String> entry: object2.entrySet()){
          object2.put(entry.getKey(), entry.getValue());
        }
        StringByteIterator.putAllAsByteIterators(result, object2);
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
      String url = "https://us-central1-serverless-functions-217415.cloudfunctions.net/function-1/insert";
      String payload = "{\"table\":\"User\", \"key\":\""+key+"\", ";
      for (Map.Entry<String, String> field : map.entrySet()){
        payload+="\""+field.getKey()+"\":\""+URLEncoder.encode(field.getValue(), "UTF-8")+"\", ";
      }
      payload = payload.substring(0, payload.length()-2);
      payload+="}";
      System.out.println(payload);
      System.out.println(url);
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setDoOutput(true);
      con.setRequestProperty("Content-Type", "application/json");
      con.setRequestProperty("Accept", "application/json");
      con.setRequestMethod("POST");
      con.connect();
      byte[] outputBytes = payload.getBytes("UTF-8");
      OutputStream os = con.getOutputStream();
      os.write(outputBytes);
      os.close();
      con.getResponseCode();
      jedis.zadd(INDEX, hash(key), key);
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      String url = "https://us-central1-serverless-functions-217415.cloudfunctions.net/function-1/delete?table=user&key="+key;
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
    return jedis.zrem(INDEX, key) == 0 ?  Status.ERROR
       : Status.OK;

  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> map = StringByteIterator.getStringMap(values);
      String url = "https://us-central1-serverless-functions-217415.cloudfunctions.net/function-1/update?table=user&key="+key+"&";
      for (Map.Entry<String, String> field : map.entrySet()){
        url+=field.getKey()+"="+URLEncoder.encode(field.getValue(), "UTF-8")+"&";
      }
      url = url.substring(0, url.length()-1);
      URL obj;
      HttpURLConnection con = null;
      obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();
      con.setDoOutput(true);
      con.setRequestProperty("Content-Type", "application/json");
      con.setRequestProperty("Accept", "application/json");
con.setRequestMethod("POST");
      con.connect();
      con.getResponseCode();
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
      String url = "https://us-central1-serverless-functions-217415.cloudfunctions.net/function-1/scan?table=user&key="+startkey;
      url+="&recordCount="+recordcount;
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
        Map<String, Map<String, String>> object=new HashMap<String, Map<String, String>>();
        object=mapper.readValue(inputLine, new TypeReference<Map<String, Map<String, String>>>(){});
        Map<String, String> object2=new HashMap<String, String>();
        object2=object.get("Message");
        for (Map.Entry<String, String> entry: object2.entrySet()){
          object2.put(entry.getKey(), entry.getValue());
        }
        result.addElement((HashMap<String, ByteIterator>) StringByteIterator.getByteIteratorMap(object2));
      }
      in.close();
    }catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

}




                                                                                                                                                                


                                                                                                                                                                

