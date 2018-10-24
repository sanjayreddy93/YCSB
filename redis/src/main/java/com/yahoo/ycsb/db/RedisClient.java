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
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector


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

  public static final String INDEX_KEY = "_indices";
  public void init() throws DBException {
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
      String url = "https://o9doeirora.execute-api.eu-central-1.amazonaws.com/default/serverlessfunct/read/Log?SvId=primary&type=log&table=User&key="+key+"&";
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
      String url = "https://o9doeirora.execute-api.eu-central-1.amazonaws.com/default/serverlessfunct/insert?table=User&key="+key+"&";
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
      jedis.zadd(INDEX_KEY, hash(key), key);
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      String url = "https://o9doeirora.execute-api.eu-central-1.amazonaws.com/default/serverlessfunct/delete?table=User&key="+key;
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
    return jedis.zrem(INDEX_KEY, key) == 0 ?  Status.ERROR
       : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> map = StringByteIterator.getStringMap(values);
      String url = "https://o9doeirora.execute-api.eu-central-1.amazonaws.com/default/serverlessfunct/update?table=User&key="+key+"&";
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
      String url = "https://o9doeirora.execute-api.eu-central-1.amazonaws.com/default/serverlessfunct/scanLog?table=User&key=";
      url+=startkey+"&recordCount="+recordcount+"&";
      Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
      Double.POSITIVE_INFINITY, 0, recordcount);
      HashMap<String, ByteIterator> values;
      for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
   } 
    }catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;

}

