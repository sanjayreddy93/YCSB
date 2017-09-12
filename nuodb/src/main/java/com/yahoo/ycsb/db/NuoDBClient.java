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
 * NuoDB client binding for YCSB.
 * 
 * Note: Here all YCSB records are mapped to *hash fields.*
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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


/**
 * YCSB binding for <a href="http://nuodb.com/">NuoDB</a>.
 *
 * See {@code nuodb/README.md} for details.
 */
public class NuoDBClient extends DB {

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  //disconnect
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

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      return Status.OK;
  }

 
}
