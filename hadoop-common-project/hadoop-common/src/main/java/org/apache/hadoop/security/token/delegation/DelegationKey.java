/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.security.token.delegation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.CausynthSymbolicSource;

/**
 * Key used for generating and verifying delegation tokens
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DelegationKey implements Writable {
  private int keyId;
  private long expiryDate;
  @Nullable
  private byte[] keyBytes = null;
  private static final int MAX_KEY_LEN = 1024 * 1024;

  /** Default constructore required for Writable */
  public DelegationKey() {
    this(0, 0L, (SecretKey)null);
  }

  public DelegationKey(int keyId, long expiryDate, SecretKey key) {
    this(keyId, expiryDate, key != null ? key.getEncoded() : null);
  }
  
  public DelegationKey(int keyId, long expiryDate, byte[] encodedKey) {
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    if (encodedKey != null) {
      if (encodedKey.length > MAX_KEY_LEN) {
        throw new RuntimeException("can't create " + encodedKey.length +
            " byte long DelegationKey.");
      }
      this.keyBytes = encodedKey;
    }
  }

  public int getKeyId() {
    return keyId;
  }

  /**
   * Declares the current HDFS block-key id as an exact symbolic source and
   * preserves the owning map's key/object invariant after replay writeback.
   * Outside a GraphChecker session the declaration is a no-op.
   */
  public <T extends DelegationKey> boolean
      symbolizeCausynthHdfs11741CurrentKeyId(Map<Integer, T> keys) {
    int previousId = keyId;
    T indexed = keys == null ? null : keys.get(previousId);
    if (indexed != this) {
      return false;
    }
    boolean symbolized = CausynthSymbolicSource.symbolize(
        "HDFS11741.NAMENODE.CURRENT_KEY_ID", this,
        "<org.apache.hadoop.security.token.delegation.DelegationKey: int keyId>");
    if (!symbolized) {
      keyId = previousId;
      return false;
    }
    if (keyId == previousId) {
      return true;
    }
    T collision = keys.get(keyId);
    if (collision != null && collision != this) {
      keyId = previousId;
      CausynthSymbolicSource.reject(
          "HDFS11741.NAMENODE.CURRENT_KEY_ID",
          "modeled keyId collides with another key");
      return false;
    }
    keys.remove(previousId);
    keys.put(keyId, indexed);
    return true;
  }

  /** Declares the expiry of the exact NameNode map member selected by K. */
  public boolean symbolizeCausynthHdfs11741NameNodeExpiryDate() {
    long previousExpiry = expiryDate;
    boolean symbolized = CausynthSymbolicSource.symbolize(
        "HDFS11741.NAMENODE.SELECTED_KEY_EXPIRY", this,
        "<org.apache.hadoop.security.token.delegation.DelegationKey: long expiryDate>");
    if (!symbolized) {
      expiryDate = previousExpiry;
    }
    return symbolized;
  }

  /** Declares the expiry of the exact DataNode map member selected by K. */
  public boolean symbolizeCausynthHdfs11741DataNodeExpiryDate() {
    long previousExpiry = expiryDate;
    boolean symbolized = CausynthSymbolicSource.symbolize(
        "HDFS11741.DATANODE.SELECTED_KEY_EXPIRY", this,
        "<org.apache.hadoop.security.token.delegation.DelegationKey: long expiryDate>");
    if (!symbolized) {
      expiryDate = previousExpiry;
    }
    return symbolized;
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public SecretKey getKey() {
    if (keyBytes == null || keyBytes.length == 0) {
      return null;
    } else {
      SecretKey key = AbstractDelegationTokenSecretManager.createSecretKey(keyBytes);
      return key;
    }
  }
  
  public byte[] getEncodedKey() {
    return keyBytes;
  }

  public void setExpiryDate(long expiryDate) {
    this.expiryDate = expiryDate;
  }

  /**
   */
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeVLong(out, expiryDate);
    if (keyBytes == null) {
      WritableUtils.writeVInt(out, -1);
    } else {
      WritableUtils.writeVInt(out, keyBytes.length);
      out.write(keyBytes);
    }
  }

  /**
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    keyId = WritableUtils.readVInt(in);
    expiryDate = WritableUtils.readVLong(in);
    int len = WritableUtils.readVIntInRange(in, -1, MAX_KEY_LEN);
    if (len == -1) {
      keyBytes = null;
    } else {
      keyBytes = new byte[len];
      in.readFully(keyBytes);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (expiryDate ^ (expiryDate >>> 32));
    result = prime * result + Arrays.hashCode(keyBytes);
    result = prime * result + keyId;
    return result;
  }

  @Override
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      DelegationKey r = (DelegationKey) right;
      return keyId == r.keyId &&
             expiryDate == r.expiryDate &&
             Arrays.equals(keyBytes, r.keyBytes);
    }
  }

}
