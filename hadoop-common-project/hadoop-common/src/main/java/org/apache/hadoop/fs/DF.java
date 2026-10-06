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
package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import org.apache.hadoop.classification.VisibleForTesting;

/** Filesystem disk space usage statistics.
 * Uses the JVM's file store to get mount points (GraphChecker: no 'df'
 * process), and java.io.File for space utilization. */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DF extends Shell {

  private final String dirPath;
  private final File dirFile;
  private String filesystem;
  private String mount;
  
  private ArrayList<String> output;

  public DF(File path, Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_DF_INTERVAL_DEFAULT));
  }

  public DF(File path, long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = path.getCanonicalPath();
    this.dirFile = new File(this.dirPath);
    this.output = new ArrayList<String>();
  }

  /// ACCESSORS

  /** @return the canonical path to the volume we're checking. */
  public String getDirPath() {
    return dirPath;
  }

  /**
   * @return a string indicating which filesystem volume we're checking.
   * @throws IOException raised on errors performing I/O.
   */
  public String getFilesystem() throws IOException {
    if (Shell.WINDOWS) {
      this.filesystem = dirFile.getCanonicalPath().substring(0, 2);
      return this.filesystem;
    } else {
      readFileStore();
      return filesystem;
    }
  }

  /** @return the capacity of the measured filesystem in bytes. */
  public long getCapacity() {
    return dirFile.getTotalSpace();
  }

  /** @return the total used space on the filesystem in bytes. */
  public long getUsed() {
    return dirFile.getTotalSpace() - dirFile.getFreeSpace();
  }

  /** @return the usable space remaining on the filesystem in bytes. */
  public long getAvailable() {
    return dirFile.getUsableSpace();
  }

  /** @return the amount of the volume full, as a percent. */
  public int getPercentUsed() {
    double cap = (double) getCapacity();
    double used = (cap - (double) getAvailable());
    return (int) (used * 100.0 / cap);
  }

  /**
   * @return the filesystem mount point for the indicated volume.
   * @throws IOException raised on errors performing I/O.
   */
  public String getMount() throws IOException {
    // Abort early if specified path does not exist
    if (!dirFile.exists()) {
      throw new FileNotFoundException("Specified path " + dirFile.getPath()
          + "does not exist");
    }

    if (Shell.WINDOWS) {
      // Assume a drive letter for a mount point
      this.mount = dirFile.getCanonicalPath().substring(0, 2);
    } else {
      readFileStore();
    }

    return mount;
  }

  /**
   * GraphChecker: the file system and mount point of the directory as the
   * JVM resolves them, not from a df process: each process started a stderr
   * reader thread whose timing no replay holds.  The mount point is the
   * highest ancestor still on the directory's file store; the file system
   * is the store's name, df's first column.
   */
  private void readFileStore() throws IOException {
    java.nio.file.Path dir = dirFile.toPath();
    FileStore store = Files.getFileStore(dir);
    java.nio.file.Path mountPoint = dir;
    for (java.nio.file.Path up = dir.getParent();
        up != null && Files.getFileStore(up).equals(store);
        up = up.getParent()) {
      mountPoint = up;
    }
    this.filesystem = store.name();
    this.mount = mountPoint.toString();
  }
  
  @Override
  public String toString() {
    return
      "df -k " + mount +"\n" +
      filesystem + "\t" +
      getCapacity() / 1024 + "\t" +
      getUsed() / 1024 + "\t" +
      getAvailable() / 1024 + "\t" +
      getPercentUsed() + "%\t" +
      mount;
  }

  @Override
  protected String[] getExecString() {
    // ignoring the error since the exit code it enough
    if (Shell.WINDOWS){
      throw new AssertionError(
          "DF.getExecString() should never be called on Windows");
    } else {
      return new String[] {"bash","-c","exec 'df' '-k' '-P' '" + dirPath 
                      + "' 2>/dev/null"};
    }
  }

  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    output.clear();
    String line = lines.readLine();
    while (line != null) {
      output.add(line);
      line = lines.readLine();
    }
  }
  
  @VisibleForTesting
  protected void parseOutput() throws IOException {
    if (output.size() < 2) {
      StringBuilder sb = new StringBuilder("Fewer lines of output than expected");
      if (output.size() > 0) {
        sb.append(": " + output.get(0));
      }
      throw new IOException(sb.toString());
    }
    
    String line = output.get(1);
    StringTokenizer tokens =
      new StringTokenizer(line, " \t\n\r\f%");
    
    try {
      this.filesystem = tokens.nextToken();
    } catch (NoSuchElementException e) {
      throw new IOException("Unexpected empty line");
    }
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      if (output.size() > 2) {
        line = output.get(2);
      } else {
        throw new IOException("Expecting additional output after line: "
            + line);
      }
      tokens = new StringTokenizer(line, " \t\n\r\f%");
    }

    try {
      Long.parseLong(tokens.nextToken()); // capacity
      Long.parseLong(tokens.nextToken()); // used
      Long.parseLong(tokens.nextToken()); // available
      Integer.parseInt(tokens.nextToken()); // pct used
      this.mount = tokens.nextToken();
    } catch (NoSuchElementException e) {
      throw new IOException("Could not parse line: " + line);
    } catch (NumberFormatException e) {
      throw new IOException("Could not parse line: " + line);
    }
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new DF(new File(path),
        CommonConfigurationKeysPublic.FS_DF_INTERVAL_DEFAULT).toString());
  }
}
