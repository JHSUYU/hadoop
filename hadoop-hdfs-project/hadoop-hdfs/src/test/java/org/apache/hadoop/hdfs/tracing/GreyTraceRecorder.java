package org.apache.hadoop.hdfs.tracing;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public final class GreyTraceRecorder {
  private static final Object LOCK = new Object();

  private GreyTraceRecorder() {
  }

  public static void record(String eventId, String eventType, String method,
      String unit) {
    String output = System.getProperty("grey.trace.output");
    if (output == null || output.trim().isEmpty()) {
      output = "target/grey-trace-events.jsonl";
    }
    File file = new File(output);
    File parent = file.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
    String test = System.getProperty("test", "");
    String line = "{"
        + "\"event_id\":\"" + escape(eventId) + "\","
        + "\"event_type\":\"" + escape(eventType) + "\","
        + "\"method\":\"" + escape(method) + "\","
        + "\"unit\":\"" + escape(unit) + "\","
        + "\"test\":\"" + escape(test) + "\""
        + "}";
    synchronized (LOCK) {
      FileWriter writer = null;
      try {
        writer = new FileWriter(file, true);
        writer.write(line);
        writer.write('\n');
      } catch (IOException ignored) {
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (IOException ignored) {
          }
        }
      }
    }
  }

  private static String escape(String value) {
    if (value == null) {
      return "";
    }
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
      case '\\':
        out.append("\\\\");
        break;
      case '"':
        out.append("\\\"");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        out.append(c);
      }
    }
    return out.toString();
  }
}
