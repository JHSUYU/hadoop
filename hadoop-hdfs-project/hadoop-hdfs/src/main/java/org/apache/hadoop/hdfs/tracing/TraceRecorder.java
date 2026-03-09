package org.apache.hadoop.hdfs.tracing;

import io.opentelemetry.api.baggage.Baggage;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime trace recorder injected by GraphChecker at field read/write sites
 * and starting points. Streams trace events to a file and detects cross-flow
 * state interactions.
 *
 * <p>All methods are static and safe to call from any thread. The class is
 * designed to be referenced by instrumented bytecode — the GraphChecker
 * transformer injects {@code staticinvoke} calls to these methods.
 *
 * <p>Output path is configurable via {@code -Dgraphchecker.trace.output=...}.
 */
public class TraceRecorder {

    // ── Object-level trace tracking ─────────────────────────────────────
    // Maps each object (by identity) to the set of flow keys that have
    // touched it.  Flow key format: "traceId|threadName(threadId)"
    //
    // IdentityHashMap uses reference equality (==), not .equals().
    // Strong references are fine for test-scoped JVM runs.
    private static final Map<Object, Set<String>> objectTraceMap =
            Collections.synchronizedMap(new IdentityHashMap<Object, Set<String>>());

    // ── Per-trace metadata ──────────────────────────────────────────────
    // traceId → set of "threadName(threadId)" strings
    private static final ConcurrentHashMap<String, Set<String>> traceThreads =
            new ConcurrentHashMap<>();

    // traceId → origin method signature (first starting point that created it)
    private static final ConcurrentHashMap<String, String> traceOrigins =
            new ConcurrentHashMap<>();

    // ── Output ──────────────────────────────────────────────────────────
    private static final String OUTPUT_PATH =
            System.getProperty("graphchecker.trace.output",
                    "/Users/lizhenyu/IdeaProjects/GraphChecker/experiments/hdfs/trace.txt");

    private static final PrintWriter writer;

    static {
        PrintWriter w;
        try {
            w = new PrintWriter(new BufferedWriter(new FileWriter(OUTPUT_PATH, false)), true);
        } catch (IOException e) {
            System.err.println("[TraceRecorder] Cannot open " + OUTPUT_PATH + ": " + e.getMessage());
            w = new PrintWriter(System.err, true);
        }
        writer = w;
        Runtime.getRuntime().addShutdownHook(new Thread(TraceRecorder::shutdown));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static String getTraceId() {
        try {
            return Baggage.current().getEntryValue("traceId");
        } catch (Exception e) {
            return null;
        }
    }

    private static String threadDesc() {
        Thread t = Thread.currentThread();
        return t.getName() + "(" + t.getId() + ")";
    }

    private static String flowKey(String traceId) {
        return traceId + "|" + threadDesc();
    }

    // ── Public API (called by instrumented bytecode) ────────────────────

    /**
     * Called at starting points (run/call/RPC entry) after the traceId
     * has been established in Baggage (either newly created or inherited).
     */
    public static void onTraceStart(String methodSig) {
        String traceId = getTraceId();
        if (traceId == null) return;

        String td = threadDesc();
        traceOrigins.putIfAbsent(traceId, methodSig);
        traceThreads.computeIfAbsent(traceId, k -> ConcurrentHashMap.newKeySet()).add(td);

        synchronized (writer) {
            writer.println("TRACE_START " + traceId + " " + td + " " + methodSig);
        }
    }

    /**
     * Called after an instance field write: {@code obj.field = value}.
     */
    public static void onFieldWrite(Object obj, String className, String fieldName) {
        if (obj == null) return;
        String traceId = getTraceId();
        if (traceId == null) return;

        String key = flowKey(traceId);
        String td = threadDesc();

        // Record thread participation
        traceThreads.computeIfAbsent(traceId, k -> ConcurrentHashMap.newKeySet()).add(td);

        // Add this flow to the object's trace set
        Set<String> traceSet = objectTraceMap.get(obj);
        if (traceSet == null) {
            traceSet = Collections.synchronizedSet(new HashSet<String>());
            objectTraceMap.put(obj, traceSet);
        }
        traceSet.add(key);

        synchronized (writer) {
            writer.println("FIELD_WRITE " + traceId + " " + td + " " + className + "." + fieldName);
        }
    }

    /**
     * Called after an instance field read: {@code x = obj.field}.
     * Detects cross-flow interactions when the object was previously
     * touched by a different trace.
     */
    public static void onFieldRead(Object obj, String className, String fieldName) {
        if (obj == null) return;
        String traceId = getTraceId();
        if (traceId == null) return;

        String key = flowKey(traceId);
        String td = threadDesc();

        // Record thread participation
        traceThreads.computeIfAbsent(traceId, k -> ConcurrentHashMap.newKeySet()).add(td);

        // Check for cross-flow: does this object have keys from OTHER traces?
        Set<String> crossFlowKeys = null;
        Set<String> traceSet = objectTraceMap.get(obj);
        if (traceSet != null && !traceSet.isEmpty()) {
            synchronized (traceSet) {
                for (String existing : traceSet) {
                    // Different trace if it doesn't start with "currentTraceId|"
                    if (!existing.startsWith(traceId + "|")) {
                        if (crossFlowKeys == null) crossFlowKeys = new HashSet<>();
                        crossFlowKeys.add(existing);
                    }
                }
            }
        }

        // Add this flow to the object's trace set
        if (traceSet == null) {
            traceSet = Collections.synchronizedSet(new HashSet<String>());
            objectTraceMap.put(obj, traceSet);
        }
        traceSet.add(key);

        synchronized (writer) {
            if (crossFlowKeys != null) {
                writer.println("FIELD_READ " + traceId + " " + td + " " + className + "." + fieldName
                        + " CROSS_FLOW" + crossFlowKeys);
            } else {
                writer.println("FIELD_READ " + traceId + " " + td + " " + className + "." + fieldName);
            }
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────────

    private static void shutdown() {
        synchronized (writer) {
            writer.println();
            writer.println("=== TRACE SUMMARY ===");
            for (Map.Entry<String, String> entry : traceOrigins.entrySet()) {
                String traceId = entry.getKey();
                String origin = entry.getValue();
                Set<String> threads = traceThreads.getOrDefault(traceId, Collections.<String>emptySet());
                writer.println("TRACE " + traceId + " origin=" + origin + " threads=" + threads);
            }

            // State correlations: find objects touched by multiple traces
            writer.println();
            writer.println("=== STATE CORRELATIONS ===");
            // No good way to get class.field back from the object identity,
            // so correlations are already visible in the CROSS_FLOW annotations
            // in the streaming events above. This section summarises trace pairs.
            Set<String> reportedPairs = new HashSet<>();
            synchronized (objectTraceMap) {
                for (Map.Entry<Object, Set<String>> entry : objectTraceMap.entrySet()) {
                    Set<String> keys = entry.getValue();
                    if (keys.size() <= 1) continue;
                    // Extract distinct traceIds
                    Set<String> traceIds = new HashSet<>();
                    for (String k : keys) {
                        int sep = k.indexOf('|');
                        if (sep > 0) traceIds.add(k.substring(0, sep));
                    }
                    if (traceIds.size() <= 1) continue;
                    // Report each pair once
                    String pairKey = traceIds.toString();
                    if (reportedPairs.add(pairKey)) {
                        writer.println("CORRELATED_TRACES " + traceIds + " via shared object (keys=" + keys + ")");
                    }
                }
            }

            writer.flush();
            writer.close();
        }
    }
}