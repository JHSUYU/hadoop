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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The lifecycle half of GraphChecker's RPC control plane.
 *
 * <p>{@link CausynthRpcTrace} names one exchange and moves its leaves.  This
 * class names what <em>happened</em> to that exchange, because a send and a
 * receive alone cannot tell four very different situations apart:</p>
 *
 * <ul>
 *   <li>an attempt still in flight, whose response may yet arrive;</li>
 *   <li>an attempt that <em>timed out</em> or was <em>cancelled</em>, which is
 *   terminal for its response even though the request really was sent;</li>
 *   <li>an attempt that <em>failed</em>, locally or with a remote error, which
 *   produced no response value at all;</li>
 *   <li>a response that arrives <em>after</em> its attempt was abandoned.</li>
 * </ul>
 *
 * <p>Without these events all four look identical downstream - an exchange
 * with a send and no receive - and the only way to tell them apart would be to
 * guess.  Each therefore becomes its own event, carrying its name inside the
 * correlation string so it survives into the trace.</p>
 *
 * <h3>Terminal attempts and late responses</h3>
 *
 * <p>A terminal outcome is remembered per {@code (exchange, attempt, half)}.
 * Terminating the response half does <em>not</em> terminate the request half:
 * a request that really was delivered is still legitimately bound at the
 * server even though its caller has given up waiting.  Once a half is
 * terminal, a value offered for it later is a late arrival: it is recorded
 * against that exchange as {@link #LATE_RESPONSE}, and it is neither exported
 * nor attached.  Binding it would mean handing the caller an expression for a
 * value the caller never received.</p>
 *
 * <h3>Fan-out and aggregation</h3>
 *
 * <p>A caller that sends one logical call to several endpoints opens a group.
 * Each branch's exchanges join the group through the thread that issues them,
 * never by comparing what they carry, so two branches remain two exchange
 * events even when their payloads are byte-identical.  At the end of the group
 * the trace states what the application did with them:</p>
 *
 * <ul>
 *   <li>exactly one response - {@code choice=SINGLE};</li>
 *   <li>several responses of which the application consumed one -
 *   {@code choice=GUARDED}: the consumed branch is
 *   {@link #FANOUT_MEMBER} {@code outcome=SELECTED} and every other responder
 *   is {@code outcome=ALTERNATIVE}, which is exactly the material a guarded
 *   message choice is built from;</li>
 *   <li>several responses and no single consumer - every responder remains a
 *   bounded causal alternative.  This trace layer never rejects the group or
 *   invents one distinguished response.</li>
 * </ul>
 *
 * <p>Like every other bridge here this one is fail-open and never throws into
 * a Hadoop call: with GraphChecker absent, {@link CausynthRpcTrace#isEnabled()}
 * is false and every method below returns immediately.</p>
 */
public final class CausynthExchangeLifecycle {

  /**
   * Direction of a lifecycle event.  It is deliberately neither {@code SEND}
   * nor {@code RECEIVE} so an event can never be paired as a boundary or
   * consume a boundary's pending correlation.
   */
  public static final String EVENT = "RPC_EXCHANGE_EVENT";

  /** The attempt terminated because its response did not arrive in time. */
  public static final String TIMED_OUT = "RPC_EXCHANGE_TIMED_OUT";
  /** The waiting caller was interrupted and abandoned the attempt. */
  public static final String CANCELLED = "RPC_EXCHANGE_CANCELLED";
  /** The attempt ended in a local failure; no response value exists. */
  public static final String FAILED_LOCAL = "RPC_EXCHANGE_FAILED_LOCAL";
  /** The server answered with an error; no response value exists. */
  public static final String FAILED_REMOTE = "RPC_EXCHANGE_FAILED_REMOTE";
  /** The request never left the client, so neither half can ever bind. */
  public static final String SEND_FAILED = "RPC_EXCHANGE_SEND_FAILED";

  /**
   * One bounded wait expired.  This is <em>not</em> terminal: the attempt is
   * still in flight and a later poll may still complete it.  It exists so an
   * exchange that is merely slow is distinguishable from one that is over.
   */
  public static final String POLL_EXPIRED = "RPC_EXCHANGE_POLL_EXPIRED";

  /** A response arrived for an attempt that had already terminated. */
  public static final String LATE_RESPONSE = "RPC_EXCHANGE_LATE_RESPONSE";
  /** A response arrived for an attempt this bridge cannot name. */
  public static final String ORPHAN_RESPONSE = "RPC_EXCHANGE_ORPHAN_RESPONSE";

  /** The handler deferred its response; the exchange is still open. */
  public static final String DEFERRED = "RPC_EXCHANGE_DEFERRED";
  /** A deferred response was produced and is being exported now. */
  public static final String DEFERRED_RESUMED = "RPC_EXCHANGE_DEFERRED_RESUMED";
  /** A deferred response ended in an error instead of a value. */
  public static final String DEFERRED_FAILED = "RPC_EXCHANGE_DEFERRED_FAILED";

  /** One exchange issued inside a fan-out branch. */
  public static final String FANOUT_BRANCH = "RPC_EXCHANGE_FANOUT_BRANCH";
  /** One branch of a finished group, with what became of it. */
  public static final String FANOUT_MEMBER = "RPC_EXCHANGE_FANOUT_MEMBER";
  /** The shape of a finished group's result. */
  public static final String FANOUT_RESULT = "RPC_EXCHANGE_FANOUT_RESULT";

  /** A branch outcome referring to a group this bridge no longer holds. */
  public static final String GAP_UNKNOWN_GROUP =
      "RPC_HANDOFF_FANOUT_UNKNOWN_GROUP";

  /** Both halves of an exchange, for a terminal outcome that ends it. */
  public static final String BOTH_HALVES = "BOTH";

  /** The attempt ordinal of a response whose attempt cannot be named. */
  public static final String UNKNOWN_ATTEMPT = "UNKNOWN";

  /** The half a group event is about; a group has no request or response. */
  private static final String GROUP_HALF = "GROUP";

  /** Branch outcomes reported by {@link #FANOUT_MEMBER}. */
  private static final String SELECTED = "SELECTED";
  private static final String SOLE_RESPONSE = "SOLE_RESPONSE";
  private static final String ALTERNATIVE = "ALTERNATIVE";
  private static final String FAILED = "FAILED";
  private static final String NO_RESPONSE = "NO_RESPONSE";

  /** Result shapes reported by {@link #FANOUT_RESULT}. */
  private static final String CHOICE_NONE = "NONE";
  private static final String CHOICE_SINGLE = "SINGLE";
  private static final String CHOICE_GUARDED = "GUARDED";

  private static final int MAX_TERMINAL_ATTEMPTS = 512;
  private static final int MAX_LIVE_GROUPS = 64;
  private static final int MAX_BRANCH_EXCHANGES = 64;

  private static final AtomicLong GROUP_IDS = new AtomicLong();

  /**
   * Terminal outcomes, keyed by exchange, attempt and half.  Bounded the same
   * way the exchange table is: an evicted terminal record can only make a very
   * late arrival look unnamed, never make it bind.
   */
  private static final Map<String, String> TERMINAL =
      new LinkedHashMap<String, String>(64, 0.75f, false) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> old) {
          return size() > MAX_TERMINAL_ATTEMPTS;
        }
      };

  private static final Map<String, Group> GROUPS =
      new LinkedHashMap<String, Group>(16, 0.75f, false) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Group> old) {
          return size() > MAX_LIVE_GROUPS;
        }
      };

  private static final ThreadLocal<BranchRef> CURRENT_BRANCH =
      new ThreadLocal<BranchRef>();

  private CausynthExchangeLifecycle() {
  }

  // ---------------------------------------------------------- termination --

  /**
   * Classifies how a completed-with-error attempt ended.  A remote error means
   * the server did answer, which is a different fact from never hearing back,
   * and a socket timeout is a different fact again from a broken connection.
   */
  public static String classify(IOException error) {
    if (error instanceof RemoteException) {
      return FAILED_REMOTE;
    }
    if (error instanceof SocketTimeoutException) {
      return TIMED_OUT;
    }
    return FAILED_LOCAL;
  }

  /**
   * Records a terminal outcome for one half of one attempt and retires that
   * half's exported leaves, so nothing that arrives afterwards can consume
   * them.
   */
  public static void terminate(String transport, String exchangeId,
      String attemptId, String half, String event, String detail,
      Object localOwner) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    if (BOTH_HALVES.equals(half)) {
      terminate(transport, exchangeId, attemptId, CausynthRpcTrace.REQUEST,
          event, detail, localOwner);
      terminate(transport, exchangeId, attemptId, CausynthRpcTrace.RESPONSE,
          event, detail, localOwner);
      return;
    }
    synchronized (TERMINAL) {
      TERMINAL.put(key(transport, exchangeId, attemptId, half), event);
    }
    CausynthRpcTrace.emitEvent(transport, exchangeId, attemptId, half, event,
        detail, localOwner);
  }

  /** Records one non-terminal lifecycle observation. */
  public static void note(String transport, String exchangeId,
      String attemptId, String half, String event, String detail,
      Object localOwner) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    CausynthRpcTrace.emitEvent(transport, exchangeId, attemptId, half, event,
        detail, localOwner);
  }

  /**
   * The terminal event of one half of one attempt, or null while it is still
   * open.
   */
  static String terminalOf(String transport, String exchangeId,
      String attemptId, String half) {
    synchronized (TERMINAL) {
      return TERMINAL.get(key(transport, exchangeId, attemptId, half));
    }
  }

  /**
   * Records a response that arrived for an attempt that had already
   * terminated.  Nothing is bound: the caller it belonged to is gone.
   */
  public static void lateResponse(String transport, String exchangeId,
      String attemptId, String terminal, Object localOwner) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    CausynthRpcTrace.emitEvent(transport, exchangeId, attemptId,
        CausynthRpcTrace.RESPONSE, LATE_RESPONSE, "after=" + terminal,
        localOwner);
  }

  /**
   * Records a response for which no attempt can be named at all - it belongs
   * to no live call and to no remembered terminal one.
   *
   * <p>The attempt ordinal is reported as {@link #UNKNOWN_ATTEMPT} rather than
   * being inferred from the exchange's other attempts.  An inferred ordinal
   * would be a guessed binding of exactly the kind this layer refuses.</p>
   */
  public static void orphanResponse(String transport, String exchangeId,
      Object localOwner) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    CausynthRpcTrace.emitEvent(transport, exchangeId, UNKNOWN_ATTEMPT,
        CausynthRpcTrace.RESPONSE, ORPHAN_RESPONSE, "attempt=unnamed",
        localOwner);
  }

  // -------------------------------------------------------------- fan-out --

  /**
   * Opens a group for one logical call that is about to be sent to several
   * endpoints.
   *
   * @return the group id, or the empty string when nothing is on
   */
  public static String beginFanOut(String logicalName, Object localOwner) {
    if (!CausynthRpcTrace.isEnabled()) {
      return "";
    }
    String groupId = "FANOUT:" + GROUP_IDS.incrementAndGet();
    Group group = new Group(safe(logicalName), localOwner);
    synchronized (GROUPS) {
      GROUPS.put(groupId, group);
    }
    return groupId;
  }

  /**
   * Marks this thread as executing one named branch of {@code groupId}.  Every
   * exchange the thread sends until {@link #exitBranch()} joins that branch.
   *
   * <p>Branches do not nest: one thread runs one branch of one group, which is
   * what a fan-out is.</p>
   */
  public static void enterBranch(String groupId, String branchName) {
    if (groupId == null || groupId.isEmpty()
        || !CausynthRpcTrace.isEnabled()) {
      return;
    }
    Group group = group(groupId);
    if (group == null) {
      unknownGroup(groupId, safe(branchName), "enter");
      return;
    }
    group.branch(safe(branchName));
    CURRENT_BRANCH.set(new BranchRef(groupId, safe(branchName)));
  }

  /** Ends the branch scope of the calling thread. */
  public static void exitBranch() {
    CURRENT_BRANCH.remove();
  }

  /**
   * Names a branch outcome whose group is gone - closed early, or evicted by
   * the live-group bound.  The membership fact is still recorded; it is simply
   * recorded as unattributable rather than dropped.
   */
  private static void unknownGroup(String groupId, String branchName,
      String stage) {
    CausynthRpcTrace.emitEvent(CausynthRpcTrace.TRANSPORT_HADOOP_IPC, groupId,
        "0", GROUP_HALF, FANOUT_MEMBER,
        "branch=" + branchName + "#outcome=" + GAP_UNKNOWN_GROUP
            + "#stage=" + stage, null);
  }

  /**
   * Joins one freshly allocated exchange to the branch its sending thread is
   * in.  Membership comes from the sender's own scope, never from what the
   * message contains, which is what keeps two identical-looking branches two
   * distinct exchanges.
   */
  static void joinBranch(String transport, String exchangeId,
      String attemptId) {
    BranchRef ref = CURRENT_BRANCH.get();
    if (ref == null) {
      return;
    }
    Group group = group(ref.groupId);
    if (group == null) {
      unknownGroup(ref.groupId, ref.branchName, "send");
      return;
    }
    String member = member(transport, exchangeId, attemptId);
    Object localOwner;
    synchronized (group) {
      group.branch(ref.branchName).add(member);
      localOwner = group.localOwner;
    }
    CausynthRpcTrace.emitEvent(transport, ref.groupId, "0", GROUP_HALF,
        FANOUT_BRANCH, "branch=" + ref.branchName + "#member=" + member,
        localOwner);
  }

  /** Records that the exchange named here answered its branch. */
  static void noteBranchResponse(String transport, String exchangeId,
      String attemptId) {
    BranchRef ref = CURRENT_BRANCH.get();
    if (ref == null) {
      return;
    }
    Group group = group(ref.groupId);
    if (group == null) {
      return;
    }
    synchronized (group) {
      group.branch(ref.branchName).responder =
          member(transport, exchangeId, attemptId);
    }
  }

  /**
   * The in-process name of one fan-out member.
   *
   * <p>This is a key of this class's own bookkeeping and nothing else: it is
   * written by {@link #joinBranch} and matched by {@link #noteBranchResponse},
   * and it never leaves the JVM as an exchange name.  It is spelled here, next
   * to the two places that use it, rather than borrowed from the recorded
   * correlation - which names an exchange and only an exchange, so that a
   * recorded leg and the attestation of the value it carried can be joined by
   * it.  The response half is the one a fan-out selects on, so the member key
   * names that half explicitly.</p>
   */
  private static String member(String transport, String exchangeId,
      String attemptId) {
    return transport + "|" + safe(exchangeId) + "|" + safe(attemptId) + "|"
        + CausynthRpcTrace.RESPONSE;
  }

  /** Records the branch whose response the application actually consumed. */
  public static void selectBranch(String groupId, String branchName) {
    Group group = group(groupId);
    if (group == null) {
      if (CausynthRpcTrace.isEnabled() && groupId != null
          && !groupId.isEmpty()) {
        unknownGroup(groupId, safe(branchName), "select");
      }
      return;
    }
    synchronized (group) {
      group.branch(safe(branchName)).outcome = SELECTED;
    }
  }

  /** Records a branch that produced no usable response. */
  public static void failBranch(String groupId, String branchName,
      String reason) {
    Group group = group(groupId);
    if (group == null) {
      if (CausynthRpcTrace.isEnabled() && groupId != null
          && !groupId.isEmpty()) {
        unknownGroup(groupId, safe(branchName), "fail");
      }
      return;
    }
    synchronized (group) {
      Branch branch = group.branch(safe(branchName));
      branch.outcome = FAILED;
      branch.detail = safe(reason);
    }
  }

  /**
   * Closes a group and states what its branches amount to: one expression to
   * propagate directly, several to keep as a guarded choice, or an aggregate
   * that no single exchange may be bound to.
   */
  public static void endFanOut(String groupId, Object localOwner) {
    if (groupId == null || groupId.isEmpty()) {
      return;
    }
    Group group;
    synchronized (GROUPS) {
      group = GROUPS.remove(groupId);
    }
    if (group == null) {
      if (CausynthRpcTrace.isEnabled()) {
        CausynthRpcTrace.emitEvent(CausynthRpcTrace.TRANSPORT_HADOOP_IPC,
            groupId, "0", GROUP_HALF, FANOUT_RESULT,
            "logical=?#reason=" + GAP_UNKNOWN_GROUP, localOwner);
      }
      return;
    }
    List<Branch> branches;
    synchronized (group) {
      branches = new ArrayList<Branch>(group.branches.values());
    }
    int responded = 0;
    String selected = "";
    for (Branch branch : branches) {
      if (branch.responded()) {
        responded++;
      }
      if (SELECTED.equals(branch.outcome)) {
        selected = branch.name;
      }
    }
    for (Branch branch : branches) {
      String outcome = branch.outcome;
      if (outcome == null) {
        // A branch the application never named is the sole response when it
        // is the only one there is, an alternative when it is one of several,
        // and simply no response otherwise.  None of them is promoted to the
        // branch the application selected, because it selected none.
        outcome = branch.responded()
            ? (responded == 1 ? SOLE_RESPONSE : ALTERNATIVE) : NO_RESPONSE;
      }
      CausynthRpcTrace.emitEvent(CausynthRpcTrace.TRANSPORT_HADOOP_IPC,
          groupId, "0", GROUP_HALF, FANOUT_MEMBER,
          "branch=" + branch.name + "#outcome=" + outcome
              + "#exchanges=" + branch.exchanges.size()
              + describeExchanges(branch)
              + (branch.detail.isEmpty() ? "" : "#why=" + branch.detail),
          group.localOwner);
    }
    String choice = responded == 0 ? CHOICE_NONE
        : (responded == 1 ? CHOICE_SINGLE : CHOICE_GUARDED);
    CausynthRpcTrace.emitEvent(CausynthRpcTrace.TRANSPORT_HADOOP_IPC, groupId,
        "0", GROUP_HALF, FANOUT_RESULT,
        "logical=" + group.logicalName + "#branches=" + branches.size()
            + "#responded=" + responded + "#choice=" + choice
            + "#selected=" + (selected.isEmpty() ? CHOICE_NONE : selected),
        group.localOwner);
  }

  private static String describeExchanges(Branch branch) {
    if (branch.responded()) {
      return "#member=" + branch.responder;
    }
    if (branch.exchanges.isEmpty()) {
      return "";
    }
    // One example is enough to join a branch to its exchanges; the count above
    // says how many there were.
    return "#member=" + branch.exchanges.get(0);
  }

  private static Group group(String groupId) {
    if (groupId == null || groupId.isEmpty()) {
      return null;
    }
    synchronized (GROUPS) {
      return GROUPS.get(groupId);
    }
  }

  private static String key(String transport, String exchangeId,
      String attemptId, String half) {
    return transport + "|" + exchangeId + "|" + attemptId + "|" + half;
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  /** One logical call sent to several endpoints. */
  private static final class Group {
    private final String logicalName;
    private final Object localOwner;
    private final Map<String, Branch> branches =
        new LinkedHashMap<String, Branch>();

    Group(String logicalName, Object localOwner) {
      this.logicalName = logicalName;
      this.localOwner = localOwner;
    }

    synchronized Branch branch(String name) {
      Branch branch = branches.get(name);
      if (branch == null) {
        branch = new Branch(name);
        branches.put(name, branch);
      }
      return branch;
    }
  }

  /** One endpoint's attempt at the group's logical call. */
  private static final class Branch {
    private final String name;
    private final List<String> exchanges = new ArrayList<String>(2);
    private String responder = "";
    private String outcome;
    private String detail = "";

    Branch(String name) {
      this.name = name;
    }

    void add(String member) {
      if (exchanges.size() < MAX_BRANCH_EXCHANGES) {
        exchanges.add(member);
      }
    }

    /** Whether one of this branch's exchanges really answered. */
    boolean responded() {
      return !responder.isEmpty();
    }
  }

  /** The branch scope of one thread. */
  private static final class BranchRef {
    private final String groupId;
    private final String branchName;

    BranchRef(String groupId, String branchName) {
      this.groupId = groupId;
      this.branchName = branchName;
    }
  }
}
