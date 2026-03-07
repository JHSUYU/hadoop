# Pilot-Style Tracing for HDFS

## Goal
Implement pilot-style distributed tracing by propagating a traceId
through Hadoop's RPC layer using OpenTelemetry Baggage. This enables
cross-process request flow tracing.

## What I Need to Change

### Phase 0: OpenTelemetry Dependency
- Check if Hadoop already depends on OpenTelemetry (check root pom.xml
  and hadoop-common pom.xml for opentelemetry-api / opentelemetry-context)
- If not present, add opentelemetry-api and opentelemetry-context as
  dependencies to the appropriate pom.xml files
- We only need the API/context modules, NOT the full SDK or exporters

### Phase 1: Protobuf Change
- Add a `traceId` string field to `RpcRequestHeaderProto` in the
  RPC header .proto file
- Regenerate Java code from proto

### Phase 2: RPC Client (sender)
- Before sending an RPC request, read the traceId from OpenTelemetry
  Baggage in the current context:
```java
  String traceId = Baggage.current().getEntryValue("traceId");
```
- If traceId is not null, set it in the RpcRequestHeaderProto builder

### Phase 3: RPC Server (receiver)
- When receiving an RPC request, extract traceId from the protobuf header
- If present, inject it into the current OpenTelemetry Baggage context:
```java
  Baggage baggage = Baggage.builder()
      .put("traceId", traceId)
      .build();
  Context ctx = Context.current().with(baggage);
  // Use ctx.makeCurrent() or wrap the handler scope
```
- Make sure the context scope is properly closed after processing

## Design Constraints
- Use OpenTelemetry API only (opentelemetry-api, opentelemetry-context)
- Do NOT add full OpenTelemetry SDK, exporters, or agents
- The traceId value lives in Baggage under the key "traceId"
- Must not break existing RPC functionality
- Follow existing Hadoop code style

## Key Directories
- RPC proto files: hadoop-common-project/hadoop-common/src/main/proto/
- RPC Java code: hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/
- HDFS code: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/
- Root pom: pom.xml (for dependency management)
- Common pom: hadoop-common-project/hadoop-common/pom.xml

## Build Commands
- Compile hadoop-common:
  mvn package -pl hadoop-common-project/hadoop-common -DskipTests
- Regenerate protobuf Java code:
  mvn generate-sources -pl hadoop-common-project/hadoop-common
- Run a specific test:
  mvn test -pl hadoop-common-project/hadoop-common -Dtest=TestIPC
- Compile hadoop-hdfs:
  mvn package -pl hadoop-hdfs-project/hadoop-hdfs -DskipTests

## OpenTelemetry API Reference
- Read from baggage: `Baggage.current().getEntryValue("traceId")`
- Write to baggage: `Baggage.builder().put("traceId", val).build()`
- Attach to context: `Context.current().with(baggage).makeCurrent()`
- Import: `io.opentelemetry.api.baggage.Baggage`
- Import: `io.opentelemetry.context.Context`
- Import: `io.opentelemetry.context.Scope`

## Progress

### Phase 0: OpenTelemetry Dependency ✅
- Confirmed no prior OpenTelemetry dependency existed anywhere in the project
- Dependencies are managed in `hadoop-project/pom.xml` (not the root `pom.xml`)
- Added `<opentelemetry.version>1.40.0</opentelemetry.version>` property in `hadoop-project/pom.xml` (line ~145)
- Added `<dependencyManagement>` entries for `opentelemetry-api` and `opentelemetry-context` in `hadoop-project/pom.xml` (before closing `</dependencies>` at line ~1923)
- Added `<dependency>` entries (no version, inherited from parent) in `hadoop-common-project/hadoop-common/pom.xml` (before closing `</dependencies>` at line ~375)
- Build verified: `mvn package -pl hadoop-common-project/hadoop-common -DskipTests` → BUILD SUCCESS

### Next Step: Phase 1 — Protobuf Change
- Add `optional string traceId = <next field number>` to `RpcRequestHeaderProto` in `hadoop-common-project/hadoop-common/src/main/proto/RpcHeader.proto`
- Regenerate Java code with `mvn generate-sources -pl hadoop-common-project/hadoop-common`
- Verify build still passes