# Phase 2: Log Replication - Comprehensive KRaft Implementation Analysis

## Executive Summary

Phase 2 (Log Replication) is the core mechanism by which the leader distributes log entries to followers and ensures all committed entries are replicated to a quorum. Unlike Phase 1 (Election) which uses explicit voting, Phase 2 uses a pull-based model where followers fetch entries from the leader and report their progress. The leader tracks replica progress and computes a high watermark (HWM) when entries are replicated to a majority.

---

## 1. Complete Class Catalog for Log Replication

### Core Replica Management Classes

#### LeaderState<T> (org.apache.kafka.raft.LeaderState)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java` (lines 63-1154)

**Purpose:** Manages leader state and tracks replication progress of all followers/observers

**Key Methods:**
- `updateLocalState(LogOffsetMetadata, VoterSet)` (line 820): Update local replica end offset and compute HWM
- `updateReplicaState(ReplicaKey, long, LogOffsetMetadata)` (line 846): Update follower/observer progress after fetch
- `maybeUpdateHighWatermark()` (line 727): Core HWM computation logic
- `updateCheckQuorumForFollowingVoter(ReplicaKey, long)` (line 251): Track which followers fetched in current check quorum period
- `needToSendBeginQuorumRequests(long)` (line 203): Return replicas needing BeginQuorumEpoch request
- `timeUntilCheckQuorumExpires(long)` (line 223): Check if leader lost quorum

**State Managed:**
- `highWatermark: Optional<LogOffsetMetadata>` (line 76): Current HWM offset
- `voterStates: Map<Integer, ReplicaState>` (line 77): Per-voter replica state
- `observerStates: Map<ReplicaKey, ReplicaState>` (line 81): Per-observer replica state
- `fetchedVoters: Set<Integer>` (line 85): Voters that fetched in current quorum window
- `accumulator: BatchAccumulator<T>` (line 83): Batches pending append to leader log
- `checkQuorumTimer: Timer` (line 86): Tracks leader liveness check
- `beginQuorumEpochTimer: Timer` (line 88): Tracks BeginQuorumEpoch request delays

**Dependencies:**
- `VoterSet.VoterNode`: Local voter info
- `BatchAccumulator<T>`: Pending records accumulator
- `KafkaRaftMetrics`: Observability

#### LeaderState.ReplicaState (org.apache.kafka.raft.LeaderState, inner class)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java` (lines 978-1115)

**Purpose:** Tracks replication progress of a single replica (voter or observer)

**Key Methods:**
- `updateLeaderEndOffset(LogOffsetMetadata)` (line 1060): Update leader's own end offset
- `updateFollowerState(long, LogOffsetMetadata, Optional<LogOffsetMetadata>)` (line 1068): Update after follower fetch
- `compareTo(ReplicaState)` (line 1092): Sort by descending endOffset (for HWM computation)

**State Managed:**
- `replicaKey: ReplicaKey` (line 979): Replica ID and directory ID
- `endOffset: Optional<LogOffsetMetadata>` (line 981): Last known replicated offset for this replica
- `lastFetchTimestamp: long` (line 982): When this replica last fetched
- `lastCaughtUpTimestamp: long` (line 984): When this replica last caught up to leader
- `hasAcknowledgedLeader: boolean` (line 985): Whether replica acknowledged this leader's epoch
- `listeners: Endpoints` (line 980): Replica's network endpoints

#### FollowerState (org.apache.kafka.raft.FollowerState)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/FollowerState.java` (lines 32-287)

**Purpose:** Manages follower state and tracks replication progress from leader's perspective

**Key Methods:**
- `resetFetchTimeoutForSuccessfulFetch(long)` (line 137): Reset fetch timeout after successful append
- `updateHighWatermark(OptionalLong)` (line 175): Update HWM from leader's Fetch response
- `hasFetchTimeoutExpired(long)` (line 129): Check if fetch timeout expired (triggers election)
- `hasUpdateVoterSetPeriodExpired(long)` (line 157): Check if time to send UpdateRaftVoter

**State Managed:**
- `epoch: int` (line 36): Current epoch (follower's view)
- `leaderId: int` (line 37): Current leader ID
- `leaderEndpoints: Endpoints` (line 38): Leader's network endpoints
- `highWatermark: Optional<LogOffsetMetadata>` (line 51): Last known HWM from leader
- `fetchTimer: Timer` (line 42): Timeout for Fetch responses (triggers election if expired)
- `updateVoterSetPeriodTimer: Timer` (line 44): Timeout for sending UpdateRaftVoter request
- `fetchingSnapshot: Optional<RawSnapshotWriter>` (line 59): Snapshot being fetched

#### ReplicaKey (org.apache.kafka.raft.ReplicaKey)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/ReplicaKey.java` (lines 24-82)

**Purpose:** Unique identifier for a replica, combining ID and directory ID

**Key Methods:**
- `of(int id, Uuid directoryId)` (line 76): Factory method to create ReplicaKey
- `compareTo(ReplicaKey)` (line 44): Compare by ID then directory ID

**State Managed:**
- `id: int` (line 27): Replica broker ID
- `directoryId: Optional<Uuid>` (line 28): Replica's storage directory ID (for replica tracking across restarts)

### Log Handling Classes

#### ReplicatedLog (org.apache.kafka.raft.ReplicatedLog)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java` (lines 28-321)

**Purpose:** Interface for log operations (append, fetch, truncation, HWM updates)

**Key Methods:**
- `appendAsLeader(Records, int)` (line 41): Append records as leader, assign epoch
- `appendAsFollower(Records, int)` (line 57): Append records as follower, validate epochs
- `read(long, Isolation)` (line 62): Read records from log with isolation level (COMMITTED or UNCOMMITTED)
- `updateHighWatermark(LogOffsetMetadata)` (line 183): Update HWM
- `validateOffsetAndEpoch(long, int)` (line 91): Validate fetch offset/epoch, return action (VALID/DIVERGING/SNAPSHOT)
- `truncateTo(long)` (line 164): Truncate log to offset (remove entries at/after offset)
- `endOffset()` (line 136): Get log end offset
- `highWatermark()` (line 141): Get current HWM

**Implementations:** FileBasedLog (not shown, in Kafka commons)

#### LogAppendInfo (org.apache.kafka.raft.LogAppendInfo)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LogAppendInfo.java` (lines 22)

**Purpose:** Metadata about appended batch

**State Managed:**
- `firstOffset: long`: First offset in appended batch
- `lastOffset: long`: Last offset in appended batch

#### LogFetchInfo (org.apache.kafka.raft.LogFetchInfo)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LogFetchInfo.java` (lines 24-33)

**Purpose:** Metadata and records returned from log read

**State Managed:**
- `records: Records`: Record batches
- `startOffsetMetadata: LogOffsetMetadata`: Starting offset and metadata

#### LogOffsetMetadata (org.apache.kafka.raft.LogOffsetMetadata)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LogOffsetMetadata.java` (lines 25-66)

**Purpose:** Offset with optional opaque metadata (e.g., segment info for avoiding index lookups)

**State Managed:**
- `offset: long`: Offset value
- `metadata: Optional<OffsetMetadata>`: Opaque metadata from log implementation

#### OffsetMetadata (org.apache.kafka.raft.OffsetMetadata)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/OffsetMetadata.java` (lines 20)

**Purpose:** Marker interface for opaque log metadata

**Implementations:** FileBasedLog maintains this (e.g., segment position info)

#### ValidOffsetAndEpoch (org.apache.kafka.raft.ValidOffsetAndEpoch)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/ValidOffsetAndEpoch.java` (lines 23-82)

**Purpose:** Result of offset/epoch validation, indicates action for follower

**Key Methods:**
- `valid(OffsetAndEpoch)` (line 52): Offset/epoch is valid, append from this offset
- `diverging(OffsetAndEpoch)` (line 44): Offset/epoch diverged, truncate to this offset and retry
- `snapshot(OffsetAndEpoch)` (line 48): Offset/epoch is before log start, fetch snapshot

**State Managed:**
- `kind: Kind` (VALID, DIVERGING, SNAPSHOT): Action for follower
- `offsetAndEpoch: OffsetAndEpoch`: Offset/epoch pair to use

#### Batch<T> (org.apache.kafka.raft.Batch)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/Batch.java` (lines 28-224)

**Purpose:** Immutable representation of a record batch with metadata

**Key Methods:**
- `data(long, int, long, int, List<T>)` (line 197): Create data batch (for client records)
- `control(long, int, long, int, List<ControlRecord>)` (line 160): Create control batch (for metadata)

**State Managed:**
- `baseOffset: long` (line 29): First record offset
- `epoch: int` (line 30): Leader epoch that appended batch
- `appendTimestamp: long` (line 31): When batch was appended
- `lastOffset: long` (line 33): Last record offset
- `records: List<T>` (line 34): User records (empty for control batches)
- `controlRecords: List<ControlRecord>` (line 35): Control records (empty for data batches)

#### ControlRecord (org.apache.kafka.raft.ControlRecord)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/ControlRecord.java` (not shown)

**Purpose:** Control record for metadata (leader change, voter set change, etc.)

### Batch Accumulation Classes

#### BatchAccumulator<T> (org.apache.kafka.raft.internals.BatchAccumulator)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchAccumulator.java` (lines 47-200+)

**Purpose:** Accumulates incoming records into batches, controls when batches are drained to log

**Key Methods:**
- `append(int, List<T>, boolean)` (line 118): Append records, return last offset
- `drain()` (line 3082 in KafkaRaftClient.java): Get all completed batches
- `timeUntilDrain(long)` (line 3080): Time until next drain (linger timeout)
- `allowDrain()` (line 3729): Allow draining immediately (for scheduled append)
- `needsDrain(long)` (line 3713): Check if accumulator ready to drain
- `close()`: Release all resources
- `appendControlMessages(MemoryRecordsCreator)`: Append control records (epoch change, voter set)
- `appendVotersRecord(VotersRecord, long)`: Append voter set record

**State Managed:**
- `epoch: int` (line 58): Current leader epoch
- `lingerMs: int` (line 60): Delay before draining (batching window)
- `maxBatchSizeBytes: int` (line 61): Max size of single batch
- `maxNumberOfBatches: int` (line 62): Max accumulated batches (backpressure)
- `nextOffset: long` (line 74): Next offset to assign
- `currentBatch: BatchBuilder<T>` (line 75): Batch under construction
- `completed: ConcurrentLinkedQueue<CompletedBatch<T>>` (line 69): Batches ready for append
- `drainStatus: DrainStatus` (line 70): Drain in progress?
- `drainOffset: AtomicLong` (line 68): Maximum offset allowed to drain (for delayDrain)

#### BatchBuilder<T> (org.apache.kafka.raft.internals.BatchBuilder)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchBuilder.java` (lines 49-200+)

**Purpose:** Builds a single record batch in compressed ByteBuffer

**Key Methods:**
- `appendRecord(T, ObjectSerializationCache)` (line 100): Add record to batch
- `bytesNeeded(Collection<T>, ObjectSerializationCache)`: Compute bytes needed for records
- `build()`: Finalize batch and return MemoryRecords
- `bytesUsed()`: Current batch size

**State Managed:**
- `baseOffset: long` (line 54): Batch's starting offset
- `leaderEpoch: int` (line 56): Leader epoch
- `nextOffset: long` (line 62): Next offset to assign
- `initialBuffer: ByteBuffer` (line 50): Underlying memory
- `compression: Compression` (line 51): Compression type (snappy, lz4, gzip, zstd)
- `records: List<T>` (line 60): Records accumulated

### Request/Response Message Classes

#### RaftRequest (org.apache.kafka.raft.RaftRequest)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/RaftRequest.java` (lines 25-115)

**Purpose:** Base class for outbound/inbound Raft protocol requests

**Key Subclasses:**
- `Inbound` (line 50): Request received from network
  - `apiVersion: short`: Protocol version
  - `listenerName: ListenerName`: Which listener received request
  - `completion: CompletableFuture<RaftResponse.Outbound>`: Future for response
- `Outbound` (line 91): Request to be sent to remote
  - `destination: Node`: Target node
  - `completion: CompletableFuture<RaftResponse.Inbound>`: Future for response

**Supported Message Types:**
- `FetchRequestData`/`FetchResponseData`: Log replication fetch
- `BeginQuorumEpochRequestData`/`BeginQuorumEpochResponseData`: Assert leadership
- `EndQuorumEpochRequestData`/`EndQuorumEpochResponseData`: Graceful leader resignation
- `VoteRequestData`/`VoteResponseData`: Election voting (Phase 1)
- `DescribeQuorumRequestData`/`DescribeQuorumResponseData`: Quorum status query

#### Fetch Request/Response Structure

**FetchRequestData** (from `org.apache.kafka.common.message`)
- `topics[].partitions[].fetchOffset`: Offset to fetch from
- `topics[].partitions[].lastFetchedEpoch`: Epoch of last record follower has
- `topics[].partitions[].currentLeaderEpoch`: Follower's view of current epoch
- `maxWaitMs`: Max time to wait for data (enables polling)
- `minBytes`: Min bytes to return (for batching)

**FetchResponseData**
- `errorCode`: Top-level error
- `nodeEndpoints`: All known brokers (for client bootstrap)
- `responses[].partitions[].errorCode`: Partition-level error
- `responses[].partitions[].highWatermark`: Current HWM (for followers to update)
- `responses[].partitions[].divergingEpoch`: Diverging offset/epoch (triggers truncation)
- `responses[].partitions[].snapshotId`: Snapshot to fetch (triggers snapshot fetch)
- `responses[].partitions[].records`: Batches of log entries
- `responses[].partitions[].currentLeader`: Current leader ID and epoch

### State Tracking Classes

#### LeaderAndEpoch (org.apache.kafka.raft.LeaderAndEpoch)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java` (lines 22-32)

**Purpose:** Immutable record of leader ID and epoch

**State Managed:**
- `leaderId: OptionalInt`: Leader broker ID
- `epoch: int`: Epoch number

#### Isolation (org.apache.kafka.raft.Isolation)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/Isolation.java` (lines 19-22)

**Purpose:** Enum indicating isolation level for log reads

**Values:**
- `COMMITTED`: Read only entries below HWM (safe for state machine)
- `UNCOMMITTED`: Read all entries (for replication)

### Request Management Classes

#### RequestManager (org.apache.kafka.raft.RequestManager)
**Location:** `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/RequestManager.java` (lines 44-200+)

**Purpose:** Tracks connection state and request timeouts to remote nodes

**Key Methods:**
- `findReadyBootstrapServer(long)` (line 107): Find node ready to receive Fetch request
- `isReady(Node, long)` (line 185): Is node ready for requests?
- `isBackingOff(Node, long)` (line 199): Is node in backoff state?
- `hasRequestTimedOut(Node, long)` (line 176): Has request timed out?
- `hasAnyInflightRequest(long)` (line 74): Are any requests pending?
- `onRequestSent(Node, long, long)`: Update state after sending request
- `onResponseResult(Node, long, boolean, long)`: Update state after response

**State Managed:**
- `connections: Map<String, ConnectionState>`: Per-node connection states
- `bootstrapServers: ArrayList<Node>`: Known cluster nodes
- `retryBackoffMs: int`: Backoff before retry
- `requestTimeoutMs: int`: Request timeout

---

## 2. Log Replication Architecture

### How Leader Manages Replicas

**Leader tracking (`LeaderState`):**
1. Maintains `ReplicaState` for each voter and observer
2. On each `updateReplicaState()` call (from Fetch response):
   - Updates replica's end offset
   - Tracks `lastFetchTimestamp` (when replica last fetched)
   - Tracks `lastCaughtUpTimestamp` (when replica caught up)
3. `maybeUpdateHighWatermark()` computes HWM by:
   - Sorting all `voterStates` by descending `endOffset`
   - Taking the offset at index `voterStates.size() / 2` (median)
   - Only commits offsets > `epochStartOffset` (prevents committing old leader's entries)
4. Checks quorum liveness via `checkQuorumTimer`:
   - Requires fetch from majority of voters within timeout period
   - Failure causes leader to resign (prevents zombie leaders)

### Fetch Architecture

**Leader-side (handleFetchRequest):**
1. Validates cluster ID, topic partition, offset/epoch
2. Calls `log.validateOffsetAndEpoch()` to check offset validity:
   - Returns VALID: follower can append from offset
   - Returns DIVERGING: follower's log diverged, must truncate
   - Returns SNAPSHOT: offset before log start, must fetch snapshot
3. If VALID, calls `log.read(fetchOffset, Isolation.UNCOMMITTED)` to get records
4. Calls `state.updateReplicaState()` to track replica's progress
5. If fetch would block, adds to `fetchPurgatory` (wait for new data)
6. Returns response immediately or waits in purgatory

**Follower-side (pollFollowerAsVoter):**
1. Sends `FetchRequest` to leader (via `maybeSendFetchOrFetchSnapshot()`)
2. Specifies:
   - `fetchOffset`: Next offset to fetch
   - `lastFetchedEpoch`: Epoch of last record in follower's log
   - `currentLeaderEpoch`: Follower's view of current epoch
3. On `handleFetchResponse()`:
   - If divergingEpoch present: truncate log to that offset, retry
   - If snapshotId present: start snapshot fetch, pause log fetching
   - If records present: call `appendAsFollower()` to append them
   - Updates HWM from response
4. Checks quorum:
   - Calls `updateCheckQuorumForFollowingVoter()` to reset quorum timer
   - If majority fetched in period, quorum is alive

### Append Architecture

**Leader-side:**
1. Client calls `append(epoch, records)`
2. Records go to `BatchAccumulator` with `delayDrain=true` (no backpressure)
3. Accumulator batches records with linger delay (default ~10ms)
4. When `needsDrain()` true, `maybeAppendBatches()` drains batches:
   - Gets batch from accumulator
   - Calls `appendAsLeader()` to append to leader's log
   - Updates partition state (voter set, etc.)
   - Registers future in `appendPurgatory` to wait for commit
5. `flushLeaderLog()` calls:
   - `updateLeaderEndOffsetAndTimestamp()` to update local replica state
   - `maybeUpdateHighWatermark()` to compute new HWM
   - `log.flush()` to fsync to disk
6. When `appendPurgatory` completes (offset committed), callbacks fire to listener

**Follower-side:**
1. Receives `FetchResponse` with records
2. Calls `appendAsFollower()` which:
   - Validates record epochs against current epoch
   - Appends via `log.appendAsFollower()`
   - Flushes to disk (voters required to fsync for safety)
   - Updates partition state

### High Water Mark (HWM) Computation

**HWM Computation (`maybeUpdateHighWatermark` in LeaderState):**
1. Collects all voter `ReplicaState` objects
2. Sorts by descending `endOffset` (median-based quorum)
3. Takes replica at index `voterStates.size() / 2`
4. If that replica has an `endOffset`:
   - Checks if offset > `epochStartOffset` (only commit own epoch entries)
   - If updates HWM: calls `onUpdateLeaderHighWatermark()`
5. Constrains: HWM cannot decrease, only increase or stay same

**HWM Update Propagation (`onUpdateLeaderHighWatermark`):**
1. Updates log's HWM via `log.updateHighWatermark()`
2. Notifies add/remove voter handlers (for reconfiguration completion)
3. Completes entries in `appendPurgatory` (app callbacks)
4. Completes all pending fetches in `fetchPurgatory`
5. Updates listener progress (fire handleCommit callbacks)

**Follower HWM:**
1. Follower receives HWM in FetchResponse
2. Updates via `state.updateHighWatermark(OptionalLong)`
3. Validates HWM only increases
4. Callbacks to listeners when HWM advances

### Safety: Log Matching Property

**Enforced in KRaft via:**
1. **Offset/Epoch Validation**: Leader validates follower's `lastFetchedEpoch` against log
2. **Divergence Detection**: If epoch doesn't match expected, returns `divergingEpoch`
3. **Truncation**: Follower truncates log to `divergingEpoch.endOffset` before retrying
4. **Snapshot Fallback**: If offset before log start, snapshot is sent to restore log state
5. **Epoch Assignment**: Each batch assigned to current leader epoch (in BatchBuilder)
6. **Atomicity**: Append via `appendAsLeader()` and `appendAsFollower()` are atomic

---

## 3. Data Flow in Log Replication

### Complete Flow: Request → Append → Replicate → HWM Update → State Machine

**Step 1: Client Append Request**
```
RaftClient.append(List<T> records)
  ↓
Leader validation (throw NotLeaderException if not leader)
  ↓
LeaderState.accumulator().append(epoch, records, delayDrain=true)
  ↓
BatchAccumulator collects records, buffers with linger delay
```

**Step 2: Batch Draining (when linger timeout expires)**
```
pollLeader() → maybeAppendBatches()
  ↓
BatchAccumulator.drain() → list of CompletedBatch<T>
  ↓
For each batch:
  - appendBatch(state, batch, currentTimeMs)
    - LogAppendInfo info = log.appendAsLeader(batch.data(), epoch)
    - Registers future in appendPurgatory to wait for commit at offset info.lastOffset+1
```

**Step 3: Log Append to Disk**
```
log.appendAsLeader(Records records, int epoch)
  ↓
FileBasedLog appends records to active segment
  ↓
partitionState.updateState() parses control records (voter set, etc)
  ↓
metrics.updateAppendRecords() and updateLogEnd()
```

**Step 4: Leader Flush**
```
flushLeaderLog(state, currentTimeMs)
  ↓
updateLeaderEndOffsetAndTimestamp(state, currentTimeMs)
  ↓
state.updateLocalState(log.endOffset(), lastVoterSet)
  ↓
Calls maybeUpdateHighWatermark() if HWM advances
  ↓
log.flush(false) → fsync active segment
```

**Step 5: Follower Fetch (pulled by follower)**
```
pollFollowerAsVoter() → maybeSendFetchOrFetchSnapshot()
  ↓
Builds FetchRequest with:
  - fetchOffset: next offset to fetch
  - lastFetchedEpoch: epoch of last record in log
  - currentLeaderEpoch: follower's epoch
  ↓
Sent to leader via network
```

**Step 6: Leader Responds to Fetch**
```
handleFetchRequest(FetchRequestData request)
  ↓
validateOffsetAndEpoch(request.fetchOffset, request.lastFetchedEpoch)
  ↓
If VALID:
  - LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED)
  - state.updateReplicaState(replicaKey, currentTimeMs, info.startOffsetMetadata)
    ↓ (may trigger maybeUpdateHighWatermark)
  - Returns FetchResponse with records
Else if DIVERGING:
  - Returns FetchResponse with divergingEpoch (no records)
Else if SNAPSHOT:
  - Returns FetchResponse with snapshotId (follower fetches snapshot)
```

**Step 7: Follower Appends (from Fetch)**
```
handleFetchResponse(FetchResponseData response)
  ↓
If divergingEpoch present:
  - Truncates log: log.truncateTo(divergingEpoch.endOffset)
  - partitionState.truncateNewEntries()
  - Retries fetch from diverging point
Else if snapshotId present:
  - Sets fetchingSnapshot and pauses log fetches
  - Sends FetchSnapshotRequest
Else:
  - appendAsFollower(response.records())
    ↓
    log.appendAsFollower(records, epoch)
    ↓
    Validates epochs match current epoch
    ↓
    Appends to log
    ↓
    partitionState.updateState()
    ↓
    log.flush(false) for voters (fsync required)
  ↓
  Updates HWM from response:
    state.updateHighWatermark(OptionalLong.of(response.highWatermark()))
```

**Step 8: HWM Update at Leader**
```
state.updateReplicaState() returns true if HWM updated
  ↓
onUpdateLeaderHighWatermark(state)
  ↓
log.updateHighWatermark(newHWM)
  ↓
appendPurgatory.maybeComplete(hwm.offset) → completes pending append futures
  ↓
Callbacks fire: maybeFireHandleCommit() calls listener.handleCommit()
```

**Step 9: State Machine Reads (from listeners)**
```
Listener registered with RaftClient
  ↓
On HWM advance: listenerContext.fireHandleCommit(offset, records)
  ↓
Listener consumes committed records
```

**Classes at Each Step:**
1. RaftClient/append(): Initiates append
2. BatchAccumulator/BatchBuilder: Buffers and batches
3. ReplicatedLog: Stores to disk
4. LeaderState/ReplicaState: Tracks replica progress
5. RequestManager: Manages fetch request state
6. ValidOffsetAndEpoch: Validates offset/epoch consistency
7. FuturePurgatory: Waits for commit offsets
8. Listener: Consumes committed data

---

## 4. Replica State Management

### Replica States During Replication

Replicas transition through states tracked in LeaderState.ReplicaState:

**1. Unacknowledged State**
- `hasAcknowledgedLeader = false`
- Replica has not yet responded to BeginQuorumEpoch request
- Leader sends repeated BeginQuorumEpoch requests until acknowledged
- Transitioned to Acknowledged on any Fetch/FetchSnapshot from replica

**2. Acknowledged State**
- `hasAcknowledgedLeader = true`
- Replica has acknowledged leader's epoch
- Included in quorum count for check quorum

**3. In Sync (ISR) State**
- `endOffset >= leaderEndOffset` (caught up)
- `lastCaughtUpTimestamp` is recent
- Included in HWM computation

**4. Lagging State**
- `endOffset < leaderEndOffset`
- `lastFetchTimestamp` is recent (actively fetching)
- NOT included in HWM computation
- But counted as acknowledged (can be elected as next leader)

**5. Stale State**
- `lastFetchTimestamp` is very old
- Replica hasn't fetched within fetch timeout
- Triggers leader to send BeginQuorumEpoch again (if voter)

### State Transitions in ReplicaState

```
Unacknowledged
    ↓ (any Fetch/FetchSnapshot from replica)
Acknowledged → Lagging
    ↓ (endOffset < leaderEndOffset)
    ↓
    ↓ (endOffset >= leaderEndOffset)
    Caught Up (lastCaughtUpTimestamp updated)
    ↓ (lastFetchTimestamp expires)
Stale
    ↓ (receives Fetch)
Back to Lagging/Caught Up
```

### LeaderState Replica Tracking

**Classes tracking replica state:**
1. `LeaderState.voterStates: Map<Integer, ReplicaState>`
   - Tracks all voters
   - Used for HWM computation and quorum checks

2. `LeaderState.observerStates: Map<ReplicaKey, ReplicaState>`
   - Tracks observers (non-voters, e.g., learning replicas)
   - Not included in HWM computation
   - Cleaned up if inactive > 5 minutes

3. `LeaderState.fetchedVoters: Set<Integer>`
   - Tracks which voters fetched in current check quorum window
   - Used to verify quorum liveness
   - Cleared when majority fetched

4. `LeaderState.grantingVoters: Set<Integer>`
   - Voters who granted vote in election (Phase 1)
   - Used to ensure leader didn't miss any voters

---

## 5. Fetch Mechanism Details

### FetchRequest Structure

**Sent by follower to leader:**
```
FetchRequest {
  clusterId: String              // Cluster identity
  replicaId: int                // Follower's broker ID
  topics[0].name: String        // __raft_metadata
  topics[0].partitions[0] {
    partition: int              // Partition index (0 for KRaft)
    fetchOffset: long           // Offset to fetch from (next expected)
    lastFetchedEpoch: int       // Epoch of last record follower has
    currentLeaderEpoch: int     // Epoch follower believes is current
    replicaDirectoryId: Uuid    // Follower's storage directory UUID
  }
  maxWaitMs: int                // Max time to wait if no data
  minBytes: int                 // Min bytes to return
}
```

### FetchResponse Structure

**Returned by leader to follower:**
```
FetchResponse {
  errorCode: short              // Top-level error
  throttleTimeMs: int           // Broker throttling
  nodeEndpoints[]: {             // Broker endpoints for bootstrap
    nodeId: int
    host: String
    port: int
  }
  responses[0] {
    topic: String              // __raft_metadata
    partitions[0] {
      partition: int
      errorCode: short          // Partition-level error
      highWatermark: long       // Leader's current HWM (followers update from this)
      lastStableOffset: long    // Same as HWM for KRaft
      logStartOffset: long      // Log start (after snapshots)
      divergingEpoch: {         // If log diverged (follower must truncate)
        epoch: int
        endOffset: long
      }
      snapshotId: {             // If offset before log start (fetch snapshot)
        epoch: int
        endOffset: long
      }
      currentLeader: {          // Current leader info
        leaderId: int
        leaderEpoch: int
      }
      records: MemoryRecords    // Batches of log entries
    }
  }
}
```

### Fetch Success Determination

**Follower considers fetch successful if:**
1. No error code returned (errorCode == Errors.NONE)
2. Records appended successfully to log
3. Fetch timeout reset by `resetFetchTimeoutForSuccessfulFetch()`

**On fetch failure:**
1. Errors.FENCED_LEADER_EPOCH: Leader epoch is behind follower's, transition to Prospective
2. Errors.NOT_LEADER_OR_FOLLOWER: Leader stepped down, transition to Prospective
3. Errors.UNKNOWN_TOPIC_OR_PARTITION: Invalid partition, log error
4. Network timeout: Retry with backoff via RequestManager

### Conflicting Log Entry Resolution

**Detected via divergingEpoch:**
1. Follower sends lastFetchedEpoch = epoch of last record in follower's log
2. Leader calls `log.endOffsetForEpoch(lastFetchedEpoch)`
3. If leader's log has different epoch at that offset:
   - Returns `ValidOffsetAndEpoch.diverging()`
   - Sets `divergingEpoch` in FetchResponse

**Resolution flow:**
1. Follower receives divergingEpoch
2. Calls `log.truncateTo(divergingEpoch.endOffset)` to truncate conflicting entries
3. Calls `partitionState.truncateNewEntries(truncationOffset)`
4. Retries fetch from truncationOffset

**Safety guarantee:**
- Log Matching Property: If leader and follower have same offset/epoch, all previous entries are identical
- Enforced by truncation before retry

### Next Offset Calculation (Follower)

**How follower determines next fetch offset:**
1. On startup: `nextOffset = log.endOffset()` (fetch from log end)
2. After successful append: `nextOffset = log.endOffset()` (fetch from new end)
3. After truncation: `nextOffset = truncationOffset` (fetch from truncated point)
4. After snapshot fetch: `nextOffset = snapshotId.offset() + 1` (continue from snapshot)

**Tracked in follower via:**
- `log.endOffset()` always returns next offset to fetch

### Fetch Request Triggering

**Follower-driven fetch:**
1. Leader does NOT push entries (pull-based model)
2. Follower initiates fetch periodically via `maybeSendFetchToBestNode()`
3. Called every poll loop if:
   - No inflight request pending
   - RequestManager says node is ready
   - Fetch timeout not expired

**Timing:**
- If data available: fetch immediately
- If no data: wait up to maxWaitMs (from FetchRequest)
- Leader waits in purgatory for data if needed
- When data arrives: completes fetch from purgatory

---

## 6. Append Mechanism Details

### AppendRequest Structure

**Note:** KRaft doesn't use explicit AppendRequest messages. Append is implicit in FetchResponse handling.

**Log entries flow via:**
- Batches in FetchResponse.records (records returned in fetch)
- No separate RPC, piggybacks on fetch responses

### Leader Append Process

**Phase 1: Buffer Accumulation**
```
RaftClient.append(List<T> records)
  ↓
LeaderState.accumulator().append(epoch, records, delayDrain=true)
  ↓
BatchAccumulator allocates memory from BatchMemoryPool
  ↓
Serializes records into ByteBuffer via BatchBuilder
  ↓
Records buffered with configurable linger delay
  ↓
Returns last offset assigned to client
```

**Phase 2: Batch Finalization**
```
maybeAppendBatches() when linger timeout expires
  ↓
BatchAccumulator.drain() gets CompletedBatch objects
  ↓
CompletedBatch contains:
  - baseOffset: first offset in batch
  - data: MemoryRecords with serialized records
  - numRecords: count of records
  - appendTimestamp: when batch was created
  - release(): free memory back to pool
```

**Phase 3: Actual Append to Log**
```
appendBatch(state, completedBatch, currentTimeMs)
  ↓
LogAppendInfo info = appendAsLeader(batch.data)
  ↓
log.appendAsLeader(Records records, int epoch)
  ↓
FileBasedLog appends to active segment:
  - Validates batch epochs = current epoch
  - Appends to memory-mapped file
  - Updates in-memory log state
  ↓
Returns LogAppendInfo(firstOffset, lastOffset)
  ↓
Registers future in appendPurgatory:
  - future.await(offsetAndEpoch.offset() + 1, Integer.MAX_VALUE)
  - Will complete when HWM advances past this offset
```

**Phase 4: Log Flushing**
```
flushLeaderLog(state, currentTimeMs)
  ↓
updateLeaderEndOffsetAndTimestamp(state, currentTimeMs)
  ↓
state.updateLocalState(log.endOffset(), lastVoterSet)
  ↓
Triggers maybeUpdateHighWatermark() if state changed
  ↓
log.flush(false) fsync's to disk
```

### LogAppendInfo Structure

**Returned by appendAsLeader/appendAsFollower:**
```
LogAppendInfo {
  firstOffset: long    // First offset in appended batch
  lastOffset: long     // Last offset in appended batch
}
```

**Used to:**
- Register append purgatory future at `lastOffset + 1`
- Update metrics
- Track what was appended

### Epoch Assignment

**When appending:**
1. Client passes epoch from `KafkaRaftClient.currentEpoch()`
2. BatchAccumulator stores epoch at batch level
3. BatchBuilder assigns epoch to each batch
4. Log stores epoch in RecordBatch header
5. Followers validate epoch matches current epoch

**On epoch change:**
1. New leader increments epoch
2. Creates new BatchAccumulator with new epoch
3. Appends leader change control record with new epoch
4. Followers reject batches with old epoch (security)

### Handling Epoch Changes During Append

**If replica loses leadership during append:**
1. `LeaderState` transitions to `FollowerState` or other
2. BatchAccumulator still has pending batches (memory preserved)
3. BatchAccumulator.append() throws NotLeaderException on next call
4. Client must retry after learning new leader

**For in-flight appends:**
1. Records already in log persist (durability)
2. HWM might not advance if new leader doesn't replicate to quorum
3. Listeners might see records then un-see them on recovery

---

## 7. High Water Mark (HWM) Computation

### When HWM is Updated

**Updates triggered by:**
1. **Replica fetch success**: `updateReplicaState()` called in handleFetchRequest
   - Follower reported new end offset
   - Leader calls `maybeUpdateHighWatermark()`
2. **Local append**: `updateLocalState()` called in flushLeaderLog
   - Leader appended own record
   - Calls `maybeUpdateHighWatermark()`
3. **Quorum acknowledge**: When majority of replicas fetch

**Update frequency:**
- On every Fetch response from any replica
- On every local append to log
- ~10-100ms typically (batch linger delay)

### HWM Computation Algorithm

**Located in:** `LeaderState.maybeUpdateHighWatermark()` (lines 727-786)

```java
// Get all voter replica states sorted by descending endOffset
ArrayList<ReplicaState> sortedByOffset = voterStates.values()
  .stream()
  .sorted()  // Sorts by descending endOffset (see compareTo)
  .collect(Collectors.toCollection(ArrayList::new));

// Take median: if 3 voters, take 3/2=1 (2nd highest)
// If 5 voters, take 5/2=2 (3rd highest)
int indexOfHw = voterStates.size() / 2;
Optional<LogOffsetMetadata> candidate = sortedByOffset.get(indexOfHw).endOffset;

// Only commit if:
// 1. Candidate offset > epochStartOffset (this epoch's entries only)
// 2. Candidate offset > current HWM (monotonic increase)
if (candidate.isPresent() && 
    candidate.offset() > epochStartOffset &&
    candidate.offset() > currentHWM.offset()) {
  highWatermark = candidate;
  logHighWatermarkUpdate(...);
  return true;  // HWM changed
}
```

**Example (3 voters):**
- Voter A endOffset: 100
- Voter B endOffset: 98
- Voter C endOffset: 95
- Index = 3/2 = 1 → Voter B (98) → HWM = 98

**Example (5 voters):**
- Voters: 100, 99, 98, 97, 96
- Index = 5/2 = 2 → offset 98 → HWM = 98

### Replica ISR (In-Sync Replica Set) Management

**ISR != explicit tracked set. Implicit in HWM computation:**
1. Replicas with offset >= HWM are "in sync"
2. Replicas below HWM are "lagging"
3. Computed dynamically at each HWM update

**Replica eligibility for HWM:**
1. Must be voter (not observer)
2. Must have fetched at least once (endOffset != empty)
3. Must be acknowledged (hasAcknowledgedLeader)

**Quorum liveness check (`timeUntilCheckQuorumExpires`):**
- Requires majority of voters to fetch within `checkQuorumTimeoutMs`
- If majority doesn't fetch: leader becomes zombie and resigns
- Prevents leader from continuing if network partitioned

### Relationship Between HWM and Committed Offset

**In KRaft, HWM == committed offset:**
1. HWM = highest offset replicated to quorum
2. All offsets <= HWM are committed
3. Listeners only read offsets <= HWM (via Isolation.COMMITTED)

**Distinction from uncommitted:**
- `Isolation.UNCOMMITTED` reads up to log end offset (for replication)
- `Isolation.COMMITTED` reads up to HWM only (for state machine)

### HWM Effects on State Machine Updates

**When HWM advances:**
1. `onUpdateLeaderHighWatermark()` called
2. `log.updateHighWatermark()` updates log's HWM pointer
3. `appendPurgatory.maybeComplete(hwm.offset)` completes append futures
   - App callbacks fire: `handleCommit(offset, records)`
   - State machine processes committed records
4. `fetchPurgatory.completeAll()` completes parked fetch requests
   - Followers now see new HWM in response
5. `updateListenersProgress(hwm.offset)` notifies listeners
   - Reads committed records and fires handleCommit callbacks

**Listener flow:**
```
HWM advances
  ↓
updateListenersProgress(newHWM)
  ↓
For each registered listener:
  - LogFetchInfo = log.read(nextOffset, Isolation.COMMITTED)
  - listenerContext.fireHandleCommit(nextOffset, records)
  ↓
Listener.handleCommit() processes records
```

---

## 8. Safety Properties in Log Replication

### Log Matching Property

**Definition:** If leader and follower have entries with same offset and epoch, all previous entries are identical.

**KRaft Implementation:**

1. **Offset/Epoch Validation** (in `handleFetchRequest`):
```java
ValidOffsetAndEpoch validation = log.validateOffsetAndEpoch(
  fetchOffset, lastFetchedEpoch);

if (validation.kind() == Kind.VALID) {
  // Follower's offset/epoch matches, safe to send entries
  LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);
  // ... send records starting from fetchOffset
} else if (validation.kind() == Kind.DIVERGING) {
  // Offset/epoch mismatch, follower must truncate
  return buildResponse(divergingEpoch: validation.offsetAndEpoch());
}
```

2. **Divergence Detection** (in `ReplicatedLog.validateOffsetAndEpoch`):
```java
OffsetAndEpoch localEndOffset = log.endOffsetForEpoch(lastFetchedEpoch);
if (localEndOffset.epoch() != lastFetchedEpoch ||
    localEndOffset.offset() < fetchOffset) {
  // Log diverged
  return ValidOffsetAndEpoch.diverging(localEndOffset);
}
```

3. **Truncation** (in `handleFetchResponse`):
```java
if (divergingEpoch.epoch() >= 0) {
  long truncationOffset = log.truncateToEndOffset(divergingEpoch);
  log.truncateTo(truncationOffset);  // Remove conflicting entries
  partitionState.truncateNewEntries(truncationOffset);
  // Retry fetch from truncated point
}
```

**Safety guarantee:**
- If validation succeeds, followers will have same entries as leader
- Epoch ensures each leader's entries are unique
- Previous leaders' entries never conflict once epoch increases

### Durability Guarantees

**Write durability:**
1. **Leader**: Appends via `log.appendAsLeader()` are atomic
   - All records in batch written together
   - Batch fsync'd immediately (or group commit)
2. **Follower**: Voter replicas fsync on append (in `appendAsFollower`)
   - Non-voters may not fsync (observers don't affect safety)
   - Leader only counts voters for HWM
3. **Quorum write**: Entry only committed when replicated to quorum
   - HWM only advances when majority has entry
   - No loss even if leader crashes

**Read durability:**
1. State machine only reads offset <= HWM (committed entries)
2. Can never un-commit an entry (HWM monotonically increases)
3. If partition occurs, new leader must have previously committed entries

### Conflicting Entry Prevention

**Entries never conflict because:**
1. **Epoch uniqueness**: Each leader has unique epoch
2. **Log prefix property**: If follower has offset O with epoch E, leader must have same entry at O with E to replicate further
3. **Truncation on divergence**: Followers truncate conflicting entries immediately upon divergence detection

**Example conflict resolution:**
```
Leader (epoch 5):  [A, B, C, D]
Follower (epoch 4): [A, B, X, Y]  // X,Y from old leader

Follower sends: fetchOffset=3, lastFetchedEpoch=4
Leader validates: endOffsetForEpoch(4) = offset 1 (only A,B have epoch<=4)
Returns: divergingEpoch(epoch=4, endOffset=2)  // "truncate to offset 2"

Follower: truncates to offset 2 → [A, B]
Next fetch: fetchOffset=2, lastFetchedEpoch=4
Leader: now sends [C, D] (epoch 5 records)
```

### Timeout and Retry Handling

**Fetch request timeouts** (in `RequestManager`):
1. Request sent via `onRequestSent(node, createdTime, expiryTime)`
2. If `currentTime > expiryTime`:
   - `hasRequestTimedOut()` returns true
   - `reset(node)` clears connection state
   - Follower retries with different node (bootstrap)
3. Backoff before retry: `retryBackoffMs` (default 100ms)

**Append future timeouts** (in `appendPurgatory`):
1. Append waits in purgatory for HWM to advance
2. If timeout expires before HWM advances:
   - Future completes with exception
   - App callback gets error
   - Can retry append on next leader
3. Timeout: `Integer.MAX_VALUE` effectively (no timeout in practice)

**Leader election timeout** (in `FollowerState`):
1. If fetch doesn't arrive within `fetchTimeoutMs`:
   - `hasFetchTimeoutExpired()` returns true
   - Follower transitions to Prospective (starts pre-vote)
   - Prevents waiting forever for dead leader
2. Timeout: 300ms-10s (configurable)

**Check quorum timeout** (in `LeaderState`):
1. Requires majority fetch within `checkQuorumTimeoutMs` = fetchTimeoutMs * 1.5
2. If majority doesn't fetch:
   - `timeUntilCheckQuorumExpires() == 0`
   - Leader resigns (becomes zombie)
   - Prevents leader from operating in partition
3. Timeout: 450ms-15s (1.5x fetch timeout)

### Network Partition Handling

**Scenario: Leader and followers split into two partitions**

**Follower partition (no leader):**
1. No Fetch responses arrive
2. `fetchTimeoutMs` expires
3. Transitions to Prospective state
4. Starts election (Phase 1)
5. Partition elects new leader

**Leader partition (no quorum):**
1. Majority of voters don't fetch
2. `checkQuorumTimeoutMs` expires
3. `timeUntilCheckQuorumExpires() == 0`
4. Leader transitions to ResignedState
5. Sends EndQuorumEpoch, resigns gracefully
6. Cannot accept appends anymore

**Preventing zombie leader:**
- Leader constantly checks quorum liveness
- Only leader's quorum partition can win election
- Other partitions' leaders will resign on check quorum timeout
- Network heal: Both leaders try to replicate
  - Higher epoch wins (new leader elected in higher epoch)
  - Lower epoch followers/leaders step down

---

## 9. Integration Points with Phase 1 (Election)

### Phase 1 → Phase 2 Transition

**Election completes (Phase 1):**
1. Candidate receives votes from quorum (Phase 1: VoteRequest/VoteResponse)
2. Transitions to LeaderState via `maybeTransitionToLeader()`
3. Creates new `BatchAccumulator` with new epoch

**Leader initialization:**
1. Initializes all `voterStates` with `hasAcknowledgedLeader = false` (except self)
2. Starts `beginQuorumEpochTimer` to send BeginQuorumEpoch requests
3. Appends leader change control record via `appendStartOfEpochControlRecords()`
4. Sets `epochStartOffset = log.endOffset()` (for HWM computation)

**Follower transitions from election:**
1. Receives BeginQuorumEpoch request from new leader (Phase 2 initialization)
2. Transitions to FollowerState via `transitionToFollower()`
3. Sets leader endpoints for Fetch requests
4. Resets fetch timeout
5. Starts sending Fetch requests to leader

### Class Dependencies: Phase 1 → Phase 2

**Phase 1 classes used in Phase 2:**
- `ElectionState`: Tracks leader and epoch info
- `VoterSet`: Replica set for HWM computation
- `QuorumState`: Main state machine coordinating phases

**Phase 1 → Phase 2 via QuorumState:**
```
QuorumState.transitionToLeader()
  ↓
Calls KafkaRaftClient.onBecomeLeader()
  ↓
Creates LeaderState<T> with VoterSet from Phase 1
  ↓
Initializes voterStates from VoterSet.voterNodes()
```

### Phase 2 ← Phase 1 Cycle

**If leader fails (Phase 2):**
1. Followers detect fetch timeout
2. Transition to Prospective state
3. Start election (Phase 1) from higher epoch
4. New leader elected
5. New BeginQuorumEpoch sent (Phase 2 initialization)
6. Back to Phase 2

**Leadership changes:**
- Each leader has epoch > previous
- Epoch persisted in control records
- On restart, replica recovers last known epoch
- Used to determine initial state

### Control Records: Phase 1/2 Integration

**LeaderChangeRecord** (appended when leader starts):
```
Leader appends at epoch start:
  - appendStartOfEpochControlRecords()
  - Creates LeaderChangeMessage with:
    - leaderId: current leader
    - voters: voter set at election time
    - grantingVoters: voters who voted for this leader
  - Stored in control record batch
  - Used by state machine to track leadership history
```

**VotersRecord** (appended during Phase 2 for reconfig):
```
If reconfiguration ongoing:
  - appendVotersRecord(voterSet)
  - Updates voter set stored in log
  - Used for next election (Phase 1)
```

---

## 10. Implementation Wave Breakdown

### Dependency Analysis

**Wave dependencies:**
```
Wave 1: Core data structures ← (no dependencies)
Wave 2: Append mechanism ← Wave 1
Wave 3: Fetch mechanism ← Wave 1, Wave 2
Wave 4: HWM computation ← Wave 1, Wave 2, Wave 3
Wave 5: Safety & integration ← Wave 1, Wave 2, Wave 3, Wave 4
```

### Wave 1: Core Classes (Week 1) - ~35 hours

**Objectives:**
- Implement immutable data structure classes
- Implement replica tracking structures
- Implement log interfaces

**Classes to implement:**
1. `ReplicaKey` (~100 lines)
   - Simple value object, no dependencies
   - Just ID and directory UUID
   
2. `LogOffsetMetadata` and `OffsetMetadata` (~100 lines)
   - Simple wrapper around offset
   
3. `LogAppendInfo` (~30 lines)
   - Simple record with first/last offset
   
4. `ValidOffsetAndEpoch` (~100 lines)
   - Result type for validation
   
5. `Batch<T>` (~230 lines)
   - Immutable batch with factory methods
   - No dependencies on other Kafka KRaft classes
   
6. `LeaderAndEpoch` (~50 lines)
   - Simple record of leader and epoch
   
7. `Isolation` enum (~10 lines)
   - Trivial enum
   
8. `ReplicatedLog` interface (~320 lines)
   - No implementation, just interface
   - Define contracts for append, read, truncate, HWM
   
9. `LeaderState.ReplicaState` inner class (~150 lines)
   - Replica progress tracking
   - Depends on: ReplicaKey, LogOffsetMetadata, Endpoints

**Complexity: LOW**
- Mostly data structure definitions
- No complex algorithms
- Good foundation for Wave 2+

**Testing:**
- Unit tests for immutability
- Serialization/deserialization if needed
- Comparators for sorting

---

### Wave 2: Append Mechanism (Week 2) - ~40 hours

**Objectives:**
- Implement batch accumulation
- Implement batch building
- Implement leader append to log
- Implement follower append to log

**Classes to implement:**
1. `BatchBuilder<T>` (200+ lines)
   - Builds single batch in ByteBuffer
   - Depends on: Compression, RecordSerde
   - Needs: serialize records to bytes, build record batch header
   
2. `BatchAccumulator<T>` (300+ lines)
   - Accumulates records into batches
   - Depends on: BatchBuilder, MemoryPool, Time
   - Needs: linger timer, batch draining, memory management
   - Key methods: append(), drain(), timeUntilDrain()
   
3. `LogAppendInfo` and append flow in KafkaRaftClient (~200 lines)
   - append(epoch, records) method
   - appendBatch() helper
   - appendAsLeader() and appendAsFollower() methods
   - Purgatory registration for await-commit
   
4. `ReplicatedLog` implementation (partial, ~500 lines)
   - Implement appendAsLeader(), appendAsFollower()
   - Implement basic read(), truncateTo()
   - Implement HWM management (basic)
   - Can use mock for actual file I/O initially

**Complexity: MEDIUM-HIGH**
- Batch serialization/compression
- Memory pool management
- Purgatory futures for commit tracking

**Testing:**
- Batch accumulation ordering
- Epoch assignment
- Append atomicity
- Memory pool exhaustion handling

---

### Wave 3: Fetch Mechanism (Week 3) - ~45 hours

**Objectives:**
- Implement FetchRequest/Response building
- Implement leader fetch handler
- Implement follower fetch sender
- Implement offset/epoch validation

**Classes to implement:**
1. `RequestManager` (300+ lines)
   - Track per-node connection state
   - Timeout tracking, backoff management
   - Depends on: RequestManager needs no KRaft types (just Node, time)
   - Key methods: isReady(), isBackingOff(), hasRequestTimedOut()
   
2. `handleFetchRequest()` in KafkaRaftClient (~300 lines)
   - Parse FetchRequest
   - Validate cluster ID, partition
   - Call log.validateOffsetAndEpoch()
   - Return FetchResponse with records or diverging epoch
   - Update replica state on fetch
   
3. `handleFetchResponse()` in KafkaRaftClient (~200 lines)
   - Parse FetchResponse
   - Handle diverging epoch (truncation)
   - Handle snapshot ID
   - Append records via appendAsFollower()
   - Update HWM from response
   
4. `log.validateOffsetAndEpoch()` (~100 lines)
   - Check if offset/epoch valid
   - Return diverging point if diverged
   - Return snapshot ID if before log start
   
5. Fetch request building (~100 lines)
   - buildFetchRequest() helper
   - maybeSendFetchOrFetchSnapshot() for followers
   - pollFollowerAsVoter() logic for sending fetches

**Complexity: MEDIUM-HIGH**
- Complex validation logic
- Divergence detection
- Snapshot handling integration

**Testing:**
- Valid offset/epoch scenarios
- Divergence detection
- Log truncation on divergence
- Fetch timeout handling
- Purgatory completion on new data

---

### Wave 4: High Water Mark Computation (Week 4) - ~35 hours

**Objectives:**
- Implement HWM computation algorithm
- Implement replica state updates
- Implement HWM propagation

**Classes to implement:**
1. `LeaderState` with full implementation (~500 lines)
   - Initialization with VoterSet
   - voterStates map management
   - observerStates map management
   - updateLocalState() method
   - updateReplicaState() method
   - maybeUpdateHighWatermark() - core algorithm
   - updateCheckQuorumForFollowingVoter()
   - needToSendBeginQuorumRequests()
   - timeUntilCheckQuorumExpires()
   
2. `onUpdateLeaderHighWatermark()` in KafkaRaftClient (~100 lines)
   - Update log HWM
   - Complete append purgatory futures
   - Complete fetch purgatory futures
   - Notify listeners
   
3. `FollowerState.updateHighWatermark()` (~50 lines, mostly already there)
   - Validate HWM increases
   - Store HWM
   
4. `updateLeaderEndOffsetAndTimestamp()` in KafkaRaftClient (~50 lines)
   - Update local replica state
   - Trigger HWM computation
   - Notify purgatory
   
5. `beginQuorumEpochTimer` management (~100 lines)
   - Track replica acknowledgement
   - Send BeginQuorumEpoch requests

**Complexity: MEDIUM**
- Sorting algorithm for median
- Quorum liveness tracking
- Timer management

**Testing:**
- HWM computation correctness (median calculation)
- HWM monotonicity (never decreases)
- Quorum check timeout
- Purgatory completion on HWM advance
- Observer exclusion from HWM

---

### Wave 5: Safety & Integration (Week 5) - ~45 hours

**Objectives:**
- Integrate with Phase 1 (election)
- Implement safety checks
- Handle failure scenarios
- Implement replication loop

**Classes to implement:**
1. Integration with QuorumState (~200 lines)
   - transitionToLeader() initialization
   - transitionToFollower() initialization
   - Epoch tracking across transitions
   - Control record appending
   
2. Poll loop integration (~300 lines)
   - pollLeader(): maybeAppendBatches, maybeSendBeginQuorumEpoch
   - pollFollower(): maybeSendFetchToBestNode
   - State machine callbacks (listeners)
   - Graceful shutdown handling
   
3. Failure handling (~150 lines)
   - Network partition handling
   - Leader resignation on quorum loss
   - Follower election timeout on leader loss
   - Backoff and retry logic
   
4. Optimization and tuning (~100 lines)
   - Batch linger timing
   - Fetch max wait time
   - Quorum timeout configuration
   - Memory pool sizing
   
5. Metrics and observability (~100 lines)
   - Append latency tracking
   - Fetch latency tracking
   - HWM updates tracking
   - Quorum health metrics

**Complexity: MEDIUM-HIGH**
- Coordinating multiple async operations
- Handling state transitions
- Error conditions

**Testing:**
- End-to-end replication scenarios
- Leader failure and recovery
- Network partition scenarios
- Concurrent append and fetch
- Listener callback correctness

---

### Implementation Timeline Summary

| Week | Focus | Classes | LOC | Risk |
|------|-------|---------|-----|------|
| 1 | Core types | ReplicaKey, Batch, LogOffset, etc. | ~1000 | LOW |
| 2 | Append | BatchAccumulator, BatchBuilder, append() | ~1200 | MED |
| 3 | Fetch | RequestManager, handleFetch(), validate | ~1000 | MED-HIGH |
| 4 | HWM | LeaderState, HWM computation, timers | ~900 | MED |
| 5 | Integration | Poll loops, transitions, failures | ~850 | HIGH |
| **Total** | | | **~5000 lines** | |

### Testing Strategy by Wave

**Wave 1:** Unit tests for immutability, serialization
**Wave 2:** Unit tests for batching, memory management; mock log tests
**Wave 3:** Request validation tests, mock network tests
**Wave 4:** HWM computation tests (many test cases for different quorum sizes)
**Wave 5:** Integration tests, simulation tests, chaos tests

### Known Challenges

1. **Wave 2**: Batch serialization with compression is complex
2. **Wave 3**: Divergence detection logic has many edge cases (snapshot, empty log, etc.)
3. **Wave 4**: HWM computation sorting and quorum timeout coordination
4. **Wave 5**: Async state transitions with concurrent requests is error-prone

### Success Criteria per Wave

**Wave 1:** All classes compile, basic unit tests pass
**Wave 2:** Batches accumulate and drain correctly, append offsets monotonic
**Wave 3:** Fetch validates offsets, divergence triggers truncation
**Wave 4:** HWM computed correctly for quorum sizes 1-5, advances monotonically
**Wave 5:** Full replication loop works, leader/follower transitions correct, failures handled

---

## Summary

Phase 2 (Log Replication) is a sophisticated distributed system component that:

1. **Uses pull-based replication** (followers fetch) rather than push (leader sends)
2. **Computes HWM via quorum median** ensuring safety under partitions
3. **Implements log validation** to detect and resolve divergence
4. **Provides multiple isolation levels** (COMMITTED vs UNCOMMITTED)
5. **Integrates with Phase 1** (election) via epoch and control records
6. **Enforces safety properties** through offset/epoch matching and truncation
7. **Handles failures** gracefully via timeouts and state transitions
8. **Supports reconfiguration** via voter set control records

The implementation is best approached in dependency order: data structures → append → fetch → HWM → integration, with thorough testing at each stage to catch subtle replication bugs early.

