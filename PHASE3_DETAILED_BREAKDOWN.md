# PHASE 3: CONFIGURATION CHANGES AND DYNAMIC MEMBER MANAGEMENT
# Comprehensive Analysis - Kafka KRaft Implementation

## EXECUTIVE SUMMARY

Phase 3 enables dynamic cluster topology management in Kafka KRaft (KIP-853). It allows brokers to be added and removed from the metadata quorum without restart, through atomic configuration changes stored as control records in the log.

**Key Implementation Classes**: 47 total
- **Core Classes**: VoterSet, VoterSet.VoterNode, DynamicVoters, DynamicVoter
- **Handlers**: AddVoterHandler, RemoveVoterHandler, UpdateVoterHandler (with state classes)
- **State Management**: KRaftControlRecordStateMachine, VoterSetHistory, LogHistory<T>, TreeMapLogHistory<T>
- **Control Records**: ControlRecord, LeaderChangeMessage, VotersRecord, KRaftVersionRecord
- **Integration**: LeaderState, QuorumState, KRaftVersionUpgrade, ReplicaKey, Endpoints, BatchAccumulator

---

## 1. COMPLETE CLASS CATALOG

### Voter Set Management (5 classes)
- **VoterSet** (519 lines): Immutable representation of voters, methods for add/remove/update
- **VoterSet.VoterNode**: Single voter with id, endpoints, kraft.version support
- **DynamicVoters** (117 lines): Parses "1@host:port:uuid" configuration strings
- **DynamicVoter**: Individual voter record from configuration
- **VoterSetHistory** (148 lines): Historical voter set tracking with overlap validation

### Configuration Handlers (6 classes + 2 state classes)
- **AddVoterHandler** (360 lines): Protocol for adding voter, 10-step algorithm
- **AddVoterHandlerState** (94 lines): Tracks add operation state (voter, endpoints, timeout, future)
- **RemoveVoterHandler** (189 lines): Protocol for removing voter, handles leader resignation
- **RemoveVoterHandlerState** (47 lines): Tracks remove operation state (offset, timeout, future)
- **UpdateVoterHandler** (306 lines): Updates voter endpoints/version, kraft.version 0/1 handling
- (No dedicated UpdateVoterHandlerState - uses handler state directly)

### Control Record Processing (4 classes)
- **ControlRecord** (100 lines): Wrapper for control messages with type and message parsing
- **KRaftControlRecordStateMachine** (336 lines): Main handler for KRAFT_VOTERS and KRAFT_VERSION records
- **LogHistory<T>** (80 lines): Generic interface for offset-keyed value tracking
- **TreeMapLogHistory<T>** (78 lines): Red-black tree implementation using NavigableMap

### Supporting Infrastructure (5 classes)
- **LeaderState** (~1200 lines): Leader state machine with voter management, appendVotersRecord()
- **QuorumState** (~250 lines): Quorum state machine using partitionState for voters
- **KRaftVersionUpgrade** (66 lines): Sealed interface for kraft.version upgrade coordination
- **KRaftVersion** (115 lines): Enum with KRAFT_VERSION_0 and KRAFT_VERSION_1, isReconfigSupported()
- **ReplicaKey**: (int id, Optional<Uuid> directoryId) - unique replica identifier
- **Endpoints**: Manages network endpoints for multiple listeners
- **BatchAccumulator<T>**: Batch accumulation with appendVotersRecord() method

**Total Code**: ~3,500+ lines of implementation

---

## 2. CONFIGURATION CHANGE ARCHITECTURE

### 2.1 Supported Configuration Changes
1. **Add Voter**: New broker joins, caught up, gets majority ack
2. **Remove Voter**: Broker leaves, leader resigns if removed
3. **Update Voter**: Endpoint/listener changes, kraft.version 0/1 handling differs
4. **Kraft Version Upgrade**: 0→1, one-time, enables reconfiguration

### 2.2 Configuration Storage (3-layer)
**Layer 1 - Persistent**: VotersRecord control records in log at specific offsets
**Layer 2 - In-Memory History**: VoterSetHistory (TreeMap<offset, VoterSet>) 
**Layer 3 - Current**: lastVoterSet() from KRaftControlRecordStateMachine

### 2.3 Leader Initialization (5 Phases)
```
Phase 1: Client sends AddRaftVoterRequest
Phase 2: Leader sends ApiVersionsRequest to discover kraft.version support
Phase 3: Leader appends VotersRecord to log
Phase 4: Replication propagates to followers (BeginQuorumEpoch + Fetch)
Phase 5: HWM advancement = commitment, response returned
```

### 2.4 Epoch vs Configuration Version
- **Epoch**: Election term (monotonic)
- **Config Version**: Log offset where VotersRecord written
- **Relationship**: Both included in same batch, new leader has latest config

### 2.5 Key Safety Property: Overlapping Majorities
```
Old Set: {A, B} (majority = 2)
New Set: {A, B, C} (majority = 2)
Overlap: {A, B} satisfies both majorities
→ No partition can have independent majority
```

---

## 3. VOTER SET MANAGEMENT DETAILS

### 3.1 Immutability Pattern
```java
Optional<VoterSet> updated = voterSet.addVoter(newVoter);  // Returns new or empty
Optional<VoterSet> updated = voterSet.removeVoter(key);     // Returns new or empty
```

### 3.2 History Tracking
- **lastValue()**: Current voters (or staticVoterSet fallback)
- **lastEntry()**: Current with offset (useful for snapshots)
- **valueAtOrBefore(offset)**: Historical voter set at specific offset
- **Truncation**: truncateNewEntries() for failed changes, truncateOldEntries() for cleanup

### 3.3 Voter vs Observer
| Aspect | Voter | Observer |
|--------|-------|----------|
| Quorum | Counts toward majority | No |
| Voting | Can vote for leader | No |
| HWM | Acks counted | Not counted |
| Reconfigurable | Via control records | Via config only |
| Use | Consensus participation | Read-only replicas |

### 3.4 Thread-Safety
- voterSetHistory: Protected by object monitor (voterSetHistory)
- kraftVersionHistory: Protected by object monitor (kraftVersionHistory)
- Multiple concurrent snapshot readers OK, single log reader (KRaft driver)
- volatile nextOffset for visibility across threads

---

## 4. CONTROL RECORDS EXPLAINED

### 4.1 Record Types
| Type | Message | Purpose |
|------|---------|---------|
| KRAFT_VOTERS | VotersRecord | Define/update voter set |
| KRAFT_VERSION | KRaftVersionRecord | Update kraft.version (0→1) |
| LEADER_CHANGE | LeaderChangeMessage | Signal election, publish voters |

### 4.2 Writing Control Records
```
Handler → BatchAccumulator.appendVotersRecord(VotersRecord, timestamp)
       → MemoryRecords.withVotersRecord()
       → MemoryRecordsBuilder (isControlBatch=true, epoch=E)
       → ReplicatedLog.append() (persists)
       → KRaftControlRecordStateMachine.updateState() (reads)
```

### 4.3 Propagation Sequence
1. T0: Leader appends VotersRecord at offset N
2. T0+100ms: BeginQuorumEpoch sent (if epoch start)
3. T0+100ms: Fetch RPC sent including VotersRecord
4. T0+150ms: Follower writes to log, KRaftControlRecordStateMachine reads
5. T0+150ms: Follower's lastVoterSet() reflects new voters (uncommitted)
6. T0+250ms: Leader calculates HWM (majority acked offset N)
7. T0+250ms: highWatermarkUpdated() fires, future completes if ackWhenCommitted

### 4.4 Safety During Propagation
- **Uncommitted visible**: OK because leader won't crash (just elected)
- **Follower truncation**: Removes uncommitted via truncateNewEntries()
- **Leader crash**: New leader has record (log completeness), config survives
- **No partial application**: voterSetHistory ordered, atomic at HWM boundary

---

## 5. DYNAMIC TOPOLOGY OPERATIONS

### 5.1 Add Broker (10-Step Algorithm)
```
1. Check HWM exists (leader fenced previous)
2. Check kraft.version >= 1
3. Check no pending changes (isOperationPending)
4. Check broker ID not already in set
5. Send ApiVersionsRequest to discover versions
6. Check broker supports current kraft.version
7. Check broker caught up (isReplicaCaughtUp)
8. Create new VoterSet, append VotersRecord
9. Save offset in AddVoterHandlerState
10. Wait HWM > offset (if ackWhenCommitted), return response
```

### 5.2 Remove Broker (7-Step Algorithm)
```
1-3. Same as add (HWM, kraft.version, pending checks)
4. Remove broker from voter set
5. Append VotersRecord with reduced set
6. Wait HWM > offset (commitment)
7. If leader removed: requestResign() → ResignedState
```

### 5.3 HWM Adjustment
```
Before add: max(acks from {A, B})      [2 needed from 2]
During: max(acks from {A, B, C})       [2 needed from 3, C must fetch first]
After: max(acks from {A, B, C})        [2 needed from 3]
```

### 5.4 Crash Scenarios
| Scenario | Outcome |
|----------|---------|
| Crash before append | Client timeout, config lost, retry |
| Crash after append, HWM < offset | Config lost, new leader truncates, retry |
| Crash after commit (HWM > offset) | Config persists, new leader has it, client may succeed immediately |

---

## 6. IMPLEMENTATION WAVE BREAKDOWN

Recommended 4 waves for Phase 3 reimplementation:

### Wave 1: Foundation (1 week, ~800 LOC)
**Deliverables**:
- VoterSet immutable data structure with add/remove/update
- VoterSet serialization (toVotersRecord/fromVotersRecord)
- ReplicaKey and Endpoints support
- ControlRecord parsing

**Files**:
- VoterSet.java (519)
- VoterSet.VoterNode (nested)
- ReplicaKey.java
- Endpoints.java
- ControlRecord.java

**Tests**: VoterSetTest, serialization round-trip

**Success Criteria**:
- VoterSet operations pass tests
- Serialization preserves all data
- ControlRecord correctly parses all types

### Wave 2: State Machine (1 week, ~600 LOC)
**Deliverables**:
- KRaftControlRecordStateMachine reads voters from log
- VoterSetHistory tracks historical changes
- LogHistory interface and TreeMapLogHistory
- Integration with snapshot creation/loading

**Files**:
- KRaftControlRecordStateMachine.java (336)
- VoterSetHistory.java (148)
- LogHistory.java (80)
- TreeMapLogHistory.java (78)

**Tests**: KRaftControlRecordStateMachineTest, VoterSetHistoryTest

**Success Criteria**:
- Reads VotersRecord from log correctly
- History truncation works for failed changes
- Snapshot loading preserves voter set

### Wave 3: Leader Operations (1 week, ~1000 LOC)
**Deliverables**:
- AddVoterHandler with 10-step algorithm
- RemoveVoterHandler with resignation
- Handler state management in LeaderState
- Timeout handling and operation pending checks

**Files**:
- AddVoterHandler.java (360)
- AddVoterHandlerState.java (94)
- RemoveVoterHandler.java (189)
- RemoveVoterHandlerState.java (47)
- LeaderState modifications for handler state

**Tests**: Handler unit tests with mock LeaderState

**Success Criteria**:
- Add/remove operations complete successfully
- Precondition checks enforce safety
- HWM advancement triggers response
- Timeout expires and clears state
- Leader resignation works

### Wave 4: Integration (1 week, ~500 LOC)
**Deliverables**:
- LeaderState integration (appendVotersRecord, handler reset)
- QuorumState uses voters from partitionState
- UpdateVoterHandler for endpoint changes
- KRaftVersionUpgrade for kraft.version 0/1 transitions
- End-to-end tests

**Files**:
- LeaderState modifications (~500)
- UpdateVoterHandler.java (306)
- KRaftVersionUpgrade.java (66)
- End-to-end integration tests

**Tests**: Full cluster simulation tests

**Success Criteria**:
- E2E broker addition test passes
- E2E broker removal test passes
- Configuration survives leader crash
- Kraft version upgrade works
- All edge cases covered

---

## 7. KEY DEPENDENCIES AND INTERACTIONS

### From Phase 1 (Election)
- **Voter Set**: Determines who can vote in PreVote/Vote
- **Voter Set**: Used to find endpoint of each voter
- **Config Change**: Must not violate overlapping majorities

### From Phase 2 (Replication)
- **LeaderState tracking**: Watches lastFetchTimestamp for caught-up check
- **HWM Calculation**: New voters immediately included
- **Fetch RPC**: Propagates VotersRecord to followers
- **BeginQuorumEpoch**: Includes voter list in LeaderChangeMessage

### To Future Phases
- **Snapshots**: Include voter set at boundary offset
- **Log Compaction**: Must preserve voter set history
- **Observers**: Use static config, not dynamic voters
- **Quorum Verification**: Checks overlapping majorities

---

## 8. SAFETY GUARANTEES CHECKLIST

- [x] No split-brain (overlapping majorities)
- [x] Committed configs not lost (HWM requirement)
- [x] Successive configs consistent (monotonic offsets)
- [x] Leader must have latest config (log completeness)
- [x] Configuration atomicity (single record per change)
- [x] Concurrent change prevention (isOperationPending)
- [x] Idempotent retries (addVoter/removeVoter return Optional)
- [x] Proper truncation on failed changes (truncateNewEntries)
- [x] Timeout handling (maybeExpirePendingOperation)

---

## 9. CRITICAL CODE PATHS

### Adding a Voter
```
AddVoterHandler.handleAddVoterRequest()
  → Check HWM/kraft.version/pending
  → Send ApiVersionsRequest
  → handleApiVersionsResponse() validates support
  → Check caught up status
  → LeaderState.appendVotersRecord(newVoterSet)
    → BatchAccumulator.appendVotersRecord()
      → MemoryRecords.withVotersRecord()
  → Save offset in AddVoterHandlerState
  → Wait for highWatermarkUpdated()
    → Check lastOffset < HWM
    → Future.complete(success)
```

### Configuration Propagation
```
KRaftControlRecordStateMachine.updateState()
  → maybeLoadLog()
    → log.read(nextOffset, UNCOMMITTED)
    → RecordsIterator for batches
    → handleBatch() for each batch
      → batch.controlRecords()
      → ControlRecord parsing
      → Switch on KRAFT_VOTERS type
      → VoterSet.fromVotersRecord()
      → voterSetHistory.addAt(offset, voters)
      → nextOffset advanced
```

---

## 10. TESTING STRATEGY

### Unit Tests
- VoterSet operations (add, remove, update, overlap checks)
- VoterSetHistory truncation and querying
- Handler precondition checks
- ControlRecord serialization/deserialization

### Integration Tests
- Leader appends voter record
- Followers receive and apply record
- HWM advancement triggers callback
- Multiple configuration changes in sequence

### Failure Tests
- Leader crash during add (before append, after append, after commit)
- Network partition during change
- Follower crash and recovery
- Concurrent change attempts (prevented)
- Stale replica rejoin with old config

### Scale Tests
- Large voter set additions/removals
- Rapid succession of changes
- Many concurrent snapshot reads during change
- Log compaction with voter history

---

## 11. IMPLEMENTATION NOTES

1. **Immutability**: VoterSet never modified, always new instance returned
2. **Synchronization**: Minimized locks, use object monitors for voterSetHistory/kraftVersionHistory
3. **Atomic Updates**: Use AtomicReference for KRaftVersionUpgrade state
4. **Safety Checks**: Overlapping majorities checked in VoterSetHistory.addAt()
5. **Timeouts**: maybeExpirePendingOperation() called on every heartbeat
6. **Error Handling**: RequestTimeOut for transient conditions, InvalidRequest for protocol violations
7. **Backward Compat**: kraft.version 0 = no dynamic reconfiguration (update voter in-memory only)

---

## TOTAL ESTIMATE

- **Lines of Code**: ~3,500 (implementation + tests)
- **Implementation Time**: 4 weeks (4 waves of 1 week each)
- **Complexity**: HIGH (coordinating 3 phases + async operations)
- **Risk Areas**: HWM advancement timing, uncommitted config visibility, edge case combinations

---

## REFERENCES

- KIP-853: KRaft Leadership
- VoterSet: /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/VoterSet.java
- Handlers: /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/internals/{Add,Remove}VoterHandler.java
- State Machine: /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/internals/KRaftControlRecordStateMachine.java
- Quorum: /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/QuorumState.java
- Leader: /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java

