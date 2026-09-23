<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# FLIP draft — Pluggable accelerated execution for bounded batch Table API/SQL

**Status:** draft design note. Not a FLIP, not a PR, not a proposal that has been
put to the Flink dev list. It exists so that the execution contract can be
reviewed before more operators are added to the implementation.

**Scope of this document.** It describes what Flink would have to contain. The
TornadoVM provider, its kernels, cuDF, cuBLAS and CUDA appear here only as the
first implementation that exercises the contract, and nothing in the contract
names them. A reviewer needs no GPU, no CUDA installation and no TornadoVM
checkout to assess anything below.

**Companion documents.** `TASKS.md` (what is being built), `VERIFY.md` (what
currently passes, and what the passing tests do *not* prove),
`../flink-tornadovm-evaluation.md` (the architectural review this responds to).

---

## 1. Scope

### 1.1 In scope

- **Bounded batch only.** Table API and SQL, `RuntimeExecutionMode.BATCH`.
  `GpuBatchOnlyTest` pins the refusal in streaming mode.
- **A subtree of the optimised plan**, offered to a provider as a device-neutral
  IR tree. Today the planner can build: `Project`, `Filter`, ungrouped and
  grouped `Aggregate`, `Sort`, `Join` (one equi-key), and `OverAggregate`.
- **A closed expression vocabulary.** `AccelFunction` is an enum of 38 constants.
  A catalog function, a user-defined function, and any built-in not in that enum
  *cannot be represented* and so cannot be offered. This is structural, not a
  check someone must remember.
- **A closed type vocabulary.** Fixed-width logical types only. Anything else
  fails IR construction and the node runs on the CPU with a recorded reason.

### 1.2 Explicitly not claimed

- **No streaming acceleration.** No claim, no partial support, no "works if".
- **No exactly-once claim.** An accelerated operator takes no checkpoint of its
  own device state; the batch failover unit is the task.
- **No general SQL coverage.** Refusal is the expected outcome for most queries
  and is not an error condition.
- **No universal transparent fallback.** See §4.3: the fallback window is
  precisely defined and is narrower than "any device failure".
- **Compiled plans do not accelerate.** See §7.

### 1.3 The user-facing invariant

The **query author** writes standard Flink SQL. No hint, no annotation, no
per-query option, no SQL syntax extension, no GPU-specific API. Every knob in
§1.4 is set by whoever runs the cluster.

The one sanctioned exception: a nullable expression input may be refused, with
`EXPLAIN` naming `NOT NULL` in the DDL as the fix. That is a schema statement the
author would make anyway, not a device instruction. (Since IR v2 the planner no
longer refuses nullable inputs itself; the provider decides.)

### 1.4 Configuration surface

All under `table.exec.accelerator.*`, all environmental:

| key | default | what it decides |
|---|---|---|
| `enabled` | `false` | whether subtrees are offered at all |
| `min-speedup` | `1.3` | policy margin before an unfamiliar path is preferred |
| `batch-size` | `262144` | rows staged before a launch |
| `require-device` | `false` | whether the slot request carries the device requirement |
| `device.slot-sharing-group` | `accelerator` | which group carries it (cannot be `default`) |
| `device.resource-name` | `gpu` | external resource name to request |
| `device.resource-amount` | `1.0` | how much of it one slot needs |
| `device.cpu-cores`, `device.task-heap` | none | mandatory when `require-device` |
| `device.task-off-heap` | `4 gb` | off-heap for that slot |
| `fuse-aggregate` | `false` | whether an ungrouped `SUM` fuses into the `Calc` below |

**Open question 1.4-a.** `device.*` is nine keys to express "put this where the
hardware is". If resource scheduling is solved properly (§3) most of them should
disappear into the scheduler's own vocabulary rather than the table layer's.

---

## 2. SPI posture

### 2.1 The decision to make

The review's first blocker: the API is named neutrally (`AcceleratorProvider`,
`AccelNode`, `table.exec.accelerator.*`) while much of the implementation is
named for GPUs (`GpuOrCpuCalcOperatorFactory`, `plan/gpu/`, `== GPU Offload ==`
in `EXPLAIN`). One of the two has to give.

**This draft proposes: provider-neutral contract, GPU-named implementation, and
the implementation names are internal and should be renamed before proposal.**
The contract genuinely contains nothing device-specific — a provider could be an
FPGA bitstream loader or a hand-written SIMD library — and the one place a
device is named is a config key the operator sets to match their TaskManager's
`external-resource.<name>`, which is Flink's existing vocabulary.

The internal GPU names are a straightforward rename and are listed as such rather
than defended. `== GPU Offload ==` in `EXPLAIN` output is user-visible and must
change.

### 2.2 Provider identity

`AcceleratorProvider.name()` returns a stable string used in `EXPLAIN`, logs and
the decision metric. It is presently unvalidated and unnamespaced.

**Proposed:** a reverse-DNS or vendor-prefixed identifier, validated at discovery,
rejected if two discovered providers share one.

### 2.3 Discovery, ordering and selection — **unresolved**

Current behaviour, stated plainly: `AcceleratorProviders.find()` loads via
`ServiceLoader`, takes **the first provider the iterator yields**, logs a warning
if there is more than one, and says in that warning that choosing between them is
not specified. That is adequate for one provider and is not a durable SPI.

The minimum a proposal must add:

1. A deterministic order. `ServiceLoader` iteration order is classpath order,
   which is not a contract.
2. A selection policy: ask every provider and take the best claimed cost, or take
   a configured preference list, or refuse when more than one matches. Asking
   every provider is the only one that uses the cost model already present, and
   is what this draft proposes.
3. A statement of what happens when a provider's `accept` is slow. It runs during
   task start-up, once per offloaded subtask, on the task thread.

**Open question 2.3-a.** Is one accelerator per TaskManager the model, or one per
*slot*? The device requirement is attached to a slot sharing group, which implies
per-slot, but discovery is per-classloader, which implies per-TaskManager.

### 2.4 Capability negotiation

Two mechanisms exist, and they are different things:

- **IR version.** `AccelIrVersion.CURRENT` is an int the provider compiles in.
  A provider reporting less than Flink emits is ignored entirely, with a log
  line. This is coarse and it is deliberately coarse: it catches "a provider that
  has never been asked to serve this node kind", which is the failure that
  produces wrong answers rather than refusals. Versions 1–7 are documented in
  `AccelIrVersion`, each recording exactly what an older provider would not have
  considered.
- **Per-subtree refusal.** `accept()` returning `Optional.empty()` is the
  ordinary answer. No device, no driver, an unsupported operation, an unfavourable
  shape — all the same answer, and all mean "run the generated operator".

**What is missing:** a provider cannot *declare* its capabilities, only
demonstrate them one subtree at a time. That is enough for execution and not
enough for `EXPLAIN` (the planner cannot say what would be offloadable) or for a
conformance suite (§5 has to test through `accept`). This draft does not propose
adding a declaration yet, and flags it: a declarative capability set is the
natural place for the semantic contract to live, and designing it before there
are two providers would be guessing.

### 2.5 Stability annotation — **the real compatibility question**

`AcceleratorProvider`, `AcceleratorPlan`, `AcceleratorCost`, `AcceleratorContext`
and the whole `AccelNode` hierarchy are `@PublicEvolving`. Two of them expose
types that make that expensive:

- `createOperator` returns `StreamOperatorFactory<RowData>`, a runtime-internal
  type, so a provider is coupled to Flink's operator framework;
- the IR and the context traffic in `RowData` and `RowType`.

**Proposed:** ship the first version as `@Experimental`, with an explicit
statement that the operator-factory return type is the known wart and that a
narrower provider-facing execution interface (consume batch, produce batch) is
the intended direction. `@PublicEvolving` on a type that returns
`StreamOperatorFactory` is a promise Flink would regret.

---

## 3. Resource scheduling

### 3.1 Two different things a user may want

- **Best effort.** "Use a device where there is one." No scheduling involvement.
  A TaskManager without a device runs the generated operator and says so in a
  metric. This works today and is the default.
- **Required device.** "This job is pointless without hardware." Needs the slot
  request to carry the requirement and the scheduler to honour it.

Flink has no soft resource constraint: a slot request either carries the
requirement or it does not. So these are two modes, not two settings of one.

### 3.2 What actually happens today

`GpuPlacementSchedulerIT` asserts all three of these:

1. The requirement **reaches the JobGraph**. The vertex carrying the offloaded
   node has a slot sharing group whose `ResourceProfile` names the external
   resource.
2. Under `jobmanager.scheduler: Default`, it is **enforced**. A TaskManager
   declaring no device never satisfies the request and the job waits. That is the
   documented consequence of asking.
3. Under **AdaptiveBatch** — which is what a batch job gets unless told otherwise
   — the same JobGraph produces `ResourceRequirement{resourceProfile=
   ResourceProfile{UNKNOWN}}`, and **the job runs anywhere**.

Point 3 is the blocker. Offload is batch-only; AdaptiveBatch is the batch
default; so the scheduler that discards the requirement is the one this feature
meets by default. `require-device` is therefore **not a dependable contract** and
this draft does not present it as one.

### 3.3 What would make it valid

Either:

- **(a)** AdaptiveBatch propagates a non-UNKNOWN `ResourceProfile` for vertices
  whose slot sharing group declares one. This is a change in
  `flink-runtime`, outside the table layer, and it interacts with the
  dynamic parallelism decisions AdaptiveBatch makes after the JobGraph exists —
  which is why the profile is UNKNOWN in the first place. It needs its own
  design and probably its own FLIP.
- **(b)** Requesting a device forces `Default` and says so. Honest, and it costs
  the user adaptive batch scheduling, which for a large batch job is a real loss.

**Proposed for the first version:** ship **best effort only**. Remove
`require-device` and the eight `device.*` keys from the proposed surface, keep
them in the research branch, and state that a required-device mode depends on
(a). A feature whose flagship guarantee does not hold under the default scheduler
should not ship that guarantee.

### 3.4 A second scheduling cost, unresolved

`device.slot-sharing-group` cannot be `default`: Flink's
`setSlotSharingAndCoLocation` takes the *region* group for a vertex named
`default` and never consults the registered resource, so registering resources
under that name is silently no requirement at all. A distinct group therefore
severs the offloaded node from its source — `isChainable` refuses to chain across
groups — and every workload measured on this branch is bound by reading that
source.

**So asking for a device costs a chain break.** This is not incidental; it is
plausibly larger than what offload wins for anything but transcendental
arithmetic. It is a second reason the first version should be best-effort.

---

## 4. Memory and failure model

### 4.1 Managed memory ownership

An accelerated node declares managed memory at plan time
(`BatchExecCalc#declareStagingMemory`, derived from row width × `batch-size`). At run
time the provider receives an `AcceleratorContext` offering:

- `providesOffHeap()` — whether a reservation exists at all. `false` is ordinary:
  no slot behind the harness, a plan compiled before the declaration existed, a
  slot too small.
- `allocateOffHeap(bytes)` — a zeroed direct `ByteBuffer` carved from the
  reservation. Stable address, so a provider can hand it to a
  `MemorySegment`, a JNI pointer or a device mapping. Flink names none of those,
  because it is compiled at a language level where some are preview features.
- `maxBatchSize()` — ceiling in rows for an operator that streams batches.
- `stagingBytes()` — the whole reservation, for an operator that must hold its
  entire input and therefore cannot use a per-batch ceiling.

The rule the SPI states: **a provider should stage through this, not allocate
privately.** Private allocation is invisible to the slot budget, so two
accelerated operators in one slot each believe they own the machine.

`ManagedOffHeapArena` owns the reservation and releases it when the task ends —
success, failure or cancellation.

### 4.2 Maximum buffered data

Two operator shapes must hold their whole input, and both size that undertaking
against the staging reservation at `open()`:

- **Sort.** A sorted batch is only a sorted run, so a sort cannot work a batch at
  a time.
- **Join.** The build side has to be resident to probe against.

Each checks the planner's estimate against the reservation at `open()` and
declines there if it does not fit — which is a **pre-consumption decline**
(§4.3 case 1), recoverable, and the right answer, because Flink's own
`SortOperator` and `HashJoinOperator` spill and these do not.

**When the estimate was low**, they used to materialise what was staged onto the
ordinary heap — an `ArrayList`, a `HashMap<Integer, List<BinaryRowData>>` — and
keep going. That answer was correct and bounded by nothing: the reservation
stopped applying at the moment of overflow, so the true bound became the task
heap, and a cardinality miss could take the TaskManager down rather than merely
run slowly. The cardinality estimate is exactly the number the adaptive scheduler
is entitled to contradict.

**Resolved (implemented).** Overflow now **fails the task** with a distinguished
`StagingCapacityExceededException` carrying the estimate and the capacity that
disagreed. The undertaking is kept or the task fails, and the heap bound holds at
every point.

Failing is acceptable rather than merely strict because of §4.3's retry rule: a
retried attempt is not offered to an accelerator, so batch failover re-runs the
task on the operator that spills. One attempt is lost and the query finishes.
Covered by `StagingCapacityTest` (six cases, no device required).

A device failure is a different case and stays survivable: by then every row is
already staged, so at most `capacity` of them are held, and finishing on the host
is bounded by the reservation that was already taken.

### 4.3 Failure states

Three cases, which the contract must keep apart:

1. **Pre-consumption decline.** `accept()` returns empty, or the accelerated
   operator fails in `open()` or on its **first record**. Nothing has been
   consumed that cannot be replayed — the first record is still in hand — so the
   generated operator takes over, the staging reservation is released first (it
   needs the managed memory), and the subtask finishes on the CPU. This is what
   `FallbackToCpuOperator` implements, and it is worth having because `open()` is
   where kernel compilation and device allocation happen, which is where failure
   is likeliest.
2. **Recoverable whole-batch retry** — **does not exist.** A device failure
   arriving mid-stream cannot be recovered in the operator: the batch's rows have
   been staged into columnar buffers, none emitted, and they cannot be
   reconstructed because the operator kept only the columns the expression
   referenced. Switching operators there would silently drop rows. Implementing
   this needs the rows kept replayably — a second copy of every batch — plus a
   provider signal for "this batch is safely emitted".
3. **Nonrecoverable failure.** Anything after the first record. **Fatal to the
   task**, deliberately, because the alternative is a wrong answer. The two
   sources are a device error mid-stream and the capacity miss of §4.2.

**The retry rule (implemented).** A task attempt after the first is not offered
to an accelerator at all; it runs the code-generated operator and reports
`RETRY_ATTEMPT`. Without it, a deterministic device failure — a kernel that will
not compile on this machine, a driver a restart does not clear, a partition
larger than the provider undertook to hold — kills every attempt identically and
spends the job's whole restart budget before failing the job, while a CPU
implementation that would have finished sat unused in the JobGraph. With it, case
3 becomes a recoverable job-level fallback: one slow attempt, then success.

It is deliberately **not** conditioned on why the previous attempt died. A task is
retried for reasons that have nothing to do with the accelerator, and declining
in those cases costs one slow attempt; distinguishing them needs the failure
cause to survive into the next attempt, which is a scheduler-side change.

**The contract, stated:** cases 1 and 3 are supported, case 2 does not exist, and
case 3 is recoverable at job level but never within the task.

### 4.4 Decline reasons — structured, not prose (implemented)

A decline used to be only a `String` on `AcceleratorChoice.Outcome`, formatted for
a human and published as the `acceleratorDecision` gauge. It was not enumerable,
so it could not be counted per reason.

`AcceleratorDeclineReason` (in `flink-table-common`, beside the IR) is now the
closed runtime vocabulary: `ACCELERATED`, `NO_PROVIDER`, `PROVIDER_DECLINED`,
`PROVIDER_THREW`, `BELOW_MIN_SPEEDUP`, `FIXED_COST_NOT_REPAID`, `TOO_FEW_ROWS`,
`OPERATOR_CREATION_FAILED`, `RETRY_ATTEMPT`, `FELL_BACK_AT_OPEN`,
`FELL_BACK_AT_FIRST_RECORD`, `PROVIDER_STOPPED_USING_DEVICE`.

Both are published: `acceleratorDecision` (the sentence, with the numbers and the
provider's name) and `acceleratorDecisionCode` (the constant, for the histogram).
The code is read through the fallback guard, so a subtask that started on a device
and fell back reports the fallback point rather than the verdict it started with.

**Still open:** the *planner's* reasons for not selecting a node — "not
expressible on device", "compiled plan" — remain free text inside
`GpuOffloadDecision.Verdict` and are rendered into `EXPLAIN` as such. They are a
different vocabulary (properties of the query, not of a machine) and giving them
the same treatment is a separate, mechanical change.

---

## 5. Semantics

### 5.1 The risk

Device arithmetic can differ from CPU SQL on floating-point reassociation, NaN,
signed zero, infinities, decimal conversion, rounding, overflow, casts,
timestamps, strings, divide-by-zero and three-valued null logic. A small
numerical difference is not a small problem: it moves a filter boundary or a
grouping key, and the wrong answer is silent.

### 5.2 Current posture

Conservative refusal, enforced structurally: a closed function enum, a closed
node hierarchy (package-private constructors), fixed-width types only, and
IR-version gating for every vocabulary addition. `DifferentialExpressionIT` and
`GpuNullRefusalIT` exercise part of the surface.

### 5.3 What a release gate requires

A **provider-independent semantic conformance suite**, living at the
Flink/provider boundary, runnable against any provider, with deterministic small
datasets and adversarial values. Every case is `CPU result == provider result`,
or a clean refusal. Refusal passes; a different answer fails.

Coverage: null and three-valued logic through every operator; NaN in comparison,
ordering and grouping; signed zero; ±infinity; overflow and wraparound per type;
every supported cast, including narrowing and the ones that lose precision;
integer division and `MOD` truncation direction; division by zero; rounding mode
in `FLOOR`/`CEIL`/`CAST`; empty input; single-row input; all-null input; duplicate
keys; and the capacity boundaries of §4.2.

This is Phase 4 work in the execution plan and is the strongest single argument
for the design being real rather than promising.

**Open question 5.3-a.** Floating-point reassociation is the one case where
refusal may be impractical: a device sum of a `DOUBLE` column will not
bit-match a sequential CPU sum, and forbidding that forbids the feature's best
case. The proposal must decide whether SQL `SUM(DOUBLE)` is order-defined in
Flink, and if not, say so explicitly rather than rely on it quietly.

---

## 6. Observability

### 6.1 The split that must be understood

`EXPLAIN` runs in the client and reports what the **planner** decided:
eligibility, which is a property of the query. Whether a device actually served
the node is decided **per subtask on the TaskManager**, against whatever hardware
that machine has. Two subtasks of one job may answer differently, and in a
heterogeneous cluster both are right.

Neither one alone answers "did my query use the accelerator?". Both are needed.

### 6.2 `EXPLAIN` — planner side

An `== GPU Offload ==` section (name must change, §2.1) listing each candidate
subtree, whether it was selected, the counted work profile (e.g. `TIMESx1`), and
where a rejection came from — `not expressible on device`, and a statement that
the cost decision happens on the TaskManager.

### 6.3 Metrics — runtime side

Registered by **Flink**, on the operator the provider returned, so a provider
that publishes nothing still reports:

- `accelerated` — gauge, 1 or 0. Read through the fallback guard, not captured,
  so a subtask that started on a device and fell back stops claiming otherwise.
- `acceleratorDecision` — gauge, the reason string.

### 6.4 Gaps

- No *counter* per structured reason. The code is published as a gauge
  (§4.4); counting occurrences across a job is a reporter-side aggregation and
  Flink publishes nothing per-reason itself.
- No metric for the selected provider or device identity.
- **No data-movement metrics at all** in Flink: staged rows, bytes to device,
  bytes back, device time. The provider measures these (`OffloadMetrics`) and
  Flink cannot see them. The review's performance requirements
  (source/read, staging, H2D/D2H, device, drain, end-to-end) are unmeetable from
  Flink's side today.

**Proposed:** an optional `AcceleratorMetrics` the provider may report into, with
Flink publishing whatever is supplied under a standard naming scheme.

---

## 7. Compiled plans

**Current behaviour: a compiled plan never accelerates.** The offload decision is
a transient field on the ExecNode, absent from the node's JSON. A plan written to
a file and read back has no assignment; the same `CompiledPlan` held in memory
still carries one. So an in-memory plan would offload and a restored one would
not — and a compiled plan exists precisely so the operators do not change under a
running job. Both are therefore refused, with the reason recorded and reported.

Serialising the assignment was considered and rejected: it would pin the IR, the
work profile and the cost model into a format Flink undertakes to keep restorable
across releases, and it still would not reproduce the original verdict, because
that verdict is settled per subtask on the TaskManager. A plan can only record
that a node was **eligible**, which is not the thing a restored plan would be
reproducing.

**Proposed support path:** serialise *eligibility* only — a boolean plus the IR
version the plan was built against — and re-derive the rest at run time. A
restored plan then offloads exactly where the original was eligible, and refuses
if the IR version no longer matches. This is a small, versionable addition, and
it should not be in the first version.

---

## 8. Columnar direction

Row staging (`RowData` → columnar buffers, field by field) is the right prototype
boundary and the wrong long-term one. It imposes a fixed per-field cost that no
amount of device speed removes, which is why the measurements on this branch show
offload winning in proportion to arithmetic per byte read and losing everywhere
else.

The direction, stated without committing to it now:

- **Explicit transition nodes.** A `RowToColumnar` and a `ColumnarToRow` node in
  the plan, visible in `EXPLAIN`, costed by the optimiser. Not an implicit
  conversion inside every accelerated operator. Visibility is the point: the
  conversion is the dominant cost and a plan that hides it cannot be reasoned
  about.
- **Buffer lifetime and ownership.** A columnar batch has one owner at a time.
  Ownership transfers on the edge; the producer may not touch a batch it has
  handed on; the consumer releases it. Reference counting is the alternative and
  is worse, because a leak becomes a slow device-memory exhaustion instead of an
  immediate error.
- **What is not proposed:** a complete columnar execution-node family. Flink
  already has `ColumnarRowData` and vectorized readers for ORC/Parquet; the
  transition nodes should meet that machinery rather than duplicate it.

**Open question 8-a.** Whether the columnar batch type is a new Flink type or an
existing one. This decides whether the accelerator work and the vectorized
reader work converge or diverge, and it is worth answering before either
proceeds.

---

## 9. Summary of unresolved questions

| # | question | blocks |
|---|---|---|
| 2.3-a | one provider per TaskManager or per slot? | multi-provider SPI |
| 2.3 | selection policy among several providers | multi-provider SPI |
| 2.4 | declarative capability set | `EXPLAIN` accuracy, conformance suite |
| 2.5 | `@Experimental` vs `@PublicEvolving`; `StreamOperatorFactory` in the SPI | the whole API commitment |
| 3.3 | AdaptiveBatch and `ResourceProfile` | any required-device guarantee |
| 3.4 | chain break as the price of a device request | whether best-effort is worth it |
| ~~4.2~~ | ~~unbounded heap in sort/join~~ | resolved: overflow now fails the task |
| ~~4.3~~ | ~~no disable-on-retry signal~~ | resolved: a retried attempt is not offered |
| 4.3-b | retry declines regardless of what actually failed | one wasted attempt after any unrelated failure |
| 5.3-a | is `SUM(DOUBLE)` order-defined in Flink SQL? | conformance suite scope |
| 6.4 | data-movement metrics | the review's performance requirements |
| 8-a | columnar batch type: new or existing? | columnar direction |

## 10. What a first version would actually contain

Deliberately smaller than what the branch contains today:

- `Calc` (`Project` + `Filter`) only. Sort, join, over-aggregate and grouped
  aggregate stay research — §4.2 is now safe rather than unbounded, but "fails
  the task and retries on the CPU" is a weaker story than a first version should
  have to defend.
- Best-effort placement only. No `require-device`, no `device.*` keys.
- `@Experimental`, one provider, first-found discovery documented as such.
- Structured decline reasons (`AcceleratorDeclineReason`), the `EXPLAIN`
  section, and the `accelerated` / `acceleratorDecision` /
  `acceleratorDecisionCode` metrics.
- The conformance suite from §5.3, as a release gate rather than an extra.
- Documented: batch only, no exactly-once, compiled plans do not accelerate.

Everything else in the branch stays research until the question that blocks it is
answered.
