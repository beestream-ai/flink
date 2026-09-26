/*
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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds configuration constants used by Flink's table module.
 *
 * <p>NOTE: All option keys in this class must start with "table.exec".
 */
@PublicEvolving
public class ExecutionConfigOptions {

    // ------------------------------------------------------------------------
    //  State Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> IDLE_STATE_RETENTION =
            key("table.exec.state.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Specifies a minimum time interval for how long idle state "
                                    + "(i.e. state which was not updated), will be retained. State will never be "
                                    + "cleared until it was idle for less than the minimum time, and will be cleared "
                                    + "at some time after it was idle. Default is never clean-up the state. "
                                    + "NOTE: Cleaning up state requires additional overhead for bookkeeping. "
                                    + "Default value is 0, which means that it will never clean up state.");

    // ------------------------------------------------------------------------
    //  Source Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_SOURCE_IDLE_TIMEOUT =
            key("table.exec.source.idle-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "When a source do not receive any elements for the timeout time, "
                                    + "it will be marked as temporarily idle. This allows downstream "
                                    + "tasks to advance their watermarks without the need to wait for "
                                    + "watermarks from this source while it is idle. "
                                    + "Default value is 0, which means detecting source idleness is not enabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE =
            key("table.exec.source.cdc-events-duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Indicates whether the CDC (Change Data Capture) sources "
                                                    + "in the job will produce duplicate change events that requires the "
                                                    + "framework to deduplicate and get consistent result. CDC source refers to the "
                                                    + "source that produces full change events, including INSERT/UPDATE_BEFORE/"
                                                    + "UPDATE_AFTER/DELETE, for example Kafka source with Debezium format. "
                                                    + "The value of this configuration is false by default.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "However, it's a common case that there are duplicate change events. "
                                                    + "Because usually the CDC tools (e.g. Debezium) work in at-least-once delivery "
                                                    + "when failover happens. Thus, in the abnormal situations Debezium may deliver "
                                                    + "duplicate change events to Kafka and Flink will get the duplicate events. "
                                                    + "This may cause Flink query to get wrong results or unexpected exceptions.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Therefore, it is recommended to turn on this configuration if your CDC tool "
                                                    + "is at-least-once delivery. Enabling this configuration requires to define "
                                                    + "PRIMARY KEY on the CDC sources. The primary key will be used to deduplicate "
                                                    + "change events and generate normalized changelog stream at the cost of "
                                                    + "an additional stateful operator.")
                                    .build());

    // ------------------------------------------------------------------------
    //  Sink Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<NotNullEnforcer> TABLE_EXEC_SINK_NOT_NULL_ENFORCER =
            key("table.exec.sink.not-null-enforcer")
                    .enumType(NotNullEnforcer.class)
                    .defaultValue(NotNullEnforcer.ERROR)
                    .withDescription(
                            "Determines how Flink enforces NOT NULL column constraints when inserting null values.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<TypeLengthEnforcer> TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER =
            key("table.exec.sink.type-length-enforcer")
                    .enumType(TypeLengthEnforcer.class)
                    .defaultValue(TypeLengthEnforcer.IGNORE)
                    .withDescription(
                            "Determines whether values for columns with CHAR(<length>)/VARCHAR(<length>)"
                                    + "/BINARY(<length>)/VARBINARY(<length>) types will be trimmed or padded "
                                    + "(only for CHAR(<length>)/BINARY(<length>)), so that their length "
                                    + "will match the one defined by the length of their respective "
                                    + "CHAR/VARCHAR/BINARY/VARBINARY column type.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<NestedEnforcer> TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER =
            key("table.exec.sink.nested-constraint-enforcer")
                    .enumType(NestedEnforcer.class)
                    .defaultValue(NestedEnforcer.IGNORE)
                    .withDescription(
                            "Determines if constraints should be enforced for nested fields. Beware that"
                                    + " enforcing constraints for nested fields adds computational"
                                    + " overhead especially when iterating through collections");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<UpsertMaterialize> TABLE_EXEC_SINK_UPSERT_MATERIALIZE =
            key("table.exec.sink.upsert-materialize")
                    .enumType(UpsertMaterialize.class)
                    .defaultValue(UpsertMaterialize.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Because of the disorder of ChangeLog data caused by Shuffle in distributed system, "
                                                    + "the data received by Sink may not be the order of global upsert. "
                                                    + "So add upsert materialize operator before upsert sink. It receives the "
                                                    + "upstream changelog records and generate an upsert view for the downstream.")
                                    .linebreak()
                                    .text(
                                            "By default, the materialize operator will be added when a distributed disorder "
                                                    + "occurs on unique keys. You can also choose no materialization(NONE) "
                                                    + "or force materialization(FORCE).")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long>
            TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW =
                    key("table.exec.sink.upsert-materialize-strategy.adaptive.threshold.low")
                            .longType()
                            .noDefaultValue()
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "When using strategy="
                                                            + SinkUpsertMaterializeStrategy.ADAPTIVE
                                                            + ", defines the number of entries per key when the implementation is changed from "
                                                            + SinkUpsertMaterializeStrategy.MAP
                                                            + " to "
                                                            + SinkUpsertMaterializeStrategy.VALUE
                                                            + ". "
                                                            + "If not specified, Flink uses state-backend specific defaults (300 for hashmap state backend and 40 for RocksDB and the rest).")
                                            .linebreak()
                                            .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long>
            TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH =
                    key("table.exec.sink.upsert-materialize-strategy.adaptive.threshold.high")
                            .longType()
                            .noDefaultValue()
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "When using strategy="
                                                            + SinkUpsertMaterializeStrategy.ADAPTIVE
                                                            + ", defines the number of entries per key when the implementation is changed from "
                                                            + SinkUpsertMaterializeStrategy.VALUE
                                                            + " to "
                                                            + SinkUpsertMaterializeStrategy.MAP
                                                            + ". "
                                                            + "If not specified, Flink uses state-backend specific defaults (400 for hashmap state backend and 50 for RocksDB and the rest).")
                                            .linebreak()
                                            .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<SinkUpsertMaterializeStrategy>
            TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY =
                    key("table.exec.sink.upsert-materialize-strategy.type")
                            .enumType(SinkUpsertMaterializeStrategy.class)
                            .defaultValue(SinkUpsertMaterializeStrategy.LEGACY)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Which strategy of SinkUpsertMaterializer to use. Supported strategies:")
                                            .linebreak()
                                            .text(
                                                    SinkUpsertMaterializeStrategy.LEGACY
                                                            + ": Simple implementation based on ValueState<List> (the original implementation).")
                                            .linebreak()
                                            .text(
                                                    SinkUpsertMaterializeStrategy.MAP
                                                            + ": SequencedMultiSetState implementation based on a combination of several MapState maintaining ordering and fast lookup properties.")
                                            .linebreak()
                                            .text(
                                                    SinkUpsertMaterializeStrategy.VALUE
                                                            + ": Similar to LEGACY, but compatible with MAP and therefore allows to switch to ADAPTIVE.")
                                            .linebreak()
                                            .text(
                                                    SinkUpsertMaterializeStrategy.ADAPTIVE
                                                            + ": Alternate between MAP and VALUE depending on the number of entries for the given key starting with VALUE and switching to MAP upon reaching threshold.high value (and back to VALUE, when reaching low).")
                                            .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<SinkKeyedShuffle> TABLE_EXEC_SINK_KEYED_SHUFFLE =
            key("table.exec.sink.keyed-shuffle")
                    .enumType(SinkKeyedShuffle.class)
                    .defaultValue(SinkKeyedShuffle.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "In order to minimize the distributed disorder problem when writing data into table with primary keys that many users suffers. "
                                                    + "FLINK will auto add a keyed shuffle by default when the sink parallelism differs from upstream operator and sink parallelism is not 1. "
                                                    + "This works only when the upstream ensures the multi-records' order on the primary key, if not, the added shuffle can not solve "
                                                    + "the problem (In this situation, a more proper way is to consider the deduplicate operation for the source firstly or use an "
                                                    + "upsert source with primary key definition which truly reflect the records evolution).")
                                    .linebreak()
                                    .text(
                                            "By default, the keyed shuffle will be added when the sink's parallelism differs from upstream operator. "
                                                    + "You can set to no shuffle(NONE) or force shuffle(FORCE).")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<RowtimeInserter> TABLE_EXEC_SINK_ROWTIME_INSERTER =
            key("table.exec.sink.rowtime-inserter")
                    .enumType(RowtimeInserter.class)
                    .defaultValue(RowtimeInserter.ENABLED)
                    .withDescription(
                            "Some sink implementations require a single rowtime attribute in the input "
                                    + "that can be inserted into the underlying stream record. This option "
                                    + "allows disabling the timestamp insertion and avoids errors around "
                                    + "multiple time attributes being present in the query schema.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT =
            key("table.exec.sink.require-on-conflict")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "When enabled, an error is thrown if the query's upsert key differs "
                                                    + "from the sink table's primary key and no ON CONFLICT clause is specified. "
                                                    + "This can lead to non-deterministic results when multiple records with different "
                                                    + "upsert keys map to the same primary key.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Set this to false to restore the old behavior where no ON CONFLICT clause "
                                                    + "was required. Note that disabling this check may lead to non-deterministic "
                                                    + "results in certain streaming scenarios.")
                                    .build());

    // ------------------------------------------------------------------------
    //  Sort Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_SORT_DEFAULT_LIMIT =
            key("table.exec.sort.default-limit")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Default limit when user don't set a limit after order by. -1 indicates that this configuration is ignored.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES =
            key("table.exec.sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge sort. It limits the number of file handles per operator. "
                                    + "If it is too small, may cause intermediate merging. But if it is too large, "
                                    + "it will cause too many files opened at the same time, consume memory and lead to random reading.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED =
            key("table.exec.sort.async-merge-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to asynchronously merge sorted spill files.");

    // ------------------------------------------------------------------------
    //  Spill Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_SPILL_COMPRESSION_ENABLED =
            key("table.exec.spill-compression.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to compress spilled data. "
                                    + "Currently we only support compress spilled data for sort and hash-agg and hash-join operators.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize> TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
            key("table.exec.spill-compression.block-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription(
                            "The memory size used to do compress when spilling data. "
                                    + "The larger the memory, the higher the compression ratio, "
                                    + "but more memory resource will be consumed by the job.");

    // ------------------------------------------------------------------------
    //  Resource Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM =
            key("table.exec.resource.default-parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Sets default parallelism for all operators "
                                    + "(such as aggregate, join, filter) to run with parallel instances. "
                                    + "This config has a higher priority than parallelism of "
                                    + "StreamExecutionEnvironment (actually, this config overrides the parallelism "
                                    + "of StreamExecutionEnvironment). A value of -1 indicates that no "
                                    + "default parallelism is set, then it will fallback to use the parallelism "
                                    + "of StreamExecutionEnvironment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY =
            key("table.exec.resource.external-buffer-memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10 mb"))
                    .withDescription(
                            "Sets the external buffer memory size that is used in sort merge join"
                                    + " and nested join and over window. Note: memory size is only a weight hint,"
                                    + " it will affect the weight of memory that can be applied by a single operator"
                                    + " in the task, the actual memory used depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY =
            key("table.exec.resource.hash-agg.memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128 mb"))
                    .withDescription(
                            "Sets the managed memory size of hash aggregate operator."
                                    + " Note: memory size is only a weight hint, it will affect the weight of memory"
                                    + " that can be applied by a single operator in the task, the actual memory used"
                                    + " depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY =
            key("table.exec.resource.hash-join.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory for hash join operator. It defines the lower"
                                    + " limit. Note: memory size is only a weight hint, it will affect the weight of"
                                    + " memory that can be applied by a single operator in the task, the actual"
                                    + " memory used depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_SORT_MEMORY =
            key("table.exec.resource.sort.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed buffer memory size for sort operator. Note: memory"
                                    + " size is only a weight hint, it will affect the weight of memory that can be"
                                    + " applied by a single operator in the task, the actual memory used depends on"
                                    + " the running environment.");

    // ------------------------------------------------------------------------
    //  Agg Options
    // ------------------------------------------------------------------------

    /** See {@code org.apache.flink.table.runtime.operators.window.grouping.HeapWindowsGrouping}. */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
            key("table.exec.window-agg.buffer-size-limit")
                    .intType()
                    .defaultValue(100 * 1000)
                    .withDescription(
                            "Sets the window elements buffer size limit used in group window agg operator.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED =
            ConfigOptions.key("table.exec.local-hash-agg.adaptive.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable adaptive local hash aggregation. Adaptive local hash "
                                    + "aggregation is an optimization of local hash aggregation, which can adaptively "
                                    + "determine whether to continue to do local hash aggregation according to the distinct "
                                    + "value rate of sampling data. If distinct value rate bigger than defined threshold "
                                    + "(see parameter: table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold), "
                                    + "we will stop aggregating and just send the input data to the downstream after a simple "
                                    + "projection. Otherwise, we will continue to do aggregation. Adaptive local hash aggregation "
                                    + "only works in batch mode. Default value of this parameter is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD =
            ConfigOptions.key("table.exec.local-hash-agg.adaptive.sampling-threshold")
                    .longType()
                    .defaultValue(500000L)
                    .withDescription(
                            "If adaptive local hash aggregation is enabled, this value defines how "
                                    + "many records will be used as sampled data to calculate distinct value rate "
                                    + "(see parameter: table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold) "
                                    + "for the local aggregate. The higher the sampling threshold, the more accurate "
                                    + "the distinct value rate is. But as the sampling threshold increases, local "
                                    + "aggregation is meaningless when the distinct values rate is low. "
                                    + "The default value is 500000.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Double>
            TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD =
                    ConfigOptions.key(
                                    "table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold")
                            .doubleType()
                            .defaultValue(0.5d)
                            .withDescription(
                                    "The distinct value rate can be defined as the number of local "
                                            + "aggregation results for the sampled data divided by the sampling "
                                            + "threshold (see "
                                            + TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD
                                                    .key()
                                            + "). "
                                            + "If the computed result is lower than the given configuration value, "
                                            + "the remaining input records proceed to do local aggregation, otherwise "
                                            + "the remaining input records are subjected to simple projection which "
                                            + "calculation cost is less than local aggregation. The default value is 0.5.");

    // ------------------------------------------------------------------------
    //  Async Lookup Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_ASYNC_LOOKUP_KEY_ORDERED =
            key("table.exec.async-lookup.key-ordered-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, async lookup joins would follow the upsert key order in cdc streams. "
                                    + "If there is no defined upsert key, then the total record is considered as the upsert key. "
                                    + "Setting this for insert-only streams has no effect because record in insert-only streams is "
                                    + "independent and does not affect the state of previous records. "
                                    + "Besides, since records in insert-only streams typically do not involve a primary key then "
                                    + "no upsertKey can be derived. This makes them be unordered processed even if key ordered enabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY =
            key("table.exec.async-lookup.buffer-capacity")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The max number of async i/o operation that the async lookup join can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT =
            key("table.exec.async-lookup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The async timeout for the asynchronous operation to complete.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<AsyncOutputMode> TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE =
            key("table.exec.async-lookup.output-mode")
                    .enumType(AsyncOutputMode.class)
                    .defaultValue(AsyncOutputMode.ORDERED)
                    .withDescription(
                            "Output mode for asynchronous operations which will convert to {@see AsyncDataStream.OutputMode}, ORDERED by default. "
                                    + "If set to ALLOW_UNORDERED, will attempt to use {@see AsyncDataStream.OutputMode.UNORDERED} when it does not "
                                    + "affect the correctness of the result, otherwise ORDERED will be still used.");

    // ------------------------------------------------------------------------
    //  Async Scalar Function
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_SCALAR_MAX_CONCURRENT_OPERATIONS =
            key("table.exec.async-scalar.max-concurrent-operations")
                    .intType()
                    .defaultValue(10)
                    .withDeprecatedKeys("table.exec.async-scalar.buffer-capacity")
                    .withDescription(
                            "The max number of async i/o operation that the async scalar function can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_SCALAR_TIMEOUT =
            key("table.exec.async-scalar.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The async timeout for the asynchronous operation to complete.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<RetryStrategy> TABLE_EXEC_ASYNC_SCALAR_RETRY_STRATEGY =
            key("table.exec.async-scalar.retry-strategy")
                    .enumType(RetryStrategy.class)
                    .defaultValue(RetryStrategy.FIXED_DELAY)
                    .withDescription(
                            "Restart strategy which will be used, FIXED_DELAY by default.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY =
            key("table.exec.async-scalar.retry-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription("The delay to wait before trying again.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS =
            key("table.exec.async-scalar.max-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max number of async retry attempts to make before task "
                                    + "execution is failed.");

    // ------------------------------------------------------------------------
    //  Async Table Function
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_TABLE_MAX_CONCURRENT_OPERATIONS =
            key("table.exec.async-table.max-concurrent-operations")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The max number of concurrent async i/o operations that the async table function can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_TABLE_TIMEOUT =
            key("table.exec.async-table.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The async timeout for the asynchronous operation to complete, including any retries which may occur.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<RetryStrategy> TABLE_EXEC_ASYNC_TABLE_RETRY_STRATEGY =
            key("table.exec.async-table.retry-strategy")
                    .enumType(RetryStrategy.class)
                    .defaultValue(RetryStrategy.FIXED_DELAY)
                    .withDescription(
                            "Restart strategy which will be used, FIXED_DELAY by default.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_TABLE_RETRY_DELAY =
            key("table.exec.async-table.retry-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription("The delay to wait before trying again.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_TABLE_MAX_RETRIES =
            key("table.exec.async-table.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max number of async retry attempts to make before task "
                                    + "execution is failed.");

    // ------------------------------------------------------------------------
    //  Async ML_PREDICT Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer>
            TABLE_EXEC_ASYNC_ML_PREDICT_MAX_CONCURRENT_OPERATIONS =
                    key("table.exec.async-ml-predict.max-concurrent-operations")
                            .intType()
                            .defaultValue(10)
                            .withDescription(
                                    "The max number of async i/o operation that the async ml predict can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_ML_PREDICT_TIMEOUT =
            key("table.exec.async-ml-predict.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The async timeout for the asynchronous operation to complete. If the deadline fails, a timeout exception will be thrown to indicate the error.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<AsyncOutputMode> TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE =
            key("table.exec.async-ml-predict.output-mode")
                    .enumType(AsyncOutputMode.class)
                    .defaultValue(AsyncOutputMode.ORDERED)
                    .withDescription(
                            "Output mode for async ML predict, which describes whether or not the the output should attempt to be ordered or not. The supported options are: "
                                    + "ALLOW_UNORDERED means the operator emit the result when execution finishes. The planner will attempt use ALLOW_UNORDERED whn it doesn't affect "
                                    + "the correctness of the results.\n"
                                    + "ORDERED ensures that the operator emits the result in the same order as the data enters it. This is the default.");

    // ------------------------------------------------------------------------
    //  Async VECTOR_SEARCH Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer>
            TABLE_EXEC_ASYNC_VECTOR_SEARCH_MAX_CONCURRENT_OPERATIONS =
                    key("table.exec.async-vector-search.max-concurrent-operations")
                            .intType()
                            .defaultValue(10)
                            .withDescription(
                                    "The max number of async i/o operation that the async vector search can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_VECTOR_SEARCH_TIMEOUT =
            key("table.exec.async-vector-search.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The total time which can pass before the invocation (including "
                                    + "retries) is considered timed out and task execution is failed.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<AsyncOutputMode> TABLE_EXEC_ASYNC_VECTOR_SEARCH_OUTPUT_MODE =
            key("table.exec.async-vector-search.output-mode")
                    .enumType(AsyncOutputMode.class)
                    .defaultValue(AsyncOutputMode.ORDERED)
                    .withDescription(
                            "Output mode for async vector search, which describes whether or not "
                                    + "the output should attempt to be ordered or not.\n"
                                    + "The supported options are:\n"
                                    + "ALLOW_UNORDERED means the operator emits the result when "
                                    + "execution finishes. The planner will attempt to use "
                                    + "ALLOW_UNORDERED when it doesn't affect the correctness of "
                                    + "the results.\n"
                                    + "ORDERED means that the operator emits the result in the "
                                    + "same order as the data enters it. This is the default.");

    // ------------------------------------------------------------------------
    //  MiniBatch Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_MINIBATCH_ENABLED =
            key("table.exec.mini-batch.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specifies whether to enable MiniBatch optimization. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "This is disabled by default. To enable this, users should set this config to true. "
                                    + "NOTE: If mini-batch is enabled, 'table.exec.mini-batch.allow-latency' and "
                                    + "'table.exec.mini-batch.size' must be set.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_MINIBATCH_ALLOW_LATENCY =
            key("table.exec.mini-batch.allow-latency")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "The maximum latency can be used for MiniBatch to buffer input records. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. "
                                    + "NOTE: If "
                                    + TABLE_EXEC_MINIBATCH_ENABLED.key()
                                    + " is set true, its value must be greater than zero.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_MINIBATCH_SIZE =
            key("table.exec.mini-batch.size")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The maximum number of input records can be buffered for MiniBatch. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. "
                                    + "NOTE: MiniBatch only works for non-windowed aggregations currently. If "
                                    + TABLE_EXEC_MINIBATCH_ENABLED.key()
                                    + " is set true, its value must be positive.");

    // ------------------------------------------------------------------------
    //  Other Exec Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_EXEC_DISABLED_OPERATORS =
            key("table.exec.disabled-operators")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mainly for testing. A comma-separated list of operator names, each name "
                                    + "represents a kind of disabled operator.\n"
                                    + "Operators that can be disabled include \"NestedLoopJoin\", \"ShuffleHashJoin\", \"BroadcastHashJoin\", "
                                    + "\"SortMergeJoin\", \"HashAgg\", \"SortAgg\".\n"
                                    + "By default no operator is disabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED =
            key("table.exec.operator-fusion-codegen.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, multiple physical operators will be compiled into a single operator by planner which can improve the performance.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<LegacyCastBehaviour> TABLE_EXEC_LEGACY_CAST_BEHAVIOUR =
            key("table.exec.legacy-cast-behaviour")
                    .enumType(LegacyCastBehaviour.class)
                    .defaultValue(LegacyCastBehaviour.DISABLED)
                    .withDescription(
                            "Determines whether CAST will operate following the legacy behaviour "
                                    + "or the new one that introduces various fixes and improvements.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_RANK_TOPN_CACHE_SIZE =
            ConfigOptions.key("table.exec.rank.topn-cache-size")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Rank operators have a cache which caches partial state contents "
                                    + "to reduce state access. Cache size is the number of records "
                                    + "in each ranking task.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED =
            key("table.exec.simplify-operator-name-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will simplify the operator name with id and type of ExecNode and keep detail in description. Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean>
            TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED =
                    key("table.exec.deduplicate.insert-update-after-sensitive-enabled")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "Set whether the job (especially the sinks) is sensitive to "
                                            + "INSERT messages and UPDATE_AFTER messages. "
                                            + "If false, Flink may, sometimes (e.g. deduplication "
                                            + "for last row), send UPDATE_AFTER instead of INSERT "
                                            + "for the first row. If true, Flink will guarantee to "
                                            + "send INSERT for the first row, in that case there "
                                            + "will be additional overhead. Default is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean>
            TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED =
                    ConfigOptions.key("table.exec.deduplicate.mini-batch.compact-changes-enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Set whether to compact the changes sent downstream in row-time "
                                            + "mini-batch. If true, Flink will compact changes and send "
                                            + "only the latest change downstream. Note that if the "
                                            + "downstream needs the details of versioned data, this "
                                            + "optimization cannot be applied. If false, Flink will send "
                                            + "all changes to downstream just like when the mini-batch is "
                                            + "not enabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> UNBOUNDED_OVER_VERSION =
            ConfigOptions.key("table.exec.unbounded-over.version")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Which version of the unbounded over aggregation to use: "
                                    + " 1 - legacy version"
                                    + " 2 - version with improved performance");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<UidGeneration> TABLE_EXEC_UID_GENERATION =
            key("table.exec.uid.generation")
                    .enumType(UidGeneration.class)
                    .defaultValue(UidGeneration.PLAN_ONLY)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "In order to remap state to operators during a restore, "
                                                    + "it is required that the pipeline's streaming "
                                                    + "transformations get a UID assigned.")
                                    .linebreak()
                                    .text(
                                            "The planner can generate and assign explicit UIDs. If no "
                                                    + "UIDs have been set by the planner, the UIDs will "
                                                    + "be auto-generated by lower layers that can take "
                                                    + "the complete topology into account for uniqueness "
                                                    + "of the IDs. See the DataStream API for more information.")
                                    .linebreak()
                                    .text(
                                            "This configuration option is for experts only and the default "
                                                    + "should be sufficient for most use cases. By default, "
                                                    + "only pipelines created from a persisted compiled plan will "
                                                    + "get UIDs assigned explicitly. Thus, these pipelines can "
                                                    + "be arbitrarily moved around within the same topology without "
                                                    + "affecting the stable UIDs.")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<String> TABLE_EXEC_UID_FORMAT =
            key("table.exec.uid.format")
                    .stringType()
                    .defaultValue("<id>_<transformation>")
                    .withDescription(
                            "Defines the format pattern for generating the UID of an ExecNode streaming transformation. "
                                    + "The pattern can be defined globally or per-ExecNode in the compiled plan. "
                                    + "Supported arguments are: <id> (from static counter), <type> (e.g. 'stream-exec-sink'), "
                                    + "<version>, and <transformation> (e.g. 'constraint-validator' for a sink). "
                                    + "In Flink 1.15.x the pattern was wrongly defined as '<id>_<type>_<version>_<transformation>' "
                                    + "which would prevent migrations in the future.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL =
            key("table.exec.interval-join.min-cleanup-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Specifies a minimum time interval for how long cleanup unmatched records in the interval join operator. "
                                    + "Before Flink 1.18, the default value of this param was the half of interval duration. "
                                    + "Note: Set this option greater than 0 will cause unmatched records in outer joins to be output later than watermark, "
                                    + "leading to possible discarding of these records by downstream watermark-dependent operators, such as window operators. "
                                    + "The default value is 0, which means it will clean up unmatched records immediately.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_ASYNC_STATE_ENABLED =
            key("table.exec.async-state.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Set whether to use the SQL/Table operators based on the asynchronous state api. "
                                    + "Default value is false.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED =
            key("table.exec.delta-join.cache-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable the cache of delta join. If enabled, the delta join caches the "
                                    + "records from remote dim table. Default is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_DELTA_JOIN_LEFT_CACHE_SIZE =
            key("table.exec.delta-join.left.cache-size")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "The cache size used to cache the lookup results of the left table in delta join. "
                                    + "This value must be positive when enabling cache. Default is 10000.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_DELTA_JOIN_RIGHT_CACHE_SIZE =
            key("table.exec.delta-join.right.cache-size")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "The cache size used to cache the lookup results of the right table in delta join. "
                                    + "This value must be positive when enabling cache. Default is 10000.");

    // ------------------------------------------------------------------------------------------
    // Enum option types
    // ------------------------------------------------------------------------------------------

    /** The enforcer to guarantee NOT NULL column constraint when writing data into sink. */
    @PublicEvolving
    public enum NotNullEnforcer implements DescribedEnum {
        ERROR(text("Throw a runtime exception when writing null values into NOT NULL column.")),
        DROP(
                text(
                        "Drop records silently if a null value would have to be inserted "
                                + "into a NOT NULL column."));

        private final InlineElement description;

        NotNullEnforcer(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /**
     * The enforcer to guarantee that length of CHAR/VARCHAR/BINARY/VARBINARY columns is respected
     * when writing data into a sink.
     */
    @PublicEvolving
    public enum TypeLengthEnforcer implements DescribedEnum {
        IGNORE(
                text(
                        "Don't apply any trimming and padding, and instead "
                                + "ignore the CHAR/VARCHAR/BINARY/VARBINARY length directive.")),
        TRIM_PAD(
                text(
                        "Trim and pad string and binary values to match the length "
                                + "defined by the CHAR/VARCHAR/BINARY/VARBINARY length.")),
        ERROR(
                text(
                        "Throw a runtime exception when writing data into a "
                                + "CHAR/VARCHAR/BINARY/VARBINARY column which does not match the length"
                                + " constraint"));

        private final InlineElement description;

        TypeLengthEnforcer(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** The enforcer to check the constraints on nested types. */
    @PublicEvolving
    public enum NestedEnforcer implements DescribedEnum {
        IGNORE(text("Don't perform check on nested types in ROWS/ARRAYS/MAPS")),
        ROWS(text("Perform checks on nested types in ROWS.")),
        ROWS_AND_COLLECTIONS(
                text(
                        "Perform checks on nested types in ROWS/ARRAYS/MAPS. Be aware that the"
                                + " more checks the more performance impact. Especially checking"
                                + " types in ARRAYS/MAPS can be expensive."));

        private final InlineElement description;

        NestedEnforcer(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Upsert materialize strategy before sink. */
    @PublicEvolving
    public enum UpsertMaterialize {

        /** In no case will materialize operator be added. */
        NONE,

        /** Add materialize operator when a distributed disorder occurs on unique keys. */
        AUTO,

        /** Add materialize operator in any case. */
        FORCE
    }

    /** Shuffle by primary key before sink. */
    @PublicEvolving
    public enum SinkKeyedShuffle {

        /** No keyed shuffle will be added for sink. */
        NONE,

        /** Auto add keyed shuffle when the sink's parallelism differs from upstream operator. */
        AUTO,

        /** Add keyed shuffle in any case except single parallelism. */
        FORCE
    }

    /** Rowtime attribute insertion strategy for the sink. */
    @PublicEvolving
    public enum RowtimeInserter implements DescribedEnum {
        ENABLED(
                text(
                        "Insert a rowtime attribute (if available) into the underlying stream record. "
                                + "This requires at most one time attribute in the input for the sink.")),
        DISABLED(text("Do not insert the rowtime attribute into the underlying stream record."));

        private final InlineElement description;

        RowtimeInserter(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Output mode for asynchronous operations, equivalent to {@see AsyncDataStream.OutputMode}. */
    @PublicEvolving
    public enum AsyncOutputMode {

        /** Ordered output mode, equivalent to {@see AsyncDataStream.OutputMode.ORDERED}. */
        ORDERED,

        /**
         * Allow unordered output mode, will attempt to use {@see
         * AsyncDataStream.OutputMode.UNORDERED} when it does not affect the correctness of the
         * result, otherwise ORDERED will be still used.
         */
        ALLOW_UNORDERED
    }

    /** Retry strategy in the case of failure. */
    @PublicEvolving
    public enum RetryStrategy {
        /** When a failure occurs, don't retry. */
        NO_RETRY,

        /** A fixed delay before retrying again. */
        FIXED_DELAY
    }

    /** SinkUpsertMaterializer strategy. */
    @PublicEvolving
    public enum SinkUpsertMaterializeStrategy {
        /**
         * Simple implementation based on {@code ValueState<List>} (the original implementation).
         *
         * <ul>
         *   <li>optimal for cases with history under approx. 100 elements
         *   <li>limited TTL support (per key granularity, i.e. no expiration for old history
         *       elements)
         * </ul>
         */
        LEGACY,
        /**
         * OrderedMultiSetState-based implementation based on a combination of several MapState
         * maintaining ordering and fast lookup properties.
         *
         * <ul>
         *   <li>faster and more memory-efficient on long histories
         *   <li>slower on short histories
         *   <li>currently, no TTL support (to be added in the future)
         *   <li>requires more space
         * </ul>
         */
        MAP,
        /**
         * Similar to LEGACY, but compatible with MAP and therefore allows to switch to ADAPTIVE.
         */
        VALUE,
        /**
         * Alternate between MAP and VALUE depending on the number of entries for the given key
         * starting with VALUE and switching to MAP upon reaching threshold.high value (and back to
         * VALUE, when reaching low).
         */
        ADAPTIVE
    }

    /** Determine if CAST operates using the legacy behaviour or the new one. */
    @Deprecated
    public enum LegacyCastBehaviour implements DescribedEnum {
        ENABLED(true, text("CAST will operate following the legacy behaviour.")),
        DISABLED(false, text("CAST will operate following the new correct behaviour."));

        private final boolean enabled;
        private final InlineElement description;

        LegacyCastBehaviour(boolean enabled, InlineElement description) {
            this.enabled = enabled;
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }

        public boolean isEnabled() {
            return enabled;
        }
    }

    /**
     * Strategy for generating transformation UIDs for remapping state to operators during restore.
     */
    @PublicEvolving
    public enum UidGeneration implements DescribedEnum {
        PLAN_ONLY(
                text(
                        "Sets UIDs on streaming transformations if and only if the pipeline definition "
                                + "comes from a compiled plan. Pipelines that have been constructed in "
                                + "the API without a compilation step will not set an explicit UID as "
                                + "it might not be stable across multiple translations.")),
        ALWAYS(
                text(
                        "Always sets UIDs on streaming transformations. This strategy is for experts only! "
                                + "Pipelines that have been constructed in the API without a compilation "
                                + "step might not be able to be restored properly. The UID generation "
                                + "depends on previously declared pipelines (potentially across jobs "
                                + "if the same JVM is used). Thus, a stable environment must be ensured. "
                                + "Pipeline definitions that come from a compiled plan are safe to use.")),

        DISABLED(text("No explicit UIDs will be set."));

        private final InlineElement description;

        UidGeneration(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Accelerator offload
    //
    //  Named `accelerator` rather than `gpu`: what these configure is an extension point, and the
    //  IR, the SPI and the provider interface are all device-neutral. A key saying `gpu` would be
    //  the one place the contract named a kind of hardware, and config keys are the hardest thing
    //  to rename once published.
    //
    //  Every one of these is an environmental parameter, set by whoever runs the cluster. None is
    //  a query-author knob: a correct query must run correctly, and be eligible for offload,
    //  without its author writing anything device-specific.
    // ------------------------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_ACCELERATOR_ENABLED =
            ConfigOptions.key("table.exec.accelerator.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, subtrees of the plan whose per-row work can be represented "
                                    + "for an accelerator are offered to a provider on the "
                                    + "TaskManager, which decides against its own hardware "
                                    + "whether to run them. Off by default.");

    /**
     * Whether an offloaded task should <em>ask</em> to be placed where a device is.
     *
     * <p>Off by default, and the default is not timidity. Flink has no soft resource constraint: a
     * slot request either carries the requirement or it does not. Turn this on against a cluster
     * where no TaskManager declares {@code external-resource.gpu} and the request can never be
     * satisfied, so the job waits for a slot indefinitely -- strictly worse than today's outcome,
     * which is that it runs on the CPU and says so in the TaskManager log.
     *
     * <p>So this is opt-in for the operator, and the operator has to have configured the other
     * side: {@code external-resource.list: gpu} and {@code external-resource.gpu.amount} on the
     * TaskManagers that have one.
     *
     * <p><b>And {@code jobmanager.scheduler: Default}.</b> The requirement reaches the JobGraph
     * intact under either scheduler -- the offloaded vertex's slot sharing group carries a {@code
     * ResourceProfile} naming the resource -- but only the default scheduler declares it.
     * AdaptiveBatch, which is what a batch job gets unless told otherwise, declares {@code
     * ResourceProfile{UNKNOWN}} instead, and a TaskManager with no device serves the job happily.
     * {@code GpuPlacementSchedulerIT} pins both halves of that, so the day it changes is a test
     * failure rather than a surprise.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE =
            ConfigOptions.key("table.exec.accelerator.require-device")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, a job containing an offloaded node requests a slot that "
                                    + "declares the external resource named by "
                                    + "table.exec.accelerator.device.resource-name. Requires "
                                    + "TaskManagers to declare that resource; if none does, the "
                                    + "job will wait for a slot rather than fall back to the CPU. "
                                    + "Needs jobmanager.scheduler: Default -- the AdaptiveBatch "
                                    + "scheduler flattens the requirement to UNKNOWN before the "
                                    + "ResourceManager sees it, so the job runs anywhere.");

    /**
     * The slot sharing group the requirement is attached to, which cannot be the default one.
     *
     * <p>It would be better if it could. {@code StreamingJobGraphGenerator#isChainable} refuses to
     * chain two operators in different slot sharing groups, so a group of its own severs the
     * offloaded Calc from its source, and every workload measured here is bound by reading that
     * source. Attaching the requirement to the region everything already shares would cost nothing.
     *
     * <p>Flink does not allow it. In {@code setSlotSharingAndCoLocation}, a vertex whose group is
     * the default name takes the <em>region</em> slot sharing group and the resource registered
     * under that name is never consulted; only the branch for a named group calls {@code
     * streamGraph.getSlotSharingGroupResource(k)}. Registering resources for {@code default} is
     * therefore not a weaker requirement, it is silently no requirement at all -- so this rejects
     * that name rather than accepting a setting that does nothing.
     */
    public static final String DEFAULT_SLOT_SHARING_GROUP_NAME = "default";

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_EXEC_ACCELERATOR_DEVICE_SLOT_SHARING_GROUP =
            ConfigOptions.key("table.exec.accelerator.device.slot-sharing-group")
                    .stringType()
                    .defaultValue("accelerator")
                    .withDescription(
                            "Slot sharing group carrying the device requirement. Cannot be "
                                    + "\"default\": Flink ignores resources registered for that "
                                    + "name. Because the group must therefore be a distinct one, "
                                    + "the offloaded node is no longer chained to its input.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_NAME =
            ConfigOptions.key("table.exec.accelerator.device.resource-name")
                    .stringType()
                    .defaultValue("gpu")
                    .withDescription(
                            "External resource name to request, matching the TaskManager's "
                                    + "external-resource.<name>.amount.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Double> TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_AMOUNT =
            ConfigOptions.key("table.exec.accelerator.device.resource-amount")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "How much of that resource one slot needs. One device per slot bounds "
                                    + "concurrent subtasks per device to the number of devices, "
                                    + "which is worth having on its own: throughput falls past "
                                    + "roughly eight subtasks sharing one card.");

    /**
     * CPU and heap for that slot, which are not optional.
     *
     * <p>{@code SlotSharingGroup.build()} rejects a group that specifies any resource without also
     * specifying cpu cores and task heap memory, so requesting a device means specifying the whole
     * slot. The planner has no basis for inventing those two numbers -- they depend on the cluster,
     * not the query -- so they are configuration, and requesting a device without setting them
     * deliberately fails fast rather than guessing.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Double> TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES =
            ConfigOptions.key("table.exec.accelerator.device.cpu-cores")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "CPU cores for a slot that carries the device requirement. Required "
                                    + "when require-device is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize> TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP =
            ConfigOptions.key("table.exec.accelerator.device.task-heap")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Task heap for a slot that carries the device requirement. Required "
                                    + "when require-device is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize> TABLE_EXEC_ACCELERATOR_DEVICE_TASK_OFF_HEAP =
            ConfigOptions.key("table.exec.accelerator.device.task-off-heap")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4 gb"))
                    .withDescription(
                            "Off-heap for that slot. Staging buffers are off-heap, so this is not "
                                    + "the place to leave the default of zero.");

    /**
     * Whether an ungrouped aggregate is fused into the {@code Calc} beneath it.
     *
     * <p>On by default, because fusing is the point: unfused, the {@code Calc} writes every
     * projected row to the host for a separate aggregate to read back, and the intermediate is the
     * thing a device pass exists to avoid materialising.
     *
     * <p>It is switchable because the two paths are otherwise impossible to compare. Both arms have
     * to run in one session -- this project's own numbers moved 6% between sittings -- and without
     * a switch the unfused path is unreachable once the planner has learned to fuse.
     *
     * <p><b>Default false since M4.5.</b> M4.0 measured both halves of the question on an RTX 5070
     * Ti. Fusing the two <em>CPU</em> operators is worth nothing -- 0.6%, with overlapping ranges
     * -- because Flink's own operator chaining already does it. Fusing them on a <em>device</em> is
     * worth 1.06x to 4.53x, because the intermediate stops crossing the bus. M1.1 removed the
     * device route and nothing has restored it, so today the only thing fusion does is replace the
     * projection's {@code GpuOrCpuCalcOperatorFactory} with a chained CPU pair -- which is to say
     * it stops {@code SELECT SUM(heavy) FROM t} from offloading at all. The default follows the
     * measurement; flip it back when a fused pair can run on a device.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_ACCELERATOR_FUSE_AGGREGATE =
            ConfigOptions.key("table.exec.accelerator.fuse-aggregate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, an ungrouped SUM over an offloaded Calc is fused with it "
                                    + "into one operator. Off by default: the fused pair has no "
                                    + "accelerator implementation yet, so fusing it takes away the "
                                    + "projection's own accelerator path and gives back only what "
                                    + "operator chaining already did. Turn on to measure fusion, "
                                    + "or once a fused pair can run on a device.");

    /**
     * How much faster than the CPU a provider must claim to be before Flink prefers it.
     *
     * <p>A <em>policy</em> constant, and the distinction matters: it says how much margin Flink
     * wants before taking an unfamiliar execution path, and it means the same thing on a GPU, an
     * FPGA and whatever comes next. The floors it replaced -- 96 weighted operations per row, ten
     * million in total -- were measurements of one RTX 4070 and meant nothing on any other device.
     *
     * <p>1.3 rather than 1.0 because the paths are not otherwise equal: the accelerated one defers
     * output by a batch, allocates staging buffers, and is the less familiar of the two. A claimed
     * 5% win is not worth those.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    /**
     * The margin a provider must claim before its plan is taken.
     *
     * <p>Lenient on purpose, and provisionally. Choosing well between a CPU operator and a device
     * one is a research question this project has not answered: the estimate a provider makes is
     * its own, for its own hardware, about a query it has not run, and no measurement here
     * establishes what margin is the right price for being wrong. Until one does, the default
     * prefers the device wherever a provider expects it to be no slower, so the cost of a bad
     * guess is a query that could have been faster rather than an accelerator that never runs.
     *
     * <p>The risk it accepts is bounded and the risk it avoids is not. Offloading something the
     * CPU would have done marginally quicker costs that margin. Declining everything makes the
     * whole feature invisible, which is what a 1.3 default did to queries the device would have
     * drawn or won.
     *
     * <p>This governs <em>cost</em> only. Nothing here can make a subtree eligible that the
     * provenance analysis refused, or that the provider declined for a type it cannot hold: those
     * refusals are about the answer, not about the price, and no configuration relaxes them.
     */
    public static final ConfigOption<Double> TABLE_EXEC_ACCELERATOR_MIN_SPEEDUP =
            ConfigOptions.key("table.exec.accelerator.min-speedup")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "How much faster than the CPU a provider must claim to be before its "
                                    + "plan is preferred. Compared against the provider's own "
                                    + "estimate for its own device, made on the TaskManager. Not "
                                    + "a hardware constant: it means the same on any accelerator. "
                                    + "The default of 1.0 offloads whenever a provider expects to "
                                    + "be no slower than the CPU; it was 1.3, which declined work "
                                    + "the device would have won or drawn.");

    /**
     * The largest subtask parallelism at which a node may still be offloaded.
     *
     * <p>A containment, not a tuning knob, and the distinction matters. Repeated jobs at
     * parallelism above 1 sharing one device have been observed to fail intermittently with {@code
     * CUDA_ERROR_LAUNCH_FAILED}, and the first failing operation has not been identified. Device
     * memory was ruled out by measurement -- the card peaked at 2.4 GiB of 8 GiB during a failing
     * run -- so the cause is open.
     *
     * <p>Until it is closed, a subtask whose parallelism exceeds this runs the generated CPU
     * operator and says so, which is the contract's rule for a configuration that cannot be
     * admitted safely: decline predictably beforehand rather than fail unpredictably during
     * execution. The default of 1 is the largest value tested clean, not a statement that 2 is
     * unsafe; raise it to measure, and expect intermittent failures above 1 until the cause is
     * found.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_ACCELERATOR_MAX_PARALLELISM =
            ConfigOptions.key("table.exec.accelerator.max-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The largest subtask parallelism at which a node may be offloaded. "
                                    + "Above it the generated CPU operator runs instead and the "
                                    + "decision metric reports PARALLELISM_ABOVE_SUPPORTED. The "
                                    + "default of 1 contains an unexplained intermittent CUDA "
                                    + "launch failure seen at higher parallelism on a single "
                                    + "device; it is not a performance setting.");

    /**
     * Whether an accelerated subtree may compute values a device rounds differently from Flink.
     *
     * <p>{@code EXP}, {@code LN}, {@code SIN} and the rest come from the device's math library,
     * which specifies its own accuracy and is not required to agree with {@code java.lang.Math}. A
     * query that returns different numbers depending on where it ran is not the same query, so this
     * is off by default and such a subtree simply runs on the CPU.
     *
     * <p>Turning it on gives up the <em>output-value</em> guarantee and nothing else. The
     * <em>membership</em> guarantee is still enforced: an offload whose drifting value could reach
     * a filter, join, grouping, sort or limit is refused either way, so the set of rows a query
     * returns does not depend on this setting. It is an explicitly approximate mode and is not
     * transparent acceleration.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS =
            ConfigOptions.key("table.exec.accelerator.approximate-projections")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether an accelerated subtree may compute values a device rounds"
                                    + " differently from Flink. Off by default; turning it on gives up"
                                    + " the output-value guarantee, and the set of rows returned is"
                                    + " unchanged either way.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_ACCELERATOR_BATCH_SIZE =
            ConfigOptions.key("table.exec.accelerator.batch-size")
                    .intType()
                    .defaultValue(262_144)
                    .withDescription(
                            "Rows staged before a kernel launch. Batching is not optional -- a "
                                    + "device kernel needs a sized contiguous buffer -- so this "
                                    + "trades memory per subtask against dispatch overhead. "
                                    + "Measured from 64K to 4M, device work stays near a tenth of "
                                    + "the total throughout and larger batches mainly reduce "
                                    + "the accelerator runtime dispatch cost.");
}
