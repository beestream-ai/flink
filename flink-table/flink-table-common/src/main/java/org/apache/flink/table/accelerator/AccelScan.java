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

package org.apache.flink.table.accelerator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A leaf that reads rows from storage, rather than receiving them from upstream.
 *
 * <p>{@link AccelInput} says "rows arrive, from somewhere that is Flink's business". That is the
 * right answer for every operator above the source and the wrong one for the source itself, where
 * <em>where the rows come from</em> is the entire question a provider is being asked. A subtree
 * rooted at an {@code AccelInput} can at best start work after Flink has decoded a file on the
 * CPU and handed the rows over one at a time; measured on 8M rows of 31 columns, that decode is
 * ~4.3 s and the handover another ~4.6 s, against ~57 ms of device execution.
 *
 * <p>So this node exists to let a provider answer a different question: not "can I compute this
 * over rows you give me" but "can I read this file myself". What it carries is what that answer
 * needs — a format, the files or splits, the columns actually wanted, and the row they produce.
 *
 * <p>It is a leaf: {@link #inputs()} is empty, exactly as {@code AccelInput}'s is. A provider that
 * does not recognise it declines, and the planner plans the CPU source it would have planned
 * anyway.
 *
 * <p>Nothing here names a storage system or a vendor. {@code format} is the identifier Flink's own
 * factories use — {@code "parquet"}, {@code "orc"}, {@code "csv"} — and a provider that does not
 * know one declines on the string rather than on a type it was made to import.
 */
@PublicEvolving
public final class AccelScan extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final String format;

    private final List<String> paths;

    private final int[] projectedFields;

    private final RowType producedType;

    /**
     * @param format the format identifier, as Flink's factories spell it, lower case.
     * @param paths the files or split locations this subtask is to read. Never empty — a scan of
     *     nothing is not a scan a provider should be asked about.
     * @param projectedFields indices into the table's row, in the order {@code producedType}
     *     declares them, so a provider reads the columns the query wants and no others.
     * @param producedType the row this scan yields, which is the projection applied, not the
     *     table's full row.
     */
    public AccelScan(
            String format, List<String> paths, int[] projectedFields, RowType producedType) {
        this.format = Objects.requireNonNull(format, "format");
        this.paths =
                Collections.unmodifiableList(
                        new java.util.ArrayList<>(Objects.requireNonNull(paths, "paths")));
        this.projectedFields = Objects.requireNonNull(projectedFields, "projectedFields").clone();
        this.producedType = Objects.requireNonNull(producedType, "producedType");
        if (this.paths.isEmpty()) {
            throw new IllegalArgumentException("a scan must read at least one path");
        }
        if (this.projectedFields.length != producedType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "projectedFields has "
                            + this.projectedFields.length
                            + " entries but the produced row has "
                            + producedType.getFieldCount()
                            + " fields");
        }
    }

    /** The format identifier, as Flink's factories spell it. */
    public String format() {
        return format;
    }

    /** The files or splits this subtask reads, in no particular order. */
    public List<String> paths() {
        return paths;
    }

    /** Indices into the table's row, one per field of {@link #outputType()}. */
    public int[] projectedFields() {
        return projectedFields.clone();
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.emptyList();
    }

    @Override
    public RowType outputType() {
        return producedType;
    }

    @Override
    public String toString() {
        return "Scan[" + format + ", " + paths.size() + " paths]" + producedType.getFieldNames();
    }
}
