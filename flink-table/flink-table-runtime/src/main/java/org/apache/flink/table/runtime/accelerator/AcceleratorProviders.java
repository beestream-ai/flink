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

package org.apache.flink.table.runtime.accelerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.accelerator.AccelIrVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

/** Finds the {@link AcceleratorProvider} on a TaskManager's classpath, if there is one. */
@Internal
public final class AcceleratorProviders {

    private static final Logger LOG = LoggerFactory.getLogger(AcceleratorProviders.class);

    private AcceleratorProviders() {}

    /**
     * Loads the single provider visible to {@code classLoader}.
     *
     * <p>Absence is the common case and is not an error: most TaskManagers in most clusters have no
     * accelerator, and a job must run there unchanged.
     *
     * <p>Not cached. This runs once per offloaded subtask as its operator is built, and holding a
     * provider in a static field would outlive the user classloader that supplied it.
     *
     * <p>Every failure mode is swallowed deliberately. A service file naming a class that is not
     * present, a provider whose static initialiser cannot find a native library, a provider built
     * against an IR this Flink no longer emits -- each is a reason to run on the CPU, and none is a
     * reason to fail a job.
     */
    public static Optional<AcceleratorProvider> find(ClassLoader classLoader) {
        try {
            Iterator<AcceleratorProvider> found =
                    ServiceLoader.load(AcceleratorProvider.class, classLoader).iterator();
            if (!found.hasNext()) {
                return Optional.empty();
            }
            AcceleratorProvider provider = found.next();
            if (provider.supportedIrVersion() < AccelIrVersion.CURRENT) {
                LOG.info(
                        "Ignoring accelerator provider {}: it understands IR v{}, this Flink emits v{}",
                        provider.name(),
                        provider.supportedIrVersion(),
                        AccelIrVersion.CURRENT);
                return Optional.empty();
            }
            if (found.hasNext()) {
                LOG.warn(
                        "More than one accelerator provider on the classpath; using {}. "
                                + "Choosing between them is not specified yet.",
                        provider.name());
            }
            return Optional.of(provider);
        } catch (Throwable unusable) {
            // Throwable, not Exception: a missing class or an unsatisfied native link arrives as
            // an Error, and on a machine without the hardware that is the expected outcome.
            LOG.info("No usable accelerator provider on this TaskManager: {}", unusable.toString());
            return Optional.empty();
        }
    }

    /**
     * The first provider that will read this scan itself, or empty.
     *
     * <p>Asked on the JobManager while planning, unlike everything else here, because a source has
     * to be in the job graph before anything runs. A provider that answers is claiming the
     * capability for the TaskManagers too; see {@link AcceleratorProvider#createScanSource}.
     *
     * <p>The loader is the caller's to supply, and it has to be the user-code one. This class ships
     * in {@code flink-table-runtime}, which lives in {@code lib/} and is loaded by the app
     * classloader; that loader cannot see a job's submitted jar. Resolving against it would make a
     * provider packaged with the job invisible to the only plan-time lookup there is, while the
     * identical provider is still found at runtime by {@link #find} -- so the scan substitution
     * would simply never fire, with nothing in the plan to say why.
     */
    public static java.util.Optional<
                    org.apache.flink.api.connector.source.Source<
                            org.apache.flink.table.data.RowData, ?, ?>>
            createScanSource(
                    org.apache.flink.table.accelerator.AccelNode subtree,
                    org.apache.flink.table.types.logical.RowType outputType,
                    ClassLoader loader) {
        final Optional<AcceleratorProvider> found = find(loader);
        if (found.isPresent()) {
            final AcceleratorProvider provider = found.get();
            try {
                final java.util.Optional<
                                org.apache.flink.api.connector.source.Source<
                                        org.apache.flink.table.data.RowData, ?, ?>>
                        source = provider.createScanSource(subtree, outputType);
                if (source.isPresent()) {
                    return source;
                }
            } catch (Throwable declined) {
                // Throwable, not Exception: on a machine without the hardware an unsatisfied
                // native link arrives as an Error, and that is the expected outcome, not a fault.
                // Planning continues on the CPU rather than failing the query, which is what
                // absence has always meant here.
                LOG.info(
                        "Provider {} will not read the source on a device; planning the CPU source"
                                + " instead: {}",
                        provider.name(),
                        declined.toString());
            }
        }
        return java.util.Optional.empty();
    }
}
