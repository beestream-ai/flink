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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String CALC_TRANSFORMATION = "calc";

    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_PROJECTION)
    protected final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    protected final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    protected CommonExecCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                generateCalcOperator(ctx, inputTransform);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(CALC_TRANSFORMATION, config),
                substituteOperator(planner, config, substituteStreamOperator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
    }

    /**
     * The operator this Calc will run, given the one code generation produced.
     *
     * <p>The generated operator, unchanged, unless a subclass says otherwise — and only {@link
     * org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc} does. This is the seam
     * that keeps that substitution out of here: what it substitutes is a batch-only capability, and
     * before M2.6 it lived in this class where {@code StreamExecCalc} inherited it and was kept out
     * only by the fact that nobody registered the graph processor that would have selected it. A
     * registration is a thing somebody can change by accident. A method that does not exist on the
     * streaming node is not.
     */
    protected StreamOperatorFactory<RowData> substituteOperator(
            PlannerBase planner, ExecNodeConfig config, CodeGenOperatorFactory<RowData> generated) {
        return generated;
    }

    /**
     * The code-generated operator for this Calc, over a given input.
     *
     * <p>Separated from {@link #translateToPlanInternal} so an aggregate that has fused this node
     * can generate it without also generating this node's transformation. When the pair runs as one
     * operator there is no Calc transformation to build, but there is still a Calc operator: it is
     * what the TaskManager runs, chained ahead of the aggregate, when no device is available.
     */
    public CodeGenOperatorFactory<RowData> generateCalcOperator(
            CodeGeneratorContext ctx, Transformation<RowData> inputTransform) {
        return CalcCodeGenerator.generateCalcOperator(
                ctx,
                inputTransform,
                (RowType) getOutputType(),
                JavaScalaConversionUtil.toScala(projection),
                JavaScalaConversionUtil.toScala(Optional.ofNullable(this.condition)),
                retainHeader,
                getClass().getSimpleName());
    }
}
