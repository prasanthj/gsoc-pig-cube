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

package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POCube extends PhysicalOperator {

    private boolean isHolistic;
    private String algebraicAttr;
    private List<Tuple> cubeLattice;
    private String holisticMeasure;
    private boolean isPostAggRequired;
    private PhysicalOperator postAggLR;
    private int numDimensions;

    // Holistic measures. These values will be used during post processing stage and
    // also when printing MRPlan
    public static final String HOLISTIC_COUNT_DISTINCT = "count_distinct";
    public static final String HOLISTIC_TOPK = "topk";

    public POCube(OperatorKey k, int rp) {
        this(k, rp, null, false, null);
    }

    public POCube(OperatorKey k, int rp, List<PhysicalOperator> inp, boolean isHolistic, String algebraicAttr) {
        super(k, rp, inp);
        this.isHolistic = isHolistic;
        this.algebraicAttr = algebraicAttr;
        this.cubeLattice = null;
        this.holisticMeasure = null;
        this.isPostAggRequired = false;
        this.postAggLR = null;
        this.numDimensions = 0;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCube(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public String name() {
        String measure;
        if (isHolistic) {
            measure = "holistic[" + this.getHolisticMeasure() + "]";
        } else {
            measure = "algebraic";
        }
        return getAliasString() + "POCube[" + DataType.findTypeName(resultType) + "]" + " Measure - " + measure + " - "
        + mKey.toString();
    }

    public boolean isHolistic() {
        return isHolistic;
    }

    public void setHolistic(boolean isHolistic) {
        this.isHolistic = isHolistic;
    }

    public String getAlgebraicAttr() {
        return algebraicAttr;
    }

    public void setAlgebraicAttr(String algebraicAttr) {
        this.algebraicAttr = algebraicAttr;
    }

    public List<Tuple> getCubeLattice() {
        return cubeLattice;
    }

    public void setCubeLattice(List<Tuple> cubeLattice) {
        this.cubeLattice = cubeLattice;
    }

    public String getHolisticMeasure() {
        return holisticMeasure;
    }

    public void setHolisticMeasure(String holisticMeasure) {
        this.holisticMeasure = holisticMeasure;
    }

    public boolean isPostAggRequired() {
        return isPostAggRequired;
    }

    public void setPostAggRequired(boolean isPostAggRequired) {
        this.isPostAggRequired = isPostAggRequired;
    }

    public PhysicalOperator getPostAggLR() {
        return postAggLR;
    }

    public void setPostAggLR(PhysicalOperator postAggLR) {
        this.postAggLR = postAggLR;
    }

    public int getNumDimensions() {
        return numDimensions;
    }

    public void setNumDimensions(int numDimensions) {
        this.numDimensions = numDimensions;
    }
}
