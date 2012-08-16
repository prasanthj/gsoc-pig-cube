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

package org.apache.pig.newplan.logical.relational;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

/**
 * CUBE operator implementation for data cube computation.
 * <p>
 * Cube operator syntax
 * 
 * <pre>
 * {@code alias = CUBE rel BY { CUBE | ROLLUP }(col_ref) [, { CUBE | ROLLUP }(col_ref) ...];}
 * alias - output alias 
 * CUBE - operator
 * rel - input relation
 * BY - operator
 * CUBE | ROLLUP - cube or rollup operation
 * col_ref - column references or * or range in the schema referred by rel
 * </pre>
 * 
 * </p>
 * <p>
 * The cube computation and rollup computation using UDFs {@link org.apache.pig.builtin.CubeDimensions} and
 * {@link org.apache.pig.builtin.RollupDimensions} can now be represented like below
 * 
 * <pre>
 * {@code
 * events = LOAD '/logs/events' USING EventLoader() AS (lang, event, app_id, event_id, total);
 * eventcube = CUBE events BY CUBE(lang, event), ROLLUP(app_id, event_id);
 * result = FOREACH eventcube GENERATE FLATTEN(group) as (lang, event),
 *          COUNT_STAR(cube), SUM(cube.total);
 * STORE result INTO 'cuberesult';
 * }
 * </pre>
 * 
 * In the above example, CUBE(lang, event) will generate all combinations of aggregations {(lang,event), (lang,),
 * (,event), (,)}. For n dimensions, 2^n combinations of aggregations will be generated.
 * 
 * Similarly, ROLLUP(app_id, event_id) will generate aggregations from the most detailed to the most general
 * (grandtotal) level in the hierarchical order like {(app_id,event_id), (app_id,), (,)}. For n dimensions, n+1
 * combinations of aggregations will be generated.
 * 
 * The output of the above example will have the following combinations of aggregations {(lang, event, app_id,
 * event_id), (lang, , app_id, event_id), (, event, app_id, event_id), (, , app_id, event_id), (lang, event, app_id, ),
 * (lang, , app_id, ), (, event, app_id, ), (, , app_id, ), (lang, event, , ), (lang, , , ), (, event, , ), (, , , ),}
 * 
 * Total number of combinations will be ( 2^n * (n+1) )
 * </p>
 */
public class LOCube extends LogicalRelationalOperator {
    public static final String CUBE_OP = "CUBE";
    public static final String ROLLUP_OP = "ROLLUP";
    private MultiMap<Integer, LogicalExpressionPlan> mExpressionPlans;
    private List<String> operations;
    private MultiMap<Integer, String> dimensions;
    private String algebraicAttr;
    private int algebraicAttrCol;

    /*
     * This is a map storing Uids which have been generated for an input This map is required to make the uids
     * persistant between calls of resetSchema and getSchema
     */
    private Map<Integer, Long> generatedInputUids = new HashMap<Integer, Long>();

    public LOCube(LogicalPlan plan) {
	super("LOCube", plan);
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
	// if schema is calculated before, just return
	if (schema != null) {
	    return schema;
	}

	// just return the immediate predecessor's schema
	List<Operator> preds = plan.getPredecessors(this);
	schema = null;
	if (preds != null) {
	    for (Operator pred : preds) {
		if (pred instanceof LOCogroup) {
		    schema = ((LogicalRelationalOperator) pred).getSchema();
		}
	    }
	}

	return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
	try {
	    ((LogicalRelationalNodesVisitor) v).visit(this);
	} catch (ClassCastException cce) {
	    throw new FrontendException("Expected LogicalPlanVisitor", cce);
	}
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
	try {
	    return checkEquality((LogicalRelationalOperator) other);
	} catch (ClassCastException cce) {
	    throw new FrontendException("Exception while casting CUBE operator", cce);
	}
    }

    public MultiMap<Integer, LogicalExpressionPlan> getExpressionPlans() {
	return mExpressionPlans;
    }

    public void setExpressionPlans(MultiMap<Integer, LogicalExpressionPlan> plans) {
	this.mExpressionPlans = plans;
    }

    @Override
    public void resetUid() {
	generatedInputUids = new HashMap<Integer, Long>();
    }

    public List<Operator> getInputs(LogicalPlan plan) {
	return plan.getPredecessors(this);
    }

    public String getAlgebraicAttr() {
	return algebraicAttr;
    }

    public void setAlgebraicAttr(String algebraicAttr) {
	this.algebraicAttr = algebraicAttr;
    }

    public MultiMap<Integer, String> getDimensions() {
	return dimensions;
    }

    public void setDimensions(MultiMap<Integer, String> dimensions) {
	this.dimensions = dimensions;
    }

    public List<String> getOperations() {
	return operations;
    }

    public void setOperations(List<String> operations) {
	this.operations = operations;
    }

    public int getAlgebraicAttrCol() {
	return algebraicAttrCol;
    }

    public void setAlgebraicAttrCol(int algebraicAttrCol) {
	this.algebraicAttrCol = algebraicAttrCol;
    }
}
