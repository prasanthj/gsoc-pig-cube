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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCube;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.builtin.CubeDimensions;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.builtin.RollupDimensions;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.HolisticCube;
import org.apache.pig.impl.builtin.HolisticCubeCompoundKey;
import org.apache.pig.impl.builtin.PartitionMaxGroup;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.builtin.PostProcessCube;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadSingleLoader;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class CubeCompiler {

    private static final Log LOG = LogFactory.getLog(MRCompiler.class);
    private MRCompiler mrc;

    public CubeCompiler(MRCompiler mrc) throws MRCompilerException {
        this.mrc = mrc;
    }

    public void visit(POCube op) throws VisitorException {
        // if the measure is not holistic we do not need mr-cube approach
        // we can fallback to naive approach. Also in illustrate mode
        // we can just illustrate using the naive approach
        if (mrc != null && op.isHolistic() && !mrc.pigContext.inIllustrator) {
            try {
                // save the so far compiled mrjob. After inserting the new sample job
                // the old job will continue compiling other operators
                MapReduceOper prevJob = mrc.compiledInputs[0];

                // the output of sample job i.e region label and corresponding
                // value partitions will be saved to this file
                FileSpec sampleJobOutput = mrc.getTempFileSpec();

                double sampleSize = 0.0;
                sampleSize = determineSamplePercentage(op);
                if (sampleSize == 0.0) {
                    LOG.info("Input dataset is estimated to be small enough for performing naive cubing.");
                } else {
                    if (op.getAlgebraicAttr() == null) {
                        LOG.warn("[CUBE] Algebraic attribute is null. Falling back to naive cubing.");
                    } else {
                        // If the measure is found to be holistic then to get to the final results
                        // we need the following MRJobs
                        // MRJOB-1: Sample naive cube job - to determine the value partition for large groups
                        // MRJOB-2: Actual full cube job - based on output of MRJOB-1 it performs cubing
                        // and partitions the large groups to their corresponding bins and executes the measure
                        // MRJOB-3: Post aggregation job - Aggregates output of MRJOB-2 to produce final results

                        // post aggregation job need information about the number of dimensions.
                        // so storing it with POCube operator for later use by post aggregation job
                        int[] totalDimensions = new int[1];
                        mrc.curMROp = getCubeSampleJob(op, sampleSize, sampleJobOutput, totalDimensions);
                        op.setNumDimensions(totalDimensions[0]);

                        // manually connect the sample job with the previous job. This cannot be automatically done
                        // because the output of sample job is not the input of next job
                        mrc.MRPlan.add(mrc.curMROp);
                        mrc.MRPlan.connect(mrc.curMROp, prevJob);

                        // sample job is inserted now. resetting back to original compilation sequence
                        mrc.curMROp = prevJob;

                        // setting up the result of sample job (which is the annotated lattice)
                        // to the mrc.curMROp. This file will be distributed using distributed cache
                        // to all mappers running the actual full cube materialization job
                        mrc.curMROp.setAnnotatedLatticeFile(sampleJobOutput.getFileName());
                        mrc.curMROp.setFullCubeJob(true);

                        // since the requested parallelism was adjusted to 1 in sample job
                        // we need to reset the requested parallelism back to the original value
                        // MRJOB-1 requires the parallelism to be 1. MRJOB-2 and MRJOB-3 requires
                        // the parallelism as requested by the user.
                        mrc.curMROp.requestedParallelism = op.getRequestedParallelism();

                        // The original plan sequence contains CubeDimensions/RollupDimensions UDF. This
                        // cannot be used anymore. So replace it with HolisticCube UDF
                        byte algAttrType = modifyCubeUDFsForHolisticMeasure(op, sampleJobOutput.getFileName());
                        mrc.compiledInputs[0] = mrc.curMROp;

                        // add the column that contains the bin number to local rearrange.
                        // the bin numbers for large groups are calculated by algebraicAttribute%partitionFactor.
                        // save this LR in POCube as it will be required during post aggregation job.
                        PhysicalOperator lrForPostAgg = addAlgAttrColToLR(op, algAttrType);
                        op.setPostAggLR(lrForPostAgg);
                        mrc.curMROp.setMapDone(true);

                        insertPostProcessUDF(op);

                        // Post aggregation of output is required to get the correct aggregated result.
                        // We cannot insert the post aggregate job here because we have not visited the
                        // foreach that contains measure yet. While visiting the POForEach that contains
                        // holistic measure then we will append the post aggregation steps.
                        // See visitPOForEach for the conditional check that inserts post aggregation step.
                        op.setPostAggRequired(true);
                    }
                }
            } catch (PlanException e) {
                int errCode = 2167;
                String msg = "Error compiling operator " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            } catch (IOException e) {
                int errCode = 2167;
                String msg = "Error compiling operator " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    // This function adds the algebraic attribute column to dimensions list
    // which now contains algebraicAttribute%partitionFactor value
    private PhysicalOperator addAlgAttrColToLR(POCube op, byte algAttrType) throws PlanException {
        PhysicalPlan mapPlan = mrc.curMROp.mapPlan;
        PhysicalOperator succ = (PhysicalOperator) mapPlan.getLeaves().get(0);
        PhysicalOperator cloneLR;
        try {
            cloneLR = succ.clone();
        } catch (CloneNotSupportedException e) {
            throw new PlanException(e);
        }
        if (succ instanceof POLocalRearrange) {
            List<PhysicalPlan> inps = ((POLocalRearrange) succ).getPlans();
            PhysicalPlan pplan = new PhysicalPlan();
            POProject proj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            proj.setColumn(inps.size());
            proj.setResultType(algAttrType);
            pplan.add(proj);
            inps.add(pplan);
            ((POLocalRearrange) succ).setPlans(inps);
        }
        return cloneLR;
    }

    // This function determines the sampling percentage based on the estimated
    // total number of rows in the input dataset.
    private double determineSamplePercentage(POCube op) throws IOException {
        PhysicalOperator opRoot = null;
        List<MapReduceOper> roots = mrc.MRPlan.getRoots();
        long inputFileSize = 0;
        long actualTupleSize = 0;
        long estTotalRows = 0;
        double sampleSize = 0.0;

        PhysicalPlan mapPlan = roots.get(0).mapPlan;
        opRoot = mapPlan.getRoots().get(0);

        if (opRoot instanceof POLoad) {
            inputFileSize = getInputFileSize((POLoad) opRoot);
            actualTupleSize = getActualTupleSize((POLoad) opRoot);
            estTotalRows = inputFileSize / actualTupleSize;

            // Refer mr-cube paper (http://arnab.org/files/mrcube.pdf)
            // page #6 for sample selection and experimentation
            if (estTotalRows > 2000000000) {
                // for #rows beyond 2B, 2M samples are sufficient
                sampleSize = (double) 2000000 / (double) estTotalRows;
            } else if (estTotalRows > 2000000 && estTotalRows < 2000000000) {
                // for #rows between 2M to 2B, 100K tuples are sufficient
                sampleSize = (double) 100000 / (double) estTotalRows;
            } else {
                sampleSize = 0.0;
            }
        }

        return sampleSize;
    }

    // This method modifies the map plan containing CubeDimensions/RollupDimensions and replaces
    // it will HolisticCube UDF.
    private byte modifyCubeUDFsForHolisticMeasure(POCube op, String annotatedLatticeFile) throws PlanException {
        byte algAttrType = DataType.BYTEARRAY;
        PhysicalPlan mapPlan = mrc.curMROp.mapPlan;
        PhysicalOperator oper = mapPlan.getRoots().get(0);
        List<PhysicalOperator> succs = null;

        while ((succs = mapPlan.getSuccessors(oper)) != null) {
            // Iterate through the plan to get the inputs for HolisticCube UDF
            // i.e. all projections from CubeDimensions/RollupDimensions UDFs
            for (PhysicalOperator succ : succs) {
                // PhysicalOperator pop = poMap.get(entry.getKey());
                if (succ instanceof POForEach) {
                    String[] ufArgs = new String[op.getCubeLattice().size()];
                    getLatticeAsStringArray(ufArgs, op.getCubeLattice());
                    PhysicalPlan dimPlan = new PhysicalPlan();
                    POUserFunc hUserFunc = new POUserFunc(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)),
                            op.getRequestedParallelism(), null, new FuncSpec(HolisticCube.class.getName(), ufArgs));
                    hUserFunc.setResultType(DataType.BAG);
                    dimPlan.add(hUserFunc);

                    List<PhysicalPlan> feIPlans = new ArrayList<PhysicalPlan>();
                    List<Boolean> feFlat = new ArrayList<Boolean>();
                    feIPlans.add(dimPlan);
                    feFlat.add(true);

                    // connect the dimensions from CubeDimensions and RollupDimensions UDF to
                    // a separate foreach operator containing HolisticCube UDF.
                    // Once all dimensions are connected replace the existing foreach operator
                    // with the newly created foreach operator
                    for (PhysicalPlan pp : ((POForEach) succ).getInputPlans()) {
                        for (PhysicalOperator leaf : pp.getLeaves()) {
                            if (leaf instanceof POUserFunc) {
                                String className = ((POUserFunc) leaf).getFuncSpec().getClassName();
                                if (className.equals(CubeDimensions.class.getName())) {
                                    for (PhysicalOperator expOp : ((POUserFunc) leaf).getInputs()) {
                                        dimPlan.add(expOp);
                                        // if its a cast operator then connect the inner projections
                                        if (expOp instanceof POCast) {
                                            for (PhysicalOperator projOp : ((POCast) expOp).getInputs()) {
                                                dimPlan.add(projOp);
                                                dimPlan.connect(projOp, expOp);
                                            }
                                        }
                                        dimPlan.connect(expOp, hUserFunc);
                                    }
                                } else if (className.equals(RollupDimensions.class.getName())) {
                                    for (PhysicalOperator expOp : ((POUserFunc) leaf).getInputs()) {
                                        dimPlan.add(expOp);
                                        // if its a cast operator then connect the inner projections
                                        if (expOp instanceof POCast) {
                                            for (PhysicalOperator projOp : ((POCast) expOp).getInputs()) {
                                                dimPlan.add(projOp);
                                                dimPlan.connect(projOp, expOp);
                                            }
                                        }
                                        dimPlan.connect(expOp, hUserFunc);
                                    }
                                }
                            } else {
                                // if not UserFunc then it will be ProjectExpression/CastExpression.
                                // These projections will be the non-dimensional columns.
                                // Find the algebraic attribute column from the non-dimensional
                                // columns and project a clone of it as a last dimensional column.
                                PhysicalPlan nonDimPlan = new PhysicalPlan();

                                // if its a cast operator then connect the inner projections
                                if (leaf instanceof POCast) {
                                    for (PhysicalOperator castInp : ((POCast) leaf).getInputs()) {
                                        // these non-dimensional columns are projected only after UserFuncExpressions
                                        // so we can be sure that the algebraic attribute will be the
                                        // last one to be appended to dimensions list
                                        if (op.getAlgebraicAttr().equals(((POCast) leaf).getFieldSchema().getName())) {
                                            try {
                                                POCast cloneCast = (POCast) leaf.clone();
                                                // add cloned copy to dimension list and original to non-dimensional
                                                // list
                                                dimPlan.add(cloneCast);
                                                nonDimPlan.add(leaf);
                                                algAttrType = ((POCast) leaf).getResultType();
                                                for (PhysicalOperator hprojOp : ((POCast) leaf).getInputs()) {
                                                    POProject cloneProj = (POProject) hprojOp.clone();
                                                    cloneProj.setInputs(hprojOp.getInputs());

                                                    List<PhysicalOperator> castInps = new ArrayList<PhysicalOperator>();
                                                    castInps.add(cloneProj);
                                                    cloneCast.setInputs(castInps);

                                                    dimPlan.add(cloneProj);
                                                    dimPlan.connect(cloneProj, cloneCast);

                                                    nonDimPlan.add(castInp);
                                                    nonDimPlan.connect(castInp, leaf);
                                                }
                                                dimPlan.connect(cloneCast, hUserFunc);
                                                feIPlans.add(nonDimPlan);
                                                feFlat.add(false);
                                            } catch (CloneNotSupportedException e) {
                                                throw new PlanException(e);
                                            }
                                        } else {
                                            nonDimPlan.add(leaf);
                                            nonDimPlan.add(castInp);
                                            nonDimPlan.connect(castInp, leaf);
                                            feIPlans.add(nonDimPlan);
                                            feFlat.add(false);
                                        }
                                    }
                                } else {
                                    // else it will be POProject
                                    // POProject doesn't store the alias of the projected column.
                                    // the alias of the columns are attached to POCast and not to POProject.
                                    // alias of the column is a MUST for holistic cubing because the
                                    // algebraic attribute will be identified by the column alias and
                                    // projected to HolisticCube UDF
                                    // FIXME: Don't know how to handle this case!!
                                    throw new PlanException(
                                            "Cannot determine algebraic attribute's alias from POProject. "
                                                    + "May be a cast is missing in the schema corresponding to algebraic attribute '"
                                                    + op.getAlgebraicAttr() + "'.");
                                }
                            }
                        }
                    }
                    // we will create a new foreach operator containing HolisticCube UDF with all dimension
                    // and non-dimension columns and then replace with old foreach
                    POForEach foreach = new POForEach(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)),
                            op.getRequestedParallelism(), feIPlans, feFlat);
                    foreach.addOriginalLocation(succ.getAlias(), succ.getOriginalLocations());
                    foreach.setInputs(succ.getInputs());
                    mapPlan.replace(succ, foreach);
                }
                // to facilitate iteration
                oper = succ;
            }
        }
        return algAttrType;
    }

    // This function inserts PostProcessCube UDF to the reduce plan of the full cube job.
    // The PostProcessCube UDF just strips off the bin numbers from key and values
    private void insertPostProcessUDF(POCube op) throws PlanException {
        List<PhysicalPlan> feIPlans = new ArrayList<PhysicalPlan>();
        List<Boolean> feFlat = new ArrayList<Boolean>();
        POForEach foreach = null;

        PhysicalPlan fPlan = new PhysicalPlan();
        POProject projStar = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
        projStar.setStar(true);
        fPlan.add(projStar);

        POUserFunc userFunc = new POUserFunc(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)),
                op.getRequestedParallelism(), null, new FuncSpec(PostProcessCube.class.getName()));
        userFunc.setResultType(DataType.TUPLE);
        fPlan.add(userFunc);
        fPlan.connect(projStar, userFunc);
        feIPlans.add(fPlan);
        feFlat.add(true);

        foreach = new POForEach(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1, feIPlans, feFlat);
        foreach.setResultType(DataType.BAG);
        try {
            foreach.visit(mrc);
        } catch (VisitorException e) {
            throw new PlanException(e);
        }
    }

    // This method modifies the current mr-plan to insert a new sampling job.
    // This function contains the following sequence of operatos
    // 1) POLoad
    // 2) POForEach with star projection
    // 3) POFilter with ConstantExpression containing sample size and LessThanExpr
    // The way POFilter is used is similar to the way SAMPLE operator works
    // 4) POForEach with HolisticCubeCompoundKey UDF
    // 5) POLocalRearrange
    // 6) POGlobalRearrange
    // 7) Finally attaches a reduce plan
    private MapReduceOper getCubeSampleJob(POCube op, double sampleSize, FileSpec sampleJobOutput, int[] totalDimensions)
            throws VisitorException {
        try {
            PhysicalOperator opRoot = null;
            POForEach foreach = null;

            MapReduceOper mro = mrc.getMROp();
            mrc.curMROp = mro;
            // For sample job we need just one reducer to store annotated lattice
            mrc.curMROp.requestedParallelism = 1;
            mrc.curMROp.setMapDone(false);
            mrc.compiledInputs[0] = mrc.curMROp;
            long inputFileSize = 0;
            long actualTupleSize = 0;

            List<PhysicalOperator> dimOperators = new ArrayList<PhysicalOperator>();
            List<PhysicalOperator> nonDimOperators = new ArrayList<PhysicalOperator>();
            List<Boolean> dimFlat = new ArrayList<Boolean>();
            List<MapReduceOper> leaves = mrc.MRPlan.getLeaves();

            PhysicalPlan mapPlan = leaves.get(0).mapPlan;
            opRoot = mapPlan.getRoots().get(0);

            if (opRoot instanceof POLoad) {
                mro.mapPlan.add(opRoot);
                inputFileSize = getInputFileSize((POLoad) opRoot);
                actualTupleSize = getActualTupleSize((POLoad) opRoot);
            }

            // if the predecessor of cube operator is load then there will
            // be a foreach plan with CubeDimensions/RollupDimensions UDFs
            // we do not need those udfs for sampling we just need the dimension
            // columns and non-dimensional columns. The CubeDimensions/RollupDimensions
            // UDF will be replaced by HolisticCubeCompoundKey UDF with all the
            // dimension columns attached to it.

            // Foreach with star projection
            List<PhysicalPlan> feIPlans = new ArrayList<PhysicalPlan>();
            List<Boolean> feFlat = new ArrayList<Boolean>();

            PhysicalPlan projStarPlan = new PhysicalPlan();
            POProject projStar = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            projStar.setStar(true);
            projStarPlan.add(projStar);
            feIPlans.add(projStarPlan);
            feFlat.add(false);
            foreach = new POForEach(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1, feIPlans, feFlat);
            foreach.setResultType(DataType.BAG);
            foreach.visit(mrc);

            // collect all dimension columns and non-dimension columns
            // dimension columns will be passed to HolisticCubeCompoundKey UDF
            PhysicalOperator po = mapPlan.getSuccessors(opRoot).get(0);
            if (po instanceof POForEach) {
                for (PhysicalPlan iPlan : ((POForEach) po).getInputPlans()) {
                    for (PhysicalOperator pOp : iPlan.getLeaves()) {
                        if (pOp instanceof POUserFunc) {
                            for (PhysicalOperator inp : pOp.getInputs()) {
                                dimOperators.add(inp);
                            }
                        } else {
                            // The non-dimensional columns will be pushed down.
                            // these columns will be used later by measures
                            nonDimOperators.add(pOp);
                        }
                    }
                }
            }

            totalDimensions[0] = dimOperators.size();

            // Filter plan replicating the SAMPLE operator
            POFilter filter = new POFilter(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            filter.setResultType(DataType.BAG);
            filter.visit(mrc);

            PhysicalPlan filterInnerPlan = new PhysicalPlan();
            POUserFunc randomUF = new POUserFunc(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1,
                    null, new FuncSpec(RANDOM.class.getName()));
            randomUF.setResultType(DataType.DOUBLE);
            filterInnerPlan.add(randomUF);

            ConstantExpression ce = new ConstantExpression(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            ce.setValue(sampleSize);
            ce.setResultType(DataType.DOUBLE);
            filterInnerPlan.add(ce);

            LessThanExpr le = new LessThanExpr(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            le.setResultType(DataType.BOOLEAN);
            le.setOperandType(DataType.DOUBLE);
            le.setLhs(randomUF);
            le.setRhs(ce);
            filterInnerPlan.add(le);

            filterInnerPlan.connect(randomUF, le);
            filterInnerPlan.connect(ce, le);
            filter.setPlan(filterInnerPlan);

            // Foreach plan with HolisticCubeCoupundKey UDF
            List<PhysicalPlan> foreachInpPlans = new ArrayList<PhysicalPlan>();
            PhysicalPlan userFuncPlan = new PhysicalPlan();
            String[] lattice = new String[op.getCubeLattice().size()];
            getLatticeAsStringArray(lattice, op.getCubeLattice());
            POUserFunc hUserFunc = new POUserFunc(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1,
                    null, new FuncSpec(HolisticCubeCompoundKey.class.getName(), lattice));
            hUserFunc.setResultType(DataType.BAG);
            userFuncPlan.add(hUserFunc);

            // add dimensional columns
            for (PhysicalOperator dimOp : dimOperators) {
                if (dimOp instanceof POCast) {
                    for (PhysicalOperator innerOp : dimOp.getInputs()) {
                        POProject innerProj = new POProject(
                                new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
                        innerProj.setResultType(innerOp.getResultType());
                        innerProj.setColumn(((POProject) innerOp).getColumn());
                        userFuncPlan.add(innerProj);
                        userFuncPlan.connect(innerProj, hUserFunc);
                    }
                } else if (dimOp instanceof POProject) {
                    POProject proj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
                    proj.setResultType(dimOp.getResultType());
                    proj.setColumn(((POProject) dimOp).getColumn());
                    userFuncPlan.add(proj);
                    userFuncPlan.connect(proj, hUserFunc);
                }
            }
            foreachInpPlans.add(userFuncPlan);
            dimFlat.add(true);

            // add non-dimensional columns
            for (PhysicalOperator nDimOp : nonDimOperators) {
                PhysicalPlan pPlan = new PhysicalPlan();
                if (nDimOp instanceof POCast) {
                    for (PhysicalOperator innerOp : nDimOp.getInputs()) {
                        POProject innerProj = new POProject(
                                new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
                        innerProj.setResultType(innerOp.getResultType());
                        innerProj.setColumn(((POProject) innerOp).getColumn());
                        pPlan.add(innerProj);
                    }
                } else if (nDimOp instanceof POProject) {
                    POProject proj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
                    proj.setResultType(nDimOp.getResultType());
                    proj.setColumn(((POProject) nDimOp).getColumn());
                    pPlan.add(proj);
                }
                foreachInpPlans.add(pPlan);
                dimFlat.add(false);
            }

            POForEach fe = new POForEach(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1,
                    foreachInpPlans, dimFlat);
            fe.setResultType(DataType.BAG);
            fe.visit(mrc);

            // Rearrange operations
            List<PhysicalPlan> lrPlans = new ArrayList<PhysicalPlan>();
            PhysicalPlan lrPlan = new PhysicalPlan();

            // POProject's for Local Rearrange
            POProject lrProj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            lrProj.setColumn(0);
            lrProj.setResultType(DataType.TUPLE);
            lrPlan.add(lrProj);
            lrPlans.add(lrPlan);

            // create local rearrange
            POLocalRearrange lr = new POLocalRearrange(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            lr.setKeyType(DataType.TUPLE);
            lr.setIndex(0);
            lr.setPlans(lrPlans);
            lr.setResultType(DataType.TUPLE);
            lr.visit(mrc);

            // create POGlobalRearrange
            POGlobalRearrange gr = new POGlobalRearrange(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            gr.setResultType(DataType.TUPLE);
            gr.visit(mrc);

            // generate the reduce plan for this sampling job
            generateReducePlanSampleJob(op, mro, sampleJobOutput, actualTupleSize, inputFileSize);
            return mro;
        } catch (PlanException e) {
            int errCode = 2167;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        } catch (ExecException e) {
            int errCode = 2167;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        } catch (IOException e) {
            int errCode = 2167;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    // Relies on InputSizeReducerEstimator for getting the total file size
    // It will try to determine the file size from the loader or fallback
    // to HDFS to get the file size
    private long getInputFileSize(POLoad proot) throws IOException {
        Configuration conf = new Configuration();
        List<POLoad> loads = new ArrayList<POLoad>();
        loads.add(proot);
        return InputSizeReducerEstimator.getTotalInputFileSize(conf, loads, new org.apache.hadoop.mapreduce.Job(conf));
    }

    // FIXME ReadSingleLoader loads only the first tuple in the dataset
    // to find the raw tuple size. This will not be accurate because some
    // fields may have variable size (bytearray/chararray, empty values for
    // certain fields). Hence need to figure out a better way for finding the
    // average raw tuple size. Because of this estimation of number of rows will
    // have high error rate.
    // Try: Read n random samples and then determine the average tuple size
    private long getActualTupleSize(POLoad proot) throws IOException {
        long size = 0;
        Configuration conf = new Configuration();
        String fileName = proot.getLFile().getFileName();
        ReadSingleLoader rsl;
        try {
            rsl = new ReadSingleLoader(proot.getLoadFunc(), conf, fileName, 0);
        } catch (IOException e) {
            throw new IOException(fileName + " does not exists or contents empty.");
        }
        size = rsl.getRawTupleSize();
        if (size == -1) {
            throw new IOException("Cannot determine the raw size of the tuple.");
        }
        return size;
    }

    private long getBytesPerReducer() {
        Configuration conf = new Configuration();
        return conf.getLong(PigReducerEstimator.BYTES_PER_REDUCER_PARAM, PigReducerEstimator.DEFAULT_BYTES_PER_REDUCER);
    }

    // This method generates reduce plan for cube sample job.
    // It inserts the following operators to the plan
    // 1) POPackage
    // 2) POForEach with PartitionMaxGroup UDF
    // 3) POSort for enabling secondary sort
    // 4) End the plan by storing the annotated lattice
    private void generateReducePlanSampleJob(POCube op, MapReduceOper mro, FileSpec sampleJobOutput,
            long actualTupleSize, long overallDataSize) throws VisitorException {
        try {
            // create POPackage
            POPackage pkg = new POPackage(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            pkg.setKeyType(DataType.TUPLE);
            pkg.setResultType(DataType.TUPLE);
            pkg.setNumInps(1);
            boolean[] inner = { false };
            pkg.setInner(inner);
            pkg.visit(mrc);

            // Foreach with PartitionMaxGroup UDF
            List<PhysicalPlan> inpPlans = new ArrayList<PhysicalPlan>();
            List<Boolean> flat = new ArrayList<Boolean>();

            PhysicalPlan projPlan = new PhysicalPlan();
            POProject proj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            proj.setColumn(0);
            proj.setResultType(DataType.TUPLE);
            projPlan.add(proj);
            inpPlans.add(projPlan);
            flat.add(false);

            // Reuse the property used by skewedjoin for getting the percentage usage of memory
            String percentMemUsage = mrc.pigContext.getProperties().getProperty("pig.skewedjoin.reduce.memusage",
                    String.valueOf(PartitionSkewedKeys.DEFAULT_PERCENT_MEMUSAGE));
            PhysicalPlan ufPlan = new PhysicalPlan();
            String[] ufArgs = new String[4];
            ufArgs[0] = String.valueOf(overallDataSize);
            ufArgs[1] = String.valueOf(getBytesPerReducer());
            ufArgs[2] = String.valueOf(actualTupleSize);
            ufArgs[3] = String.valueOf(percentMemUsage);
            POUserFunc uf = new POUserFunc(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)), -1, null,
                    new FuncSpec(PartitionMaxGroup.class.getName(), ufArgs));
            uf.setResultType(DataType.TUPLE);
            ufPlan.add(uf);
            flat.add(true);

            // project only the value from package operator. key is not required since the value itself contains
            // the key in 1st field of each tuple
            POProject ufProj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            ufProj.setColumn(1);
            ufProj.setResultType(DataType.BAG);
            ufPlan.add(ufProj);
            inpPlans.add(ufPlan);

            // enable secondary sort on group values
            List<Boolean> ascCol = new ArrayList<Boolean>();
            List<PhysicalPlan> sortPlans = new ArrayList<PhysicalPlan>();
            ascCol.add(false);
            POSort sort = new POSort(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)),
                    op.getRequestedParallelism(), null, sortPlans, ascCol, null);
            sort.setResultType(DataType.BAG);

            List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
            inputs.add(ufProj);

            PhysicalPlan sortPlan = new PhysicalPlan();
            POProject sortProj = new POProject(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)));
            sortProj.setColumn(1);
            sortProj.setResultType(DataType.TUPLE);
            sortPlan.add(sortProj);
            sortPlans.add(sortPlan);
            sort.setInputs(inputs);

            ufPlan.add(sort);
            ufPlan.connect(ufProj, sort);
            ufPlan.connect(sort, uf);

            // create ForEach with PartitionMaxGroup for finding the group size
            POForEach fe = new POForEach(new OperatorKey(mrc.scope, mrc.nig.getNextNodeId(mrc.scope)),
                    op.getRequestedParallelism(), inpPlans, flat);
            fe.setResultType(DataType.BAG);
            fe.visit(mrc);

            // finally store the annotated lattice to HDFS
            // this file will contains tuples with 2 fields
            // 1st field - region label
            // 2nd field - partition factor
            mrc.endSingleInputPlanWithStr(sampleJobOutput);
        } catch (PlanException e) {
            int errCode = 2167;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        } catch (IOException e) {
            int errCode = 2167;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    private void getLatticeAsStringArray(String[] ufArgs, List<Tuple> cubeLattice) {
        for (int i = 0; i < cubeLattice.size(); i++) {
            ufArgs[i] = cubeLattice.get(i).toString();
            // strip off the parantheses when tuple is converted to string
            ufArgs[i] = ufArgs[i].substring(1, ufArgs[i].length() - 1);
        }
    }
}
