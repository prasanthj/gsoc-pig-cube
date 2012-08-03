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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * TODO write doc TODO implement algebraic interface
 */

public class MaxGroupSize extends EvalFunc<Tuple> {

    private Log log = LogFactory.getLog(getClass());
    private long totalSampleCount;
    private TupleFactory tf;
    private long inMemTupleSize;
    private long overallDataSize;
    private boolean isFirstTuple;

    // Using this default value from skewed join code
    public static final float DEFAULT_PERCENT_MEMUSAGE = 0.3f;

    public MaxGroupSize() {
	this(null);
    }

    public MaxGroupSize(String[] args) {
	tf = TupleFactory.getInstance();
	this.totalSampleCount = 0;
	this.overallDataSize = Long.valueOf(args[0]);
	this.inMemTupleSize = 0;
	this.isFirstTuple = true;
    }

    /**
     * @param in
     *            - input tuple
     * @return - tuple have 2 fields 1 - max group size 2 - partition factor
     */
    public Tuple exec(Tuple in) throws IOException {
	log.info("[CUBE] Input - " + in);

	if (in == null || in.size() == 0) {
	    return null;
	}

	Tuple result = tf.newTuple(1);
	Tuple prevGroup = null;
	DataBag bg = (DataBag) in.get(0);
	Iterator<Tuple> iter = bg.iterator();
	long grpCount = 0;
	long maxGroupSize = 0;
	int partitionFactor = 0;
	long firstGroupSize = 0;

	while (iter.hasNext()) {
	    Tuple tup = iter.next();
	    Tuple currGroup = (Tuple) tup.get(1);
	    if (prevGroup == null) {
		prevGroup = currGroup;
	    }

	    if (isFirstTuple == true) {
		firstGroupSize = firstGroupSize + (Long) tup.get(2);
	    }
	    if (currGroup.equals(prevGroup) == true) {
		grpCount = grpCount + (Long) tup.get(2);
	    } else {
		if (grpCount > maxGroupSize) {
		    maxGroupSize = grpCount;
		}
		grpCount = (Long) tup.get(2);
		prevGroup = currGroup;
	    }
	}

	// corner case: if last group is largest
	if (grpCount > maxGroupSize) {
	    maxGroupSize = grpCount;
	}

	if (isFirstTuple == true) {
	    // first tuple will be the grand total <*,*,*>
	    // whose size is equal to the total sample count
	    totalSampleCount = firstGroupSize;
	    isFirstTuple = false;
	}
	partitionFactor = determinePartitionFactor(maxGroupSize, in);
	result.set(0, partitionFactor);
	log.info("[CUBE] Output tuple - " + result);
	return result;
    }

    private int determinePartitionFactor(long maxGroupSize, Tuple in) throws ExecException {
	// a region is identified reducer unfriendly if the group size is more
	// than 0.75rN, where r is the ratio of number of tuples that a reducer
	// can
	// handle vs overall data size and N is the total sample size.
	// This equation is taken from mr-cube paper.
	int partitionFactor = 0;
	long heapMemAvail = (long) (Runtime.getRuntime().maxMemory() * DEFAULT_PERCENT_MEMUSAGE);
	if (inMemTupleSize == 0) {
	    inMemTupleSize = getTupleSize(in);
	    log.info("[CUBE] Input bag size " + in.getMemorySize() + " bytes");
	    log.info("[CUBE] In-memory tuple size " + inMemTupleSize + " bytes");
	    log.info("[CUBE] Maximum available heap memory is " + heapMemAvail + " bytes");
	    log.info("[CUBE] Max. tuples by reducer : " + heapMemAvail / inMemTupleSize);
	    double r = 100 * ((double) (heapMemAvail / inMemTupleSize) / (double) overallDataSize);
	    log.info("[CUBE] Ratio (r): " + r);
	    log.info("[CUBE] Threshold: " + (0.75 * r * totalSampleCount));
	}
	long maxTuplesByReducer = heapMemAvail / inMemTupleSize;
	double r = ((double) maxTuplesByReducer / (double) overallDataSize) * 100;
	double threshold = 0.75 * r * totalSampleCount;
	if (maxGroupSize > threshold) {
	    log.info("[CUBE] Group size: " + maxGroupSize + " is reducer un-friendly.");
	    partitionFactor = (int) Math.round(maxGroupSize / (r * totalSampleCount));
	    log.info("[CUBE] REDUCER UN-FRIENDLY region. Partition factor: " + partitionFactor);
	} else {
	    log.info("[CUBE] Group size: " + maxGroupSize + " is reducer friendly.");
	}
	return partitionFactor;
    }

    private long getTupleSize(Tuple in) throws ExecException {
	// input tuple is a bag with tuples having multiple fields
	// Ex. {((city,state), (columbus,Ohio), $1000, 2012),(..),(..)}
	// 1st field in a tuple is a tuple with region label
	// 2nd field in a tuple is a tuple with group values
	// 3..nth fields are the dimensions that are pushed down from input
	// While calculating in-memory tuple size, the region label could be
	// omitted because in the actual mr-job the region label will not sent
	// to the reducers
	DataBag bg = (DataBag) in.get(0);
	Tuple tup = bg.iterator().next();
	Tuple newTup = tf.newTuple(tup.getAll().size());
	for (int i = 1; i < tup.getAll().size(); i++) {
	    newTup.set(i - 1, tup.get(i));
	}
	return newTup.getMemorySize();
    }

    public Type getReturnType() {
	return Tuple.class;
    }
}
