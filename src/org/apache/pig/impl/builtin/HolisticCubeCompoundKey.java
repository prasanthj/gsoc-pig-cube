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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

/**
 * TODO write doc
 */

public class HolisticCubeCompoundKey extends EvalFunc<DataBag> {

    private TupleFactory tf;
    private BagFactory bf;
    private Log log = LogFactory.getLog(getClass());
    private List<Tuple> cl;

    // for debugging
    boolean printOutputOnce = false;
    boolean printInputOnce = false;
    
    public HolisticCubeCompoundKey() {
	this(null);
    }

    public HolisticCubeCompoundKey(String[] args) {
	tf = TupleFactory.getInstance();
	bf = BagFactory.getInstance();
	cl = new ArrayList<Tuple>();
	stringArrToTupleList(cl, args);
	log.info("[CUBE] lattice - " + cl);
    }

    private void stringArrToTupleList(List<Tuple> cl, String[] args) {

	// region labels are csv strings. if trailing values of region label
	// are null/empty then split function ignores those values. specify some
	// integer value greater than the number of output tokens makes sure
	// that all null values in region label are assigned an empty string. first
	// value which is the most detailed level in the lattice doesn't have null values
	int maxIdx = args[0].length() + 1;

	for (String arg : args) {
	    Tuple newt = tf.newTuple();
	    String[] tokens = arg.split(",", maxIdx);
	    for (String token : tokens) {
		if (token.equals("") == true) {
		    newt.append(null);
		} else {
		    newt.append(token);
		}
	    }
	    cl.add(newt);
	}
    }

    /**
     * @param in
     *            - input tuple
     * @return - tuple have 3 fields (compound keys) 1 - primary key - region label 2 - secondary key - group 3 - value
     *         - 1
     */
    public DataBag exec(Tuple in) throws IOException {
	if( printInputOnce == false) {
	    log.info("[CUBE] Input - " + in);
	    printInputOnce = true;
	}
	
	if (in == null || in.size() == 0) {
	    return null;
	}
	Utils.convertNullToUnknown(in);
	List<Tuple> groups = getAllCubeCombinations(in);
	List<Tuple> results = new ArrayList<Tuple>(cl.size());

	for (int i = 0; i < groups.size(); i++) {
	    Tuple t = tf.newTuple(3);
	    t.set(0, cl.get(i));
	    t.set(1, groups.get(i));
	    t.set(2, (long) 1);
	    results.add(t);
	}

	if( printOutputOnce == false) {
	    log.info("[CUBE] Output - " + bf.newDefaultBag(results));
	    printOutputOnce = true;
	}
	
	return bf.newDefaultBag(results);
    }

    private List<Tuple> getAllCubeCombinations(Tuple in) throws IOException {
	if (cl == null || cl.size() == 0) {
	    throw new RuntimeException("Lattice cannot be null or empty.");
	}

	List<Tuple> result = new ArrayList<Tuple>(in.size());
	for (Tuple region : cl) {
	    Tuple newt = tf.newTuple(in.getAll());
	    if (region.size() != in.size()) {
		throw new RuntimeException(
		        "Number of fields in tuple should be equal to the number of fields in region tuple.");
	    }
	    for (int i = 0; i < region.size(); i++) {
		if (region.get(i) == null) {
		    newt.set(i, null);
		}
	    }
	    result.add(newt);
	}
	return result;
    }

    public Type getReturnType() {
	return DataBag.class;
    }
}
