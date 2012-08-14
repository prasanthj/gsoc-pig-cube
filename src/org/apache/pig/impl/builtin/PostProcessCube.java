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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This UDF is used by Cube operator for holistic cubing.
 * It strips off the reducer number value that is inserted
 * into each tuples. The map job of full holistic cube job
 * appends algebraicAttribute%partitionFactor to the end of
 * key and to the end of dimensional list inside values. This
 * value is inserted by the map job to make sure algebraic attributes
 * with same values goes to the same reducer.
 * 
 * For example: 
 * Input tuple: ((midwest,OH,2),{(midwest,OH,2,1007,1986,$10000),(midwest,OH,2,1007,1987,$20000)})
 * Output tuple: ((midwest,OH),{(midwest,OH,1007,1986,$10000),(midwest,OH,1007,1987,$20000)})
 *  
 */

public class PostProcessCube extends EvalFunc<Tuple> {

    private TupleFactory tf;
    private BagFactory bf;

    // for debugging
    boolean printOutputOnce = false;
    boolean printInputOnce = false;

    public PostProcessCube() {
	this.tf = TupleFactory.getInstance();
	this.bf = BagFactory.getInstance();
    }
    
    /**
     * @param in - input tuple with first field as tuple and second field as bag
     * The input tuple is from POPackage operator.
     * @return tuple with algebraicAttribute%patitionFactor value stripped off
     */
    public Tuple exec(Tuple in) throws IOException {
	if (printInputOnce == false) {
	    log.info("[CUBE] Input: " + in);
	    printInputOnce = true;
	}

	Tuple keyTuple = (Tuple) in.get(0);

	Tuple key = tf.newTuple(keyTuple.size() - 1);
	for (int i = 0; i < keyTuple.size() - 1; i++) {
	    key.set(i, keyTuple.get(i));
	}
	in.set(0, key);

	DataBag valueBag = (DataBag) in.get(1);
	int vpField = keyTuple.size() - 1;

	Iterator<Tuple> iter = valueBag.iterator();
	List<Tuple> resultBag = new ArrayList<Tuple>();
	while (iter.hasNext()) {
	    Tuple tup = iter.next();
	    Tuple newt = tf.newTuple(tup.size() - 1);
	    int idx = 0;
	    // We copy all the fields except the field
	    // with value partition
	    for (int i = 0; i < tup.size(); i++) {
		if (i != vpField) {
		    newt.set(idx, tup.get(i));
		    idx++;
		}
	    }
	    resultBag.add(newt);
	}
	in.set(1, bf.newDefaultBag(resultBag));

	if (printOutputOnce == false) {
	    log.info("[CUBE] Output: " + in);
	    printOutputOnce = true;
	}
	return in;
    }

    public Type getReturnType() {
	return Tuple.class;
    }
}
