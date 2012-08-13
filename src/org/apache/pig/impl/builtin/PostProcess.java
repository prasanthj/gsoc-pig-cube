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
 * TODO write doc TODO implement algebraic interface
 */

public class PostProcess extends EvalFunc<Tuple> {

    private TupleFactory tf;
    private BagFactory bf;
    
    public PostProcess() {
	this.tf = TupleFactory.getInstance();
	this.bf = BagFactory.getInstance();
    }
    /**
     * TODO
     */
    public Tuple exec(Tuple in) throws IOException {
	log.info("[CUBE] Input: " + in);
	Tuple keyTuple = (Tuple)in.get(0);

	Tuple key = tf.newTuple(keyTuple.size()-1);
	for(int i = 0; i < keyTuple.size()-1; i++) {
	    key.set(i, keyTuple.get(i));
	}
	in.set(0, key);

	DataBag valueBag = (DataBag)in.get(1);
	int vpField = keyTuple.size()-1;
	
	Iterator<Tuple> iter = valueBag.iterator();
	List<Tuple> resultBag = new ArrayList<Tuple>();
	while(iter.hasNext()) {
	    Tuple tup = iter.next();
	    Tuple newt = tf.newTuple(tup.size()-1);
	    int idx = 0;
	    for(int i = 0; i < tup.size(); i++) {
		if(i != vpField) {
		    newt.set(idx, tup.get(i));
		    idx++;
		}
	    }
	    resultBag.add(newt);
	}
	in.set(1, bf.newDefaultBag(resultBag));
	log.info("[CUBE] Output: " + in);
	return in;
    }
    
    public Type getReturnType() {
	return Tuple.class;
    }
}
