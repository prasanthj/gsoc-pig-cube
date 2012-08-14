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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * This UDF is used by cube operator for performing actual holistic cubing.
 * It reads the annotated lattice from local cache and based on the partition
 * factor for each region it appends algebraicAttribute%partitionfactor value
 * to the input tuple. algebraicAttribute%partitionfactor guarantees that the
 * splitting is algebraic.
 * For example: for holistic measure count(distinct users) 
 * splitting along the algebraic attribute (userid) guarantees that the same the
 * same userid cannot be sent to two different reducers.
 * For example: 
 * Input tuple: (midwest,OH,1007)
 * Annotated Lattice: {((,),5), ((region,),2), ((,state),4), ((region,state),0)}
 * Output: {(,,2),(region,,1),(,state,3),(region,state,0)}
 * This UDF expects the algebraic attribute as last argument of the input tuple.
 * It assumes that last field in the input tuple is algebraic attribute. 
 */

public class HolisticCube extends EvalFunc<DataBag> {

    private TupleFactory tf;
    private BagFactory bf;
    private List<Tuple> cl;
    private boolean isLatticeRead;
    private String annotatedLatticeLocation;
    private HashMap<Tuple, Integer> aLattice;

    // for debugging
    boolean printOutputOnce = false;
    boolean printInputOnce = false;

    public HolisticCube(String[] args) {
	this.tf = TupleFactory.getInstance();
	this.bf = BagFactory.getInstance();
	this.cl = new ArrayList<Tuple>();
	stringArrToTupleList(this.cl, args);
	this.isLatticeRead = false;
	this.aLattice = new HashMap<Tuple, Integer>();
	log.info("[CUBE] lattice - " + cl);
    }

    private void stringArrToTupleList(List<Tuple> cl, String[] args) {

	// region labels are csv strings. if trailing values of region label
	// are null/empty then split function ignores those values. specify some
	// integer value greater than the number of output tokens makes sure that
	// all null values in region label are assigned an empty string. first
	// value which is the most detailed level in the lattice doesn't have null values
	// so assigning the length of first value as max index
	int maxIdx = args[0].length();

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
     * @param in - input tuple with last field as algebraic attribute
     * @return bag with all combinations of groups with last field
     * as algebraicAttribute%partitionFactor to guarantee that algebraic
     * attribute with same value does NOT go to different reducers
     */
    public DataBag exec(Tuple in) throws IOException {
	if (printInputOnce == false) {
	    log.info("[CUBE] Input - " + in);
	    printInputOnce = true;
	}

	if (in == null || in.size() == 0) {
	    return null;
	}

	if (isLatticeRead == false) {
	    readAnnotatedLattice();
	}
	Utils.convertNullToUnknown(in);
	List<Tuple> groups = getAllCubeCombinations(in);

	if (printOutputOnce == false) {
	    log.info("[CUBE] Output - " + bf.newDefaultBag(groups));
	    printOutputOnce = true;
	}

	return bf.newDefaultBag(groups);
    }

    private void readAnnotatedLattice() throws IOException {
	ReadToEndLoader loader;
	try {
	    Configuration udfconf = UDFContext.getUDFContext().getJobConf();
	    annotatedLatticeLocation = udfconf.get("pig.annotatedLatticeFile", "");

	    if (annotatedLatticeLocation.length() == 0) {
		throw new RuntimeException(this.getClass().getSimpleName()
		        + " used but no annotated lattice file found");
	    }

	    Configuration conf = new Configuration();

	    // Hadoop security need this property to be set
	    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
		conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
	    }
	    if (udfconf.get("fs.file.impl") != null)
		conf.set("fs.file.impl", udfconf.get("fs.file.impl"));
	    if (udfconf.get("fs.hdfs.impl") != null)
		conf.set("fs.hdfs.impl", udfconf.get("fs.hdfs.impl"));
	    if (udfconf.getBoolean("pig.tmpfilecompression", false)) {
		conf.setBoolean("pig.tmpfilecompression", true);
		if (udfconf.get("pig.tmpfilecompression.codec") != null)
		    conf.set("pig.tmpfilecompression.codec", udfconf.get("pig.tmpfilecompression.codec"));
	    }
	    conf.set(MapRedUtil.FILE_SYSTEM_NAME, "file:///");

	    loader = new ReadToEndLoader(Utils.getTmpFileStorageObject(conf), conf, annotatedLatticeLocation, 0);

	    Tuple t;
	    while ((t = loader.getNext()) != null) {
		log.info("[CUBE] Annotated Region: " + t.toString());
		int paritionFactor = Integer.valueOf(t.get(1).toString());
		aLattice.put((Tuple) t.get(0), paritionFactor);
	    }

	    // after reading annotated lattice we do not need the normal
	    // lattice anymore
	    isLatticeRead = true;
	    cl = null;
	} catch (IOException e) {
	    throw new IOException("Error while processing annotated lattice " + annotatedLatticeLocation, e);
	}
    }

    private List<Tuple> getAllCubeCombinations(Tuple in) throws IOException {
	List<Tuple> result = null;
	if (isLatticeRead == true) {
	    result = new ArrayList<Tuple>(in.size());

	    // This means the lattice has been read and annotated
	    for (Map.Entry<Tuple, Integer> entry : aLattice.entrySet()) {
		Tuple region = entry.getKey();
		int pf = entry.getValue();

		Tuple newt = tf.newTuple(in.getAll());
		// input tuple will have one additional field because
		// the algebraic attribute will also be projected (last field)
		if (region.size() + 1 != in.size()) {
		    throw new RuntimeException(
			    "Number of fields in tuple should be equal to the number of fields in region tuple.");
		}
		for (int i = 0; i < region.size(); i++) {
		    if (region.get(i) == null) {
			newt.set(i, null);
		    }
		}

		// last tuple is the algebraic attribute
		// TODO check if it works correctly for all data types
		// alg attr with same values should NOT go to different reducers
		Object algAttr = newt.get(newt.size() - 1);
		if (pf > 1) {
		    newt.set(newt.size() - 1, algAttr.hashCode() % pf);
		}

		result.add(newt);
	    }
	}
	return result;
    }

    public Type getReturnType() {
	return DataBag.class;
    }
}
