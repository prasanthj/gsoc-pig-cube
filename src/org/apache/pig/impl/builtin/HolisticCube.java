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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * TODO write doc
 */

public class HolisticCube extends EvalFunc<DataBag> {

    private TupleFactory tf;
    private BagFactory bf;
    private Log log = LogFactory.getLog(getClass());
    private List<Tuple> cl;
    private boolean isLatticeRead;
    private String annotatedLatticeLocation;
    private HashMap<Tuple, Pair<Integer, Integer>> aLattice;

    public HolisticCube() {
	this(null);
    }

    public HolisticCube(String[] args) {
	this.tf = TupleFactory.getInstance();
	this.bf = BagFactory.getInstance();
	this.cl = new ArrayList<Tuple>();
	stringArrToTupleList(this.cl, args);
	this.isLatticeRead = false;
	this.annotatedLatticeLocation = args[args.length - 1];
	this.aLattice = new HashMap<Tuple, Pair<Integer, Integer>>();
	log.info("[CUBE] lattice - " + cl);
    }

    private void stringArrToTupleList(List<Tuple> cl, String[] args) {

	// region labels are csv strings. if trailing values of region label
	// are null/empty then split function ignores those values. specify some
	// integer value greater than the number of output tokens makes sure
	// that
	// all null values in region label are assigned an empty string. first
	// value
	// which is the most detailed level in the lattice doesn't have null
	// values
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

    public DataBag exec(Tuple in) throws IOException {
	log.info("[CUBE] Input - " + in);
	if (in == null || in.size() == 0) {
	    return null;
	}

	if (isLatticeRead == false) {
	    ReadToEndLoader loader;
	    try {

		Configuration udfconf = UDFContext.getUDFContext().getJobConf();
		annotatedLatticeLocation = udfconf.get("pig.annotatedLatticeFile", "");

		if (annotatedLatticeLocation.length() == 0) {
		    throw new RuntimeException(this.getClass().getSimpleName() + " used but no annotated lattice file found");
		}
		log.info("[CUBE] " + annotatedLatticeLocation);
		// use local file system to get the quantilesFile
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
		    log.info("[CUBE] Tuple: " + t.toString());
		    int paritionFactor = Integer.valueOf(t.get(1).toString());
		    Pair<Integer, Integer> ppf = new Pair<Integer, Integer>(paritionFactor, paritionFactor);
		    aLattice.put((Tuple) t.get(0), ppf);
		}
		// after reading annotated lattice we do not need the normal
		// lattice anymore
		isLatticeRead = true;
		cl = null;
	    } catch (IOException e) {
		throw new ExecException("Failed to open file '" + annotatedLatticeLocation + "'; error = " + e.getMessage());
	    }
	}
	Utils.convertNullToUnknown(in);
	List<Tuple> groups = getAllCubeCombinations(in);

	log.info("[CUBE] Output - " + bf.newDefaultBag(groups));
	return bf.newDefaultBag(groups);
    }

    private List<Tuple> getAllCubeCombinations(Tuple in) throws IOException {
	List<Tuple> result = new ArrayList<Tuple>(in.size());
	if (cl != null && cl.size() != 0) {
	    for (Tuple region : cl) {
		Tuple newt = tf.newTuple(in.getAll());
		if (region.size() != in.size()) {
		    throw new RuntimeException("Number of fields in tuple should be equal to the number of fields in region tuple.");
		}
		for (int i = 0; i < region.size(); i++) {
		    if (region.get(i) == null) {
			newt.set(i, null);
		    }
		}
		result.add(newt);
	    }
	} else {
	    // This means the lattice has been annotated
	    for (Map.Entry<Tuple, Pair<Integer, Integer>> entry : aLattice.entrySet()) {
		Tuple region = entry.getKey();
		Pair<Integer, Integer> ppf = entry.getValue();
		Tuple newt = tf.newTuple(in.getAll());
		if (region.size() != in.size()) {
		    throw new RuntimeException("Number of fields in tuple should be equal to the number of fields in region tuple.");
		}
		for (int i = 0; i < region.size(); i++) {
		    if (region.get(i) == null) {
			newt.set(i, null);
		    }
		}

		if (ppf.first > 1) {
		    if (ppf.second == 0) {
			ppf.second = ppf.first;
		    }
		    newt.append(ppf.second);
		    ppf.second = ppf.second - 1;

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
