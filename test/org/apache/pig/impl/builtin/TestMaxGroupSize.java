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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.MaxGroupSize;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMaxGroupSize {

    private static TupleFactory TF = TupleFactory.getInstance();
    private static BagFactory BF = BagFactory.getInstance();

    @Test
    public void testHolisticCubeUDF() throws IOException {
	List<Tuple> tupList = new ArrayList<Tuple>();

	Tuple t1 = TF.newTuple();
	t1.append(TF.newTuple(Lists.newArrayList("region", "state")));
	t1.append(TF.newTuple(Lists.newArrayList("midwest", "OH")));
	t1.append((long) 1);

	Tuple t2 = TF.newTuple();
	t2.append(TF.newTuple(Lists.newArrayList("region", "state")));
	t2.append(TF.newTuple(Lists.newArrayList("midwest", "OH")));
	t2.append((long) 1);

	Tuple t3 = TF.newTuple();
	t3.append(TF.newTuple(Lists.newArrayList("region", "state")));
	t3.append(TF.newTuple(Lists.newArrayList("southwest", "CA")));
	t3.append((long) 1);

	Tuple t4 = TF.newTuple();
	t4.append(TF.newTuple(Lists.newArrayList("region", "state")));
	t4.append(TF.newTuple(Lists.newArrayList("southwest", "CA")));
	t4.append((long) 1);

	Tuple t5 = TF.newTuple();
	t5.append(TF.newTuple(Lists.newArrayList("region", "state")));
	t5.append(TF.newTuple(Lists.newArrayList("southwest", "CA")));
	t5.append((long) 1);

	tupList.add(t1);
	tupList.add(t2);
	tupList.add(t3);
	tupList.add(t4);
	tupList.add(t5);

	DataBag bag = BF.newDefaultBag(tupList);
	Tuple in = TF.newTuple();
	in.append(bag);

	Tuple expected = TF.newTuple(Lists.newArrayList("region", "state", (long) 3, (int) 0));
	MaxGroupSize mgs = new MaxGroupSize();
	Tuple result = mgs.exec(in);

	assertEquals("Expected: " + expected + " Got: " + result, expected.equals(result));
    }
}
