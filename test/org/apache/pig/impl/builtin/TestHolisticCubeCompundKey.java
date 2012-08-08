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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.HolisticCubeCompoundKey;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class TestHolisticCubeCompundKey {

    private static TupleFactory TF = TupleFactory.getInstance();


    @Test
    public void testHolisticCubeUDF() throws IOException {
        Tuple t = TF.newTuple(Lists.newArrayList("a", "b", "c"));
        Tuple t1 = TF.newTuple(Lists.newArrayList("a", "b", null));
        Tuple t2 = TF.newTuple(Lists.newArrayList("a", null, null));
        Tuple t3 = TF.newTuple(Lists.newArrayList(null, null, null));
        Tuple r = TF.newTuple(Lists.newArrayList("region", "state", "city"));
        Tuple r1 = TF.newTuple(Lists.newArrayList("region", "state", null));
        Tuple r2 = TF.newTuple(Lists.newArrayList("region", null, null));
        Tuple r3 = TF.newTuple(Lists.newArrayList(null, null, null));
        Set<Tuple> expected = ImmutableSet.of(
                TF.newTuple(Lists.newArrayList(r, t, (long)1)),
                TF.newTuple(Lists.newArrayList(r1, t1, (long)1)),
                TF.newTuple(Lists.newArrayList(r2, t2, (long)1)),
                TF.newTuple(Lists.newArrayList(r3, t3, (long)1))
        );

        String[] regions = {"region,state,city", "region,state,", "region,,", ",,"};
        HolisticCubeCompoundKey hcd = new HolisticCubeCompoundKey(regions);
        DataBag bag = hcd.exec(t);
        assertEquals(bag.size(), expected.size());

        for (Tuple tup : bag) {
            assertTrue(expected.contains(tup));
        }
    }
}
