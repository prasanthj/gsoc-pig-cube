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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestPostProcessCube {

    private static TupleFactory TF = TupleFactory.getInstance();
    private static BagFactory BF = BagFactory.getInstance();

    @Test
    public void testHolisticCubeUDF() throws IOException {
        Tuple t = TF.newTuple(Lists.newArrayList("a", "b", 4));
        Tuple inTup = TF.newTuple();
        inTup.append(t);

        DataBag inBag = BF.newDefaultBag();
        inBag.add(TF.newTuple(Lists.newArrayList("a", "b", 4)));
        inBag.add(TF.newTuple(Lists.newArrayList("a", null, 4)));
        inTup.append(inBag);

        Tuple o = TF.newTuple(Lists.newArrayList("a", "b"));
        Tuple outTup = TF.newTuple();
        outTup.append(o);

        DataBag outBag = BF.newDefaultBag();
        outBag.add(TF.newTuple(Lists.newArrayList("a", "b")));
        outBag.add(TF.newTuple(Lists.newArrayList("a", null)));
        outTup.append(outBag);

        PostProcessCube ppc = new PostProcessCube();
        Tuple result = ppc.exec(inTup);

        assertTrue("Expected: " + outTup + " Got: " + result, outTup.equals(result));
    }
}
