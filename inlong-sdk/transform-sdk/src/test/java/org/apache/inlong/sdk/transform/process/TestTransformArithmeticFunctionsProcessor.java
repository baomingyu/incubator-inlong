/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.SinkInfo;
import org.apache.inlong.sdk.transform.pojo.SourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TestArithmeticFunctionsTransformProcessor
 * description: test the arithmetic functions in transform processor
 */
public class TestTransformArithmeticFunctionsProcessor {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final SourceInfo csvSource;
    private static final SinkInfo kvSink;
    static {
        for (int i = 1; i < 5; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("numeric" + i);
            srcFields.add(field);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", "|", "\\", srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }

    @Test
    public void testPowerFunction() throws Exception {
        String transformSql = "select power(numeric1, numeric2) from source";
        TransformConfig config = new TransformConfig(csvSource, kvSink, transformSql);
        // case1: 2^4
        TransformProcessor processor = new TransformProcessor(config);
        List<String> output1 = processor.transform("2|4|6|8", new HashMap<>());
        Assert.assertTrue(output1.size() == 1);
        Assert.assertEquals(output1.get(0), "result=16.0");
        // case2: 2^(-2)
        List<String> output2 = processor.transform("2|-2|6|8", new HashMap<>());
        Assert.assertTrue(output2.size() == 1);
        Assert.assertEquals(output2.get(0), "result=0.25");
        // case3: 4^(0.5)
        List<String> output3 = processor.transform("4|0.5|6|8", new HashMap<>());
        Assert.assertTrue(output3.size() == 1);
        Assert.assertEquals(output3.get(0), "result=2.0");
    }

    @Test
    public void testAbsFunction() throws Exception {
        String transformSql = "select abs(numeric1) from source";
        TransformConfig config = new TransformConfig(csvSource, kvSink, transformSql);
        // case1: |2|
        TransformProcessor processor = new TransformProcessor(config);
        List<String> output1 = processor.transform("2|4|6|8", new HashMap<>());
        Assert.assertTrue(output1.size() == 1);
        Assert.assertEquals(output1.get(0), "result=2");
        // case2: |-4.25|
        List<String> output2 = processor.transform("-4.25|4|6|8", new HashMap<>());
        Assert.assertTrue(output2.size() == 1);
        Assert.assertEquals(output2.get(0), "result=4.25");
    }

    @Test
    public void testSqrtFunction() throws Exception {
        String transformSql = "select sqrt(numeric1) from source";
        TransformConfig config = new TransformConfig(csvSource, kvSink, transformSql);
        // case1: sqrt(9)
        TransformProcessor processor = new TransformProcessor(config);
        List<String> output1 = processor.transform("9|4|6|8", new HashMap<>());
        Assert.assertTrue(output1.size() == 1);
        Assert.assertEquals(output1.get(0), "result=3.0");
        // case2: sqrt(5)
        List<String> output2 = processor.transform("5|4|6|8", new HashMap<>());
        Assert.assertTrue(output2.size() == 1);
        Assert.assertEquals(output2.get(0), "result=2.23606797749979");
    }

    @Test
    public void testLnFunction() throws Exception {
        String transformSql = "select ln(numeric1) from source";
        TransformConfig config = new TransformConfig(csvSource, kvSink, transformSql);
        // case1: ln(1)
        TransformProcessor processor = new TransformProcessor(config);
        List<String> output1 = processor.transform("1|4|6|8", new HashMap<>());
        Assert.assertTrue(output1.size() == 1);
        Assert.assertEquals(output1.get(0), "result=0.0");
        // case2: ln(10)
        List<String> output2 = processor.transform("10|4|6|8", new HashMap<>());
        Assert.assertTrue(output2.size() == 1);
        Assert.assertEquals(output2.get(0), "result=2.302585092994046");
    }
}