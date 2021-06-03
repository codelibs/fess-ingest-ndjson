/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ingest.ndjson;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class NdjsonIngesterTest extends LastaFluteTestCase {

    private NdjsonIngester ingester;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ingester = new NdjsonIngester();
        ingester.outputDir = File.createTempFile("test", "").getAbsolutePath();
        ingester.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_ds() {
        Map<String, Object> target = new HashMap<>();
        Map<String, String> params = new HashMap<>();
        target.put("aaa", "111");
        ingester.process(target, params);
        assertEquals("111", ingester.queue.poll().get("aaa"));
    }

    public void test_wf() {
        // TODO
        assertTrue(true);
    }
}
