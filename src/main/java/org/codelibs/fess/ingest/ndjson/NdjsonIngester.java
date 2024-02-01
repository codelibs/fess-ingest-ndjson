/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.codelibs.core.exception.IORuntimeException;
import org.codelibs.core.io.CloseableUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.lang.ThreadUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.crawler.Constants;
import org.codelibs.fess.crawler.entity.AccessResult;
import org.codelibs.fess.crawler.entity.AccessResultData;
import org.codelibs.fess.crawler.entity.ResponseData;
import org.codelibs.fess.crawler.entity.ResultData;
import org.codelibs.fess.crawler.transformer.Transformer;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.ingest.Ingester;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

public class NdjsonIngester extends Ingester {
    private static final Logger logger = LoggerFactory.getLogger(NdjsonIngester.class);
    private static final String NONE = "none";
    protected String outputDir;
    protected String filePrefix;
    protected ObjectMapper objectMapper = new ObjectMapper();
    protected BlockingQueue<Map<String, Object>> queue = null;
    protected Set<String> filterKeySet = null;
    protected Thread writerThread = null;
    protected boolean running;
    protected Writer currentWriter;

    @PostConstruct
    public void init() {
        outputDir = ComponentUtil.getFessConfig().getSystemProperty("ingest.ndjson.path");
        if (StringUtil.isBlank(outputDir)) {
            outputDir = null;
            return;
        }
        filePrefix = ComponentUtil.getFessConfig().getSystemProperty("ingest.ndjson.prefix", "fess-");
        if (logger.isInfoEnabled()) {
            logger.info("Output Path: {}/{}*.ndjson", outputDir, filePrefix);
        }
        final int maxLines = Integer.parseInt(ComponentUtil.getFessConfig().getSystemProperty("ingest.ndjson.max.lines", "10000"));
        final String capacity = ComponentUtil.getFessConfig().getSystemProperty("ingest.ndjson.capacity");
        queue = new LinkedBlockingQueue<>(StringUtil.isBlank(capacity) ? Integer.MAX_VALUE : Integer.parseInt(capacity));
        final String keyStr = ComponentUtil.getFessConfig().getSystemProperty("ingest.ndjson.filter.keys");
        if (StringUtil.isBlank(keyStr)) {
            filterKeySet = null;
        } else {
            filterKeySet = StreamUtil.split(keyStr, ",")
                    .get(stream -> stream.map(String::trim).filter(StringUtil::isNotEmpty).collect(Collectors.toSet()));
        }

        running = true;
        writerThread = new Thread(() -> {
            try {
                int count = 0;
                newWriter();
                Map<String, Object> target = null;
                while (running || target != null) {
                    target = queue.poll(10, TimeUnit.SECONDS);
                    if (logger.isDebugEnabled()) {
                        logger.debug("processing {}", target);
                    }

                    if (target != null) {
                        if (count >= maxLines) {
                            newWriter();
                            count = 0;
                        }
                        try {
                            final String json = objectMapper.writeValueAsString(target);
                            currentWriter.write(json);
                            currentWriter.write("\n");
                            count++;
                        } catch (final Exception e) {
                            logger.warn("Failed to write {}", target, e);
                        }
                    }

                }
                if (logger.isDebugEnabled()) {
                    logger.debug("finishing writer.");
                }
            } catch (final InterruptedException e) {
                logger.warn("interrupted.", e);
            }
        }, "ingest-ndjson");
        writerThread.start();
    }

    protected void newWriter() {
        if (currentWriter != null) {
            try {
                currentWriter.flush();
                currentWriter.close();
            } catch (final IOException e) {
                logger.warn("Failed to close writer.", e);
            }
        }
        try {
            currentWriter = new BufferedWriter(
                    new FileWriter(new File(outputDir, filePrefix + ComponentUtil.getSystemHelper().getCurrentTimeAsLong() + ".ndjson"),
                            Constants.UTF_8_CHARSET));
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @PreDestroy
    public void destroy() {
        running = false;
        if (outputDir != null) {
            try {
                writerThread.join();
            } catch (final InterruptedException e) {
                logger.warn("interrupted.", e);
            }
            try {
                currentWriter.flush();
            } catch (final IOException e) {
                logger.warn("Failed to flush the writer.", e);
            } finally {
                CloseableUtil.closeQuietly(currentWriter);
            }
        }
    }

    @Override
    public Map<String, Object> process(final Map<String, Object> target, final DataStoreParams params) {
        if (outputDir == null) {
            return target;
        }

        offer(target);
        return target;
    }

    private void offer(final Map<String, Object> target) {
        if (target == null) {
            return;
        }

        final Map<String, Object> map = filterKeySet == null ? target
                : target.entrySet().stream().filter(e -> filterKeySet.contains(e.getKey()))
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        if (logger.isDebugEnabled()) {
            logger.debug("filtered object: {}", map);
        }
        while (!queue.offer(map)) {
            logger.warn("Failed to add {}. retrying...", target);
            ThreadUtil.sleep(100L);
        }
    }

    @Override
    public ResultData process(final ResultData target, final ResponseData responseData) {
        try {
            final AccessResult<?> accessResult = ComponentUtil.getComponent("accessResult");
            accessResult.init(responseData, target);
            final AccessResultData<?> accessResultData = accessResult.getAccessResultData();
            final String transformerName = accessResultData.getTransformerName();
            if (StringUtil.isNotBlank(transformerName) && !NONE.equalsIgnoreCase(transformerName)) {
                final Transformer transformer = ComponentUtil.getComponent(transformerName);
                @SuppressWarnings("unchecked")
                final Map<String, Object> map = (Map<String, Object>) transformer.getData(accessResultData);
                offer(map);
            }
        } catch (final Exception e) {
            logger.warn("Failed to add {}.", target, e);
        }
        return target;
    }
}
