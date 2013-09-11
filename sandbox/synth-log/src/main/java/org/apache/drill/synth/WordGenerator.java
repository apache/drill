/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.synth;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Emulates an infinite list of words, a prefix of which are taken from lists of plausible words.  The first words
 * are taken from a resource that has frequencies in it.  These frequencies can be used to initialize term
 * generators to a common language.  The next batch of words are taken from a long list of words with no frequencies.
 * After that, words are coined by using an integer count.
 */
public class WordGenerator {
    private final Logger log = LoggerFactory.getLogger(WordGenerator.class);

    private BufferedReader wordReader;
    private final List<String> words = Lists.newArrayList();
    private final Map<String, Integer> baseWeights = Maps.newLinkedHashMap();

    public WordGenerator(String seed, String others) {
        // read the common words
        if (seed != null) {
            try {
                Resources.readLines(Resources.getResource(seed), Charsets.UTF_8,
                        new LineProcessor<Object>() {
                            private boolean header = true;
                            private final Splitter onTabs = Splitter.on("\t");

                            public boolean processLine(String s) throws IOException {
                                if (!s.startsWith("#")) {
                                    if (!header) {
                                        Iterator<String> fields = onTabs.split(s).iterator();
                                        fields.next();
                                        String word = fields.next();
                                        words.add(word);
                                        int count = (int) Math.rint(Double.parseDouble(fields.next()));
                                        baseWeights.put(word, count);
                                    } else {
                                        header = false;
                                    }
                                }
                                return true;
                            }

                            public Object getResult() {
                                return null;
                            }
                        });
            } catch (IOException e) {
                log.error("Can't read resource \"{}\", will continue without realistic words", seed);
            }
        }

        try {
          wordReader = new BufferedReader(Resources.newReaderSupplier(Resources.getResource(others), Charsets.UTF_8).getInput());
        } catch (IOException e) {
            log.error("Can't read resource \"{}\", will continue without realistic words", others);
            wordReader = null;
        }

    }

    public String getString(int n) {
        if (n >= words.size()) {
            synchronized (this) {
                while (n >= words.size()) {
                    try {
                        String w = wordReader.readLine();
                        if (w != null) {
                            words.add(w);
                        } else {
                            words.add("w-" + n);
                        }
                    } catch (IOException e) {
                        log.error("Error reading other words resource", e);
                        words.add("w-" + n);
                    }
                }
            }
        }
        return words.get(n);
    }

    public Map<String, Integer> getBaseWeights() {
        return baseWeights;
    }

    public int size() {
        return words.size();
    }
}
