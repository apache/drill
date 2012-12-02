package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;

/**
 * Reads CSV data.  Note that quotes are not handled correctly just yet.
 */
@JsonTypeName("scan")
public class Scan extends Consumer {
    private static Splitter onComma = Splitter.on(",").trimResults(CharMatcher.anyOf(" \"'"));

    private Consumer out;
    private File file;
    private String resource;
    private final InputSupplier<InputStreamReader> input;

    public Scan(JsonObject spec) {
        JsonElement filename = spec.get("file");
        if (filename != null) {
            file = new File(filename.getAsString());
            input = Files.newReaderSupplier(file, Charsets.UTF_8);
        } else {
            resource = spec.get("resource").getAsString();
            input = Resources.newReaderSupplier(Resources.getResource(resource), Charsets.UTF_8);
        }
    }

    @Override
    public void connect(Consumer out) {
        this.out = out;
    }

    public void start() throws IOException {
        CharStreams.readLines(input, new LineProcessor<Object>() {
            boolean first = true;
            Iterable<String> header;

            @Override
            public boolean processLine(String s) throws IOException {
                if (first) {
                    header = onComma.split(s);
                    first = false;
                } else {
                    try {
                        out.push(new MapTuple(header, onComma.split(s)));
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            @Override
            public Object getResult() {
                return null;
            }
        });
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException {
        throw new UnsupportedOperationException("Scanner can't process tuples");
    }

    public File getFile() {
        return file;
    }

    public String getResource() {
        return resource;
    }
}
