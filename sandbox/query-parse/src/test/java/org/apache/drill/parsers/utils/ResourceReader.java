package org.apache.drill.parsers.utils;

import com.google.common.base.Charsets;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;

/**
 * User: Antonio Garrote
 * Date: 24/10/2012
 * Time: 09:01
 */
public class ResourceReader {

    public static String read(String resource) throws IOException {
        InputSupplier<InputStreamReader> in = Resources.newReaderSupplier(Resources.getResource(resource), Charsets.UTF_8);
        StringWriter writer = new StringWriter();
        IOUtils.copy(in.getInput(),writer);
        return writer.toString();
    }
}
