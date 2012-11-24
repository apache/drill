package org.apache.drill.common.logical.data;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class OperatorListDeserializer {
  static final Logger logger = LoggerFactory.getLogger(OperatorListDeserializer.class);

  public LogicalOperator deserializeSequence(JsonParser jp, DeserializationContext ctxt) throws IOException,
      JsonProcessingException {

    final JavaType tp = ctxt.constructType(LogicalOperator.class);
    final JsonDeserializer<Object> d = ctxt.findRootValueDeserializer(tp);
    final JsonLocation start = jp.getCurrentLocation();
    
    JsonToken t = jp.getCurrentToken();
    LogicalOperator parent = null;
    LogicalOperator first = null;
    LogicalOperator prev = null;
    
    while (true) {
      String token = jp.getText();
      logger.debug("Working on token '{}'", token);
      jp.nextToken();
      switch (token) { // switch on field names.
      case "input":

        parent = (LogicalOperator) d.deserialize(jp, ctxt);
        break;
      case "do":
        if (!jp.isExpectedStartArrayToken())
          throwE(
              jp,
              "The do parameter of sequence should be an array of SimpleOperators.  Expected a JsonToken.START_ARRAY token but received a "
                  + t.name() + "token.");

        int pos = 0;
        while ((t = jp.nextToken()) != JsonToken.END_ARRAY) {
          logger.debug("Reading sequence child {}.", pos);
          JsonLocation l = jp.getCurrentLocation(); // get current location
                                                    // first so we can correctly
                                                    // reference the start of
                                                    // the object in the case
                                                    // that the type is wrong.
          LogicalOperator o = jp.readValueAs(LogicalOperator.class);

          if (pos == 0) {
            if (!(o instanceof SingleInputOperator) && !(o instanceof ZeroInputOperator))
              throwE(
                  l,
                  "The first operator in a sequence must be either a ZeroInput or SingleInput operator.  The provided first operator was not. It was of type "
                      + o.getClass().getName());
            first = o;
          } else {
            if (!(o instanceof SingleInputOperator))
              throwE(l, "All operators after the first must be single input operators.  The operator at position "
                  + pos + " was not. It was of type " + o.getClass().getName());
            SingleInputOperator now = (SingleInputOperator) o;
            now.setInput(prev);
          }
          prev = o;

          pos++;
        }
        break;
      default:
        throwE(jp, "Unknown field name provided for Sequence: " + jp.getText());
      }

      t = jp.nextToken();
      if (t == JsonToken.END_OBJECT) {
        break;
      }
    }

    if (first == null)
      throwE(start, "A sequence must include at least one operator.");
    if ((parent == null && first instanceof SingleInputOperator)
        || (parent != null && first instanceof ZeroInputOperator))
      throwE(start,
          "A sequence must either start with a ZeroInputOperator or have a provided input. It cannot have both or neither.");

    if (parent != null && first instanceof SingleInputOperator) {
      ((SingleInputOperator) first).setInput(parent);
    }

    return first;
  }

  private static void throwE(JsonLocation l, String e) throws JsonParseException {
    throw new JsonParseException(e, l);
  }

  private static void throwE(JsonParser jp, String e) throws JsonParseException {
    throw new JsonParseException(e, jp.getCurrentLocation());
  }
}
