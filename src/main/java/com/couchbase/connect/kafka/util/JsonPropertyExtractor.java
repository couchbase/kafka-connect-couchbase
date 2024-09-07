package com.couchbase.connect.kafka.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for extracting specific properties from a JSON input stream.
 * This class provides memory-efficient parsing of JSON data without creating
 * objects for the entire JSON structure.
 */
public class JsonPropertyExtractor {

    /**
     * Extracts specified properties from a JSON input stream.
     * 
     * @param inputStream The input stream containing JSON data.
     * @param paths       Array of property paths to extract.
     * @return A map containing the extracted properties and their values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    public static Map<String, Object> extract(InputStream inputStream, String[] paths) throws Exception {
        Set<String> desiredProperties = new HashSet<>(Set.of(paths));
        return extract(inputStream, desiredProperties);
    }

    /**
     * Extracts specified properties from a JSON input stream.
     * 
     * @param inputStream       The input stream containing JSON data.
     * @param desiredProperties Set of property paths to extract.
     * @return A map containing the extracted properties and their values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    public static Map<String, Object> extract(InputStream inputStream, Set<String> desiredProperties) throws Exception {
        JsonFactory factory = new JsonFactory();
        Map<String, Object> result = new HashMap<>();

        try (JsonParser parser = factory.createParser(inputStream)) {
            String currentPath = "";
            processJsonToken(parser, currentPath, desiredProperties, result);
        }

        return result;
    }

    /**
     * Recursively processes JSON tokens, extracting desired properties.
     * 
     * @param parser            The JSON parser.
     * @param currentPath       The current path in the JSON structure.
     * @param desiredProperties Set of property paths to extract.
     * @param result            Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processJsonToken(JsonParser parser, String currentPath, Set<String> desiredProperties,
            Map<String, Object> result) throws Exception {
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.getCurrentName();
            if (fieldName != null) {
                String newPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
                JsonToken token = parser.nextToken();

                if (desiredProperties.contains(newPath)) {
                    if (token == JsonToken.START_OBJECT) {
                        result.put(newPath, parseComplexProperty(parser));
                    } else if (token == JsonToken.START_ARRAY) {
                        result.put(newPath, parseArray(parser));
                    } else {
                        result.put(newPath, getValueByType(parser));
                    }
                } else if (token == JsonToken.START_OBJECT) {
                    processJsonToken(parser, newPath, desiredProperties, result);
                } else if (token == JsonToken.START_ARRAY) {
                    processArray(parser, newPath, desiredProperties, result);
                }
            }
        }
    }

    /**
     * Processes JSON arrays, handling nested objects and arrays.
     * 
     * @param parser            The JSON parser.
     * @param currentPath       The current path in the JSON structure.
     * @param desiredProperties Set of property paths to extract.
     * @param result            Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processArray(JsonParser parser, String currentPath, Set<String> desiredProperties,
            Map<String, Object> result) throws Exception {
        int index = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            String newPath = currentPath + "[" + index + "]";
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                processJsonToken(parser, newPath, desiredProperties, result);
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                processArray(parser, newPath, desiredProperties, result);
            } else if (desiredProperties.contains(newPath)) {
                result.put(newPath, getValueByType(parser));
            }
            index++;
        }
    }

    /**
     * Parses a complex property (object) into a Map.
     * 
     * @param parser The JSON parser.
     * @return A Map representing the complex property.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Map<String, Object> parseComplexProperty(JsonParser parser) throws Exception {
        Map<String, Object> complexProperty = new HashMap<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.getCurrentName();
            parser.nextToken();
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                complexProperty.put(fieldName, parseComplexProperty(parser));
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                complexProperty.put(fieldName, parseArray(parser));
            } else {
                complexProperty.put(fieldName, getValueByType(parser));
            }
        }
        return complexProperty;
    }

    /**
     * Parses a JSON array into an Object array.
     * 
     * @param parser The JSON parser.
     * @return An Object array representing the JSON array.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Object parseArray(JsonParser parser) throws Exception {
        Object[] array = new Object[10]; // Start with a small array size
        int index = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (index >= array.length) {
                // Resize array if necessary
                Object[] newArray = new Object[array.length * 2];
                System.arraycopy(array, 0, newArray, 0, array.length);
                array = newArray;
            }
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                array[index] = parseComplexProperty(parser);
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                array[index] = parseArray(parser);
            } else {
                array[index] = getValueByType(parser);
            }
            index++;
        }
        // Create a new array of exact size and copy elements
        Object[] result = new Object[index];
        System.arraycopy(array, 0, result, 0, index);
        return result;
    }

    /**
     * Returns the appropriate value based on the current token type.
     * 
     * @param parser The JSON parser.
     * @return The value as String, Integer, Double, Boolean, or null.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Object getValueByType(JsonParser parser) throws Exception {
        switch (parser.getCurrentToken()) {
            case VALUE_STRING:
                return parser.getValueAsString();
            case VALUE_NUMBER_INT:
                return parser.getValueAsInt();
            case VALUE_NUMBER_FLOAT:
                return parser.getValueAsDouble();
            case VALUE_TRUE:
                return true;
            case VALUE_FALSE:
                return false;
            case VALUE_NULL:
                return null;
            default:
                return parser.getValueAsString(); // Fallback to string for unknown types
        }
    }
}
