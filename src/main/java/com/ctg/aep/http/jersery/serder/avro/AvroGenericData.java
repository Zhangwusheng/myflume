package com.ctg.aep.http.jersery.serder.avro;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.util.*;

public class AvroGenericData extends GenericData {
    public static final String AVRO_REFLECT_CLASSES = "/META-INF/avro/avro.reflect.classes";
    private static Logger LOG = LoggerFactory.getLogger(ReflectionAvroProvider.class);
    private static final Map<String, String> schemas = new HashMap<>();
    public static final AvroGenericData INSTANCE = new AvroGenericData();
    static {
        Enumeration<URL> urls;
        try {
            schemas.put("java.lang.Boolean", "{\"namespace\":\"java.lang\", \"name\":\"Boolean\",\"type\":\"boolean\"}");
            schemas.put("java.lang.String", "{\"namespace\":\"java.lang\", \"name\":\"String\",\"type\":\"string\"}");
            schemas.put("java.lang.Integer", "{\"namespace\":\"java.lang\", \"name\":\"Integer\",\"type\":\"int\"}");
            schemas.put("java.lang.Integer", "{\"namespace\":\"java.lang\", \"name\":\"Integer\",\"type\":\"int\"}");
            schemas.put("java.lang.Short", "{\"namespace\":\"java.lang\", \"name\":\"Short\",\"type\":\"int\"}");
            schemas.put("java.lang.Byte", "{\"namespace\":\"java.lang\", \"name\":\"Byte\",\"type\":\"int\"}");
            schemas.put("java.lang.Long", "{\"namespace\":\"java.lang\", \"name\":\"Long\",\"type\":\"long\"}");
            schemas.put("java.lang.Float", "{\"namespace\":\"java.lang\", \"name\":\"Float\",\"type\":\"float\"}");
            schemas.put("java.lang.Double", "{\"namespace\":\"java.lang\", \"name\":\"Double\",\"type\":\"double\"}");
            urls = ReflectionAvroProvider.class.getClassLoader().getResources(AVRO_REFLECT_CLASSES);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                readPackages(url, schemas);
            }
        } catch (IOException e) {
            LOG.error("Load avro reflect packages faild :" + AVRO_REFLECT_CLASSES, e);
        }
    }

    private static void readPackages(URL url, Map<String, String> schemas) throws IOException {
        Properties p = new Properties();
        try (InputStream in = url.openStream()) {
            p.load(in);
        }
        for (String key : p.stringPropertyNames()) {
            schemas.put(key, p.getProperty(key));
        }
    }

    public static AvroGenericData get() {
        return INSTANCE;
    }

    protected boolean isArrayObject(Object datum) {
        Class<?> cls = datum.getClass();
        return cls.isArray();
    }

    @Override
    public Schema induce(Object datum) {
        if (datum == null) {
            return Schema.create(Type.NULL);
        } else if (isArrayObject(datum)) {
            Class<?> cls = datum.getClass();
            Class<?> type = cls.getComponentType();
            Schema elementType = induce(type);
            if (elementType == null) {
                throw new AvroTypeException("Empty array: " + datum);
            }
            return Schema.createArray(elementType);
        } else if (isRecord(datum)) {
            return getRecordSchema(datum);
        } else if (isArray(datum)) {
            Schema elementType = null;
            for (Object element : (Collection<?>) datum) {
                if (elementType == null) {
                    elementType = induce(element);
                } else if (!elementType.equals(induce(element))) {
                    throw new AvroTypeException("No mixed type arrays.");
                }
            }
            if (elementType == null) {
                throw new AvroTypeException("Empty array: " + datum);
            }
            return Schema.createArray(elementType);

        } else if (isMap(datum)) {
            @SuppressWarnings(value = "unchecked")
            Map<Object, Object> map = (Map<Object, Object>) datum;
            Schema value = null;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                if (value == null) {
                    value = induce(entry.getValue());
                } else if (!value.equals(induce(entry.getValue()))) {
                    throw new AvroTypeException("No mixed type map values.");
                }
            }
            if (value == null) {
                throw new AvroTypeException("Empty map: " + datum);
            }
            return Schema.createMap(value);
        } else if (datum instanceof GenericFixed) {
            return Schema.createFixed(null, null, null, ((GenericFixed) datum).bytes().length);
        } else if (isString(datum))
            return Schema.create(Type.STRING);
        else if (isBytes(datum))
            return Schema.create(Type.BYTES);
        else if (isInteger(datum))
            return Schema.create(Type.INT);
        else if (isLong(datum))
            return Schema.create(Type.LONG);
        else if (isFloat(datum))
            return Schema.create(Type.FLOAT);
        else if (isDouble(datum))
            return Schema.create(Type.DOUBLE);
        else if (isBoolean(datum))
            return Schema.create(Type.BOOLEAN);
        else if (schemas.containsKey(datum.getClass().getName())) {
            return new Schema.Parser().parse(schemas.get(datum.getClass().getName()));
        }
        throw new AvroTypeException("Can't create schema for: " + datum);
    }

    public boolean accept(Class<?> type, java.lang.reflect.Type genericType) {
        if (Collection.class.isAssignableFrom(type)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) genericType;
                java.lang.reflect.Type at = pt.getActualTypeArguments()[0];
                if (at instanceof Class) {
                    return accept((Class<?>) at, at);
                }
                return true;
            }
            return false;
        } else if (type.isArray()) {
            Class<?> et = type.getComponentType();
            return accept(et, et);
        } else if (type.isPrimitive()) {
            return true;
        } else if (Map.class.isAssignableFrom(type)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) genericType;
                java.lang.reflect.Type at = pt.getActualTypeArguments()[1];
                if (at instanceof Class) {
                    return accept((Class<?>) at, at);
                }
                return true;
            }
            return false;
        } else if (IndexedRecord.class.isAssignableFrom(type)) {
            return true;
        } else if (GenericFixed.class.isAssignableFrom(type)) {
            return true;
        } else if (schemas.containsKey(type.getName())) {
            return true;
        }
        return false;
    }
}
