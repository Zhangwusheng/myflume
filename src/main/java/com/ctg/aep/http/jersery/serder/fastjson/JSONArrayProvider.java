package com.ctg.aep.http.jersery.serder.fastjson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSONWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
@Consumes({ MediaType.APPLICATION_JSON, "text/json" })
@Produces({ MediaType.APPLICATION_JSON, "text/json" })
public class JSONArrayProvider extends AbstractJsonProvider<JSONArray> {

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return isJsonType(mediaType) && JSONArray.class == type;
    }

    @Override
    public JSONArray readFrom(Class<JSONArray> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        try (JSONReader reader = new JSONReader(new BufferedReader(new InputStreamReader(entityStream)))) {
            return reader.readObject(type);
        }
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return isJsonType(mediaType) && type == JSONArray.class;
    }

    @Override
    public void writeTo(JSONArray t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        if (t == null) {
            entityStream.close();
            return;
        }
        try (JSONWriter writer = new JSONWriter(new BufferedWriter(new OutputStreamWriter(entityStream)))) {
            writer.writeObject(t);
        }
    }

}
