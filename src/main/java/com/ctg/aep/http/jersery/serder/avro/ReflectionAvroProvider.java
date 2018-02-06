package com.ctg.aep.http.jersery.serder.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

//import com.sun.jersey.core.provider.AbstractMessageReaderWriterProvider;
//import org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider;


@Provider
@Consumes({ MediaType.APPLICATION_OCTET_STREAM, ReflectionAvroProvider.MEDIA_AVRO })
@Produces({ MediaType.APPLICATION_OCTET_STREAM, ReflectionAvroProvider.MEDIA_AVRO })
public class ReflectionAvroProvider<T> extends AbstractMessageReaderWriterProvider<T> {
    public static final String MEDIA_AVRO = "application/avro";
    public static final MediaType MEDIA_AVRO_TYPE = MediaType.valueOf(MEDIA_AVRO);
    public static final String AVRO_SCHEMA_HEADER = "avro-schema";

    protected boolean isAvroMediaType(MediaType type) {
        return MEDIA_AVRO_TYPE.getType().equalsIgnoreCase(type.getType()) && MEDIA_AVRO_TYPE.getSubtype().equalsIgnoreCase(type.getSubtype());
    }


    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return isAvroMediaType(mediaType) && AvroGenericData.get().accept(type, genericType);
    }

    @Override
    public T readFrom(Class<T> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        return null;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return isAvroMediaType(mediaType) && AvroGenericData.get().accept(type, genericType);
    }

    @Override
    public void writeTo(T t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        if (t == null) {
            httpHeaders.putSingle(AVRO_SCHEMA_HEADER, AvroGenericData.get().induce(null));
            entityStream.close();
        }

        Schema sch = AvroGenericData.get().induce(t);
        httpHeaders.putSingle(AVRO_SCHEMA_HEADER, sch.toString());
        ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(sch);
        Encoder encoder = EncoderFactory.get().binaryEncoder(entityStream, null);
        writer.write(t, encoder);
        encoder.flush();
        entityStream.flush();
        entityStream.close();
    }

}
