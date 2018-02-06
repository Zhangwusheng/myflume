package com.ctg.aep.http.jersery.serder.fastjson;

//import com.sun.jersey.core.provider.AbstractMessageReaderWriterProvider;

import org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider;

import javax.ws.rs.core.MediaType;
import java.util.HashSet;

public abstract class AbstractJsonProvider<T> extends AbstractMessageReaderWriterProvider<T> {

    protected static final HashSet<ClassKey> _untouchables = new HashSet<ClassKey>();

    public AbstractJsonProvider() {
        super();
    }

    protected boolean isJsonType(MediaType mediaType) {
        /* As suggested by Stephen D, there are 2 ways to check: either
         * being as inclusive as possible (if subtype is "json"), or
         * exclusive (major type "application", minor type "json").
         * Let's start with inclusive one, hard to know which major
         * types we should cover aside from "application".
         */
        if (mediaType != null) {
            // Ok: there are also "xxx+json" subtypes, which count as well
            String subtype = mediaType.getSubtype();
            return "json".equalsIgnoreCase(subtype) || subtype.endsWith("+json");
        }
        /* Not sure if this can happen; but it seems reasonable
         * that we can at least produce json without media type?
         */
        return true;
    }

}