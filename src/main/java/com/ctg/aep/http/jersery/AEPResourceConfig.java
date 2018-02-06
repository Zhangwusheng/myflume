package com.ctg.aep.http.jersery;

import com.ctg.aep.http.jersery.serder.avro.AvroGenericData;
import com.ctg.aep.http.jersery.serder.avro.ReflectionAvroProvider;
import com.ctg.aep.http.jersery.serder.fastjson.FastJsonProvider;
import com.ctg.aep.http.jersery.serder.fastjson.JSONArrayProvider;
import com.ctg.aep.http.jersery.serder.fastjson.JSONObjectProvider;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("/")
public class AEPResourceConfig extends ResourceConfig {
    public AEPResourceConfig(){
        packages("com.ctg.aep.jersery.action");
        register(LoggingFeature.class);
        register(FastJsonProvider.class);
        register(JSONArrayProvider.class);
        register(JSONObjectProvider.class);
        register(ReflectionAvroProvider.class);
        register(AvroGenericData.class);
    }
}
