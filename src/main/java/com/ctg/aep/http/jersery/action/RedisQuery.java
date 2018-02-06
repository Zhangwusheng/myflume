package com.ctg.aep.http.jersery.action;

import com.alibaba.fastjson.JSONObject;
import com.ctg.aep.data.RedisQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;


@Path("/api/v1/getDeviceStatus")
public class RedisQuery {
    private static Logger logger = LoggerFactory.getLogger(RedisQuery.class);
    @POST
    @Path("/Example")
    @Produces("application/json;charset=UTF-8")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_HTML})
    public String Example(@Context HttpServletRequest request){
        logger.info("Example Called");
        String ret="";
        try {
            ServletInputStream inputStream=request.getInputStream();
            StringBuilder builder=new StringBuilder();
            byte[] buff=new byte[1024];
            int len=-1;
            while((len = inputStream.read(buff))!= -1){
                builder.append(new String(buff,0,len,"UTF-8"));
            }
            //参数json
            String parameterStr = builder.toString();
            //打印参数
            System.out.println(parameterStr);
            ret="Success";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }


    @POST
    @Path("/Example2")
    @Produces("application/json;charset=UTF-8")
    @Consumes({MediaType.APPLICATION_JSON})
    public RedisQueryResult Example2(JSONObject jsonObject){
        logger.info("Example2 Called");
        System.out.println(jsonObject);

        RedisQueryResult redisQueryResult = new RedisQueryResult();
        redisQueryResult.setCode("200");
        redisQueryResult.setValue("Value-200");
        return redisQueryResult;
    }
}
