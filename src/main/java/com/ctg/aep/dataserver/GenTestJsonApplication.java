package com.ctg.aep.dataserver;

import com.ctg.aep.data.AEPDataObject;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by zws on 1/27/18.
 */
public class GenTestJsonApplication {

    public GenTestJsonApplication(){}

    public  void generateTestString(){
        AEPDataObject aepDataObject = new AEPDataObject();
        aepDataObject.deviceId = "valueOfCol1";
        aepDataObject.timestamp = System.currentTimeMillis();
        aepDataObject.col3 = "valueOfCol3";
        aepDataObject.col4 = "valueOfCol4";
        aepDataObject.col5 = "valueOfCol5";
        aepDataObject.col6 = "valueOfCol6";
        aepDataObject.col7 = "valueOfCol7";
        aepDataObject.payload = "ReaDataPayLoad".getBytes();
        aepDataObject.tenant = "tenant";
        aepDataObject.tableName = "testtbl";

        ObjectMapper objectMapper = getDefaultObjectMapper();
        String data;
        try {
            data = objectMapper.writeValueAsString(aepDataObject);
            System.out.println(data);

            AEPDataObject aepDataObject2 = objectMapper.readValue (data, AEPDataObject.class );
            System.out.println(aepDataObject2);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    public ObjectMapper getDefaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        //设置将对象转换成JSON字符串时候:包含的属性不能为空或"";
        //Include.Include.ALWAYS 默认
        //Include.NON_DEFAULT 属性为默认值不序列化
        //Include.NON_EMPTY 属性为空（""）  或者为 NULL 都不序列化
        //Include.NON_NULL 属性为NULL 不序列化
        mapper.setSerializationInclusion( JsonSerialize.Inclusion.NON_EMPTY);

        //设置将MAP转换为JSON时候只转换值不等于NULL的
        mapper.configure( SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-ddHH:mm:ss"));
//     mapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);

        //设置有属性不能映射成PO时不报错
        mapper.disable( DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
//     mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,false);  上一条也可以如此设置；

        return mapper;
    }


    public static void main(String[] args) {
        GenTestJsonApplication testApplication = new GenTestJsonApplication();
        testApplication.generateTestString();
    }
}
