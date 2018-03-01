package com.ctg.aep.data;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by zhangwusheng on 18/1/26.
 * 样例数据：
 String value = "{\"deviceId\":\"29fba1652afc4448b2a5126552fb0cd3\"" +
 ",\"deviceType\":\"\",\"tenantId\":\"300\",\"productId\":\"1\"" +
 ",\"messageType\":\"ad\",\"topic\":\"v1/up/ad\"" +
 ",\"assocAssetId\":\"\",\"timestamp\":1519715852264" +
 ",\"payload\":{\"speed\":120.3,\"duration\":2,\"city\":\"Nanjing\"}" +
 ",\"upPacketSN\":210,\"upDataSN\":785,\"datasetId\":21" +
 ",\"protocolPublicOrPrivate\":\"private\"}";
 */
public class AEPDataObject {

    //该消息在何时被EMQ X接收到
    private long timestamp;
    //消息载荷，key:value的形式
    private String payload;
    //租户ID
    private String tenantId;
    //产品ID
    private String productId;
    //设备ID
    private String deviceId;
    //设备类型
    private String deviceType;
    //消息类型（dm, tr, er, ad, ft）
    private String messageType;
    //设备关联的资产的唯一性ID，可以为空
    private String assocAssetId;

    private String topic;
    private long upPacketSN;
    private long upDataSN;
    private long datasetId;

    public Map<String, Object> getPayloadMap() {
        return payloadMap;
    }

    public void setPayloadMap(Map<String, Object> payloadMap) {
        this.payloadMap = payloadMap;
    }

    private Map<String,Object> payloadMap;
    private String protocolPublicOrPrivate;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getUpPacketSN() {
        return upPacketSN;
    }

    public void setUpPacketSN(long upPacketSN) {
        this.upPacketSN = upPacketSN;
    }

    public long getUpDataSN() {
        return upDataSN;
    }

    public void setUpDataSN(long upDataSN) {
        this.upDataSN = upDataSN;
    }

    public long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(long datasetId) {
        this.datasetId = datasetId;
    }

    public String getProtocolPublicOrPrivate() {
        return protocolPublicOrPrivate;
    }

    public void setProtocolPublicOrPrivate(String protocolPublicOrPrivate) {
        this.protocolPublicOrPrivate = protocolPublicOrPrivate;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getAssocAssetId() {
        return assocAssetId;
    }

    public void setAssocAssetId(String assocAssetId) {
        this.assocAssetId = assocAssetId;
    }


    public boolean initFromMap(Map<String,Object> objectMap, ObjectMapper jsonMapper){
        if( objectMap == null  )
            return false;

        if( !objectMap.containsKey(AEPDataObjectConstants.TENANTID)){
            return false;
        }

        if( !objectMap.containsKey(AEPDataObjectConstants.PRODUCTID)){
            return false;
        }
        if( !objectMap.containsKey(AEPDataObjectConstants.DEVICEID)){
            return false;
        }

        if( !objectMap.containsKey(AEPDataObjectConstants.PAYLOAD)){
            return false;
        }

        if( !objectMap.containsKey(AEPDataObjectConstants.TIMESTAMP)){
            return false;
        }

        setTenantId((String)objectMap.get(AEPDataObjectConstants.TENANTID));
        setProductId((String)objectMap.get(AEPDataObjectConstants.PRODUCTID));
        setTimestamp((Long)objectMap.get(AEPDataObjectConstants.TIMESTAMP));
        setDeviceId((String)objectMap.get(AEPDataObjectConstants.DEVICEID));

        try {
            Object result = objectMap.get(AEPDataObjectConstants.PAYLOAD);
            if( result instanceof Map ){
                setPayloadMap( (Map<String,Object>)objectMap.get(AEPDataObjectConstants.PAYLOAD) );
                setPayload(jsonMapper.writeValueAsString(result));
            }else{
                setPayloadMap(null);
                setPayload((String) objectMap.get(AEPDataObjectConstants.PAYLOAD));
            }
        } catch (Exception e) {
            setPayloadMap(null);
            setPayload("");
        }

        if( objectMap.containsKey(AEPDataObjectConstants.DEVICETYPE)){
            setDeviceType((String)objectMap.get(AEPDataObjectConstants.DEVICETYPE));
        }

        if( objectMap.containsKey(AEPDataObjectConstants.MESSAGETYPE)){
            setMessageType((String)objectMap.get(AEPDataObjectConstants.MESSAGETYPE));
        }

        if( objectMap.containsKey(AEPDataObjectConstants.ASSOCASSETID)){
            setAssocAssetId((String)objectMap.get(AEPDataObjectConstants.ASSOCASSETID));
        }

        if( objectMap.containsKey(AEPDataObjectConstants.TOPIC)){
            setTopic((String)objectMap.get(AEPDataObjectConstants.TOPIC));
        }

        if( objectMap.containsKey(AEPDataObjectConstants.UPDATASN)){
            //这里应该有更好的方案
            try {
                setUpDataSN((Integer) objectMap.get(AEPDataObjectConstants.UPDATASN));
            }catch(ClassCastException e){
                try {
                    setUpDataSN((Long) objectMap.get(AEPDataObjectConstants.UPDATASN));
                }catch(ClassCastException ex){
                    setUpDataSN(-1);
                }
            }
        }

        if( objectMap.containsKey(AEPDataObjectConstants.UPPACKETSN)){
            //这里应该有更好的方案
            try {
                setUpPacketSN((Integer) objectMap.get(AEPDataObjectConstants.UPPACKETSN));
            }catch(ClassCastException e){
                try {
                    setUpPacketSN((Long) objectMap.get(AEPDataObjectConstants.UPPACKETSN));
                }catch(ClassCastException ex){
                    setUpPacketSN(-1);
                }
            }
        }

        if( objectMap.containsKey(AEPDataObjectConstants.DATASETID)){
//            setDatasetId((long)objectMap.get(AEPDataObjectConstants.DATASETID));
            //这里应该有更好的方案
            try {
                setDatasetId((Integer) objectMap.get(AEPDataObjectConstants.DATASETID));
            }catch(ClassCastException e){
                try {
                    setDatasetId((Long) objectMap.get(AEPDataObjectConstants.DATASETID));
                }catch(ClassCastException ex){
                    setDatasetId(-1);
                }
            }

        }

        if( objectMap.containsKey(AEPDataObjectConstants.PROTOCOLPUBLICORPRIVATE)){
            setProtocolPublicOrPrivate((String)objectMap.get(AEPDataObjectConstants.PROTOCOLPUBLICORPRIVATE));
        }
        return true;
    }
    @Override
    public String toString() {
        return "AEPDataObject{" +
                "timestamp=" + timestamp +
                ", payload='" + payload + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", productId='" + productId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", messageType='" + messageType + '\'' +
                ", assocAssetId='" + assocAssetId + '\'' +
                ", topic='" + topic + '\'' +
                ", upPacketSN=" + upPacketSN +
                ", upDataSN=" + upDataSN +
                ", datasetId=" + datasetId +
                ", protocolPublicOrPrivate='" + protocolPublicOrPrivate + '\'' +
                '}';
    }
}
