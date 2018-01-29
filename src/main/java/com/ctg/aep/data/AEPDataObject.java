package com.ctg.aep.data;

import java.util.Arrays;

/**
 * Created by zhangwusheng on 18/1/26.
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
                '}';
    }
}
