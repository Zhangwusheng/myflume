package com.ctg.aep.data;

import java.util.Arrays;

/**
 * Created by zhangwusheng on 18/1/26.
 */
public class AEPDataObject {
    
    public String col1;
    public String col2;
    public String col3;
    public String col4;
    public String col5;
    public String col6;
    public String col7;
    public byte[] payload;
    public String tenant;
    public String tableName;


    @Override
    public String toString() {
        return "AEPDataObject{" +
                "col1='" + col1 + '\'' +
                ", col2='" + col2 + '\'' +
                ", col3='" + col3 + '\'' +
                ", col4='" + col4 + '\'' +
                ", col5='" + col5 + '\'' +
                ", col6='" + col6 + '\'' +
                ", col7='" + col7 + '\'' +
                ", payload=" + Arrays.toString(payload) +
                ", tenant='" + tenant + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
