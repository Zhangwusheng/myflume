package com.ctg.aep.data;

public class RedisQueryResult {
    private String code;
    private String value;

    @Override
    public String toString() {
        return "RedisQueryResult{" +
                "code='" + code + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCode() {

        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
