package com.ctg.aep.kerberostest;

public class KafkaCacheJaas extends KafkaComponentBase {

    @Override
    protected void doInit() throws Exception {
        resetJassFileWithCache();
    }

    @Override
    public String getName() {
        return "KafkaCacheJaas";
    }

}
