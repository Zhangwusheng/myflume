package com.ctg.aep.kerberostest;

public class KafkaKeyTab extends KafkaComponentBase {

    @Override
    protected void doInit() throws Exception {
        resetJassFileWithKeyTab();
    }


    @Override
    public String getName() {
        return "KafkaKeyTab";
    }
}
