package com.ctg.aep.kerberostest;

public class HbaseKeyTabNoLogin extends HbaseComponentBase {

    @Override
    protected void doInit() throws Exception {
        resetJassFileWithKeyTab();
    }

    @Override
    public String getName() {
        return "HbaseKeyTabNoLogin";
    }

}
