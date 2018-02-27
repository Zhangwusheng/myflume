package com.ctg.aep.kerberostest;

public class HbaseKeyTabLogin extends HbaseComponentBase {

    @Override
    protected void doInit() throws Exception{
        login();
        resetJassFileWithKeyTab();
    }

    @Override
    public String getName() {
        return "HbaseKeyTabLogin";
    }
}
