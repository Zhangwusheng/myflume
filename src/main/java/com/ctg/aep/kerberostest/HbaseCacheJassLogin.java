package com.ctg.aep.kerberostest;

public class HbaseCacheJassLogin extends HbaseComponentBase {

    @Override
    protected void doInit() throws Exception{
        login();
        resetJassFileWithCache();
    }

    @Override
    public String getName() {
        return "HbaseCacheJassLogin";
    }
}
