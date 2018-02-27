package com.ctg.aep.kerberostest;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseCacheJassNoLogin extends HbaseComponentBase {

    @Override
    protected void doInit() throws Exception {
        resetJassFileWithCache();
    }

    @Override
    public String getName() {
        return "HbaseCacheJassNoLogin";
    }
}
