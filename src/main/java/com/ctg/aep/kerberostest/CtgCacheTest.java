package com.ctg.aep.kerberostest;

import com.ctg.itrdc.cache.common.exception.CacheConfigException;
import com.ctg.itrdc.cache.core.CacheService;
import org.apache.flume.FlumeException;

import java.util.*;

public class CtgCacheTest {

    CacheService cacheService;
    String  groupId = "group.AEP.storage";
    long timeout = 5000;
    String user = "AEP";
    String passwd = "Redis123";
    String[] groups = {groupId};

    public void init(){

        try {
            cacheService = new CacheService(groups,timeout,user,passwd);
//            cacheService.close();
        } catch (CacheConfigException e) {
            throw new FlumeException(e);
        }
    }

    public void testWriteAndRead(){
        for(int i=0;i<10000;i++) {
            String code = cacheService.set(groupId, "itemKey"+i, "value"+i);

            System.out.println("code=" + code);
            String value = cacheService.get(groupId, "itemKey"+i);
            System.out.println("value=" + value);
        }
    }

    public static void main(String[] args) {

    }
}
