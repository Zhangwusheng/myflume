package com.ctg.aep.dataserver;

import com.ctg.itrdc.cache.common.exception.CacheConfigException;
import com.ctg.itrdc.cache.core.CacheService;
import org.apache.flume.FlumeException;

public class CtgCacheTest {


    public static void main(String[] args) {

        CacheService cacheService;
        String  groupId = "group.AEP.storage";
        long timeout = 5000;
        String user = "AEP";
        String passwd = "Redis123";
        String[] groups = {groupId};
        try {
            cacheService = new CacheService(groups,timeout,user,passwd);

            for(int i=0;i<10000;i++) {
                String code = cacheService.set(groupId, "itemKey"+i, "value"+i);

                System.out.println("code=" + code);
                String value = cacheService.get(groupId, "itemKey"+i);
                System.out.println("value=" + value);
            }

//            cacheService.close();
        } catch (CacheConfigException e) {
            throw new FlumeException(e);
        }

    }
}
