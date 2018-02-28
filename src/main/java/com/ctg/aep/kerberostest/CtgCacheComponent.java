package com.ctg.aep.kerberostest;

import com.ctg.itrdc.cache.core.CacheService;

public class CtgCacheComponent extends BaseComponent {

    private CacheService cacheService;
    private String  groupId = "group.AEP.storage";
    private long timeout = 5000;
    private String user = "AEP";
    private String passwd = "Redis123";
    private String[] groups = {groupId};

    @Override
    public void initialize() throws Exception{
        System.clearProperty(JassKey);
        cacheService = new CacheService(groups, timeout, user, passwd);
    }

    @Override
    public void doWork() throws Exception {

        System.out.println("*****************CtgCacheComponent.work******************");

        for (int i = 0; i < 100; i++) {
            String code = cacheService.set(groupId, "itemKey" + i, "value" + i);

            System.out.println("code=" + code);
            String value = cacheService.get(groupId, "itemKey" + i);
            System.out.println("value=" + value);
        }

        System.out.println("*****************CtgCacheComponent.work******************");
    }

    @Override
    public String getName() {
        return "CtgCacheComponent";
    }
}
