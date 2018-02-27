package com.ctg.aep.kerberostest;

public abstract class BaseComponent implements KerberosComponent {

    String fileNameKeyTab = "/etc/kafka/conf/kafka_odp_jaas_keytab.conf" ;
    String fileNameCache = "/etc/kafka/conf/kafka_odp_jaas_cache.conf";
    String JassKey = "java.security.auth.login.config";

    protected void resetJassFileWithKeyTab(){
        System.setProperty(JassKey,fileNameKeyTab);
    }

    protected void resetJassFileWithCache(){
        System.setProperty(JassKey,fileNameCache);
    }

}
