package com.ctg.aep.kerberostest;

public abstract class BaseComponent implements KerberosComponent {

    protected String fileNameKeyTab = "/etc/kafka/conf/kafka_odp_jaas_keytab.conf";
    protected String fileNameCache = "/etc/kafka/conf/kafka_odp_jaas_cache.conf";
    protected String JassKey = "java.security.auth.login.config";

    protected String principal = "odp/danalysis@DFS.COM";
    protected String keyTab = "/etc/security/keytabs/odp.user.keytab";

    protected void resetJassFileWithKeyTab(){
        System.setProperty(JassKey,fileNameKeyTab);
    }

    protected void resetJassFileWithCache(){
        System.setProperty(JassKey,fileNameCache);
    }

    protected String getPrincipal() {
        return principal;
    }

    protected String getKeyTabFileName() {
        return keyTab;
    }

    @Override
    public void init() throws Exception {
        StringBuilder sb = new StringBuilder();

        sb.append("---------------------------------------------------------------------------\n");
        sb.append("------ start init " + getName() + "------\n");
        sb.append("---------------------------------------------------------------------------\n");

        System.out.println(sb.toString());

        initialize();

        sb.setLength(0);
        sb.append("---------------------------------------------------------------------------\n");
        sb.append("------ finished init " + getName() + "------\n");
        sb.append("---------------------------------------------------------------------------\n");
        System.out.println(sb.toString());
    }

    @Override
    public void work() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("---------------------------------------------------------------------------\n");
        sb.append("------ start work " + getName() + "------\n");
        sb.append("---------------------------------------------------------------------------\n");
        System.out.println(sb.toString());

        doWork();

        sb.setLength(0);
        sb.append("---------------------------------------------------------------------------\n");
        sb.append("------ finished work " + getName() + "------\n");
        sb.append("---------------------------------------------------------------------------\n");
        System.out.println(sb.toString());
    }

    protected abstract void initialize() throws Exception;

    protected abstract void doWork() throws Exception;
}
