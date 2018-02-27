package com.ctg.aep.kerberostest;

public interface KerberosComponent {
    void init() throws Exception;
    void work() throws Exception;
    String getName();
}
