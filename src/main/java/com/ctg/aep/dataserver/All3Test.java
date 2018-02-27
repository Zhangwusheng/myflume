package com.ctg.aep.dataserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.awt.image.ImagingOpException;
import java.io.IOException;

public class All3Test {

    CtgCacheTest ctgCacheTest = new CtgCacheTest();
    HbaseTestApplicaiton hbaseTestApplicaiton = new HbaseTestApplicaiton();
    KafkaTestApplicaiton kafkaTestApplicaiton = new KafkaTestApplicaiton();
    public All3Test(){

    }


    public void init() throws IOException,LoginException{
        ctgCacheTest.init();
        kafkaTestApplicaiton.init();

//        hbaseTestApplicaiton.init();

    }

    public void work() throws  Exception{
        ctgCacheTest.testWriteAndRead();
//        hbaseTestApplicaiton.test();
        kafkaTestApplicaiton.testConsumeKafkaKerberos();
    }

    public static void main(String[] args) throws  Exception {

        String CDC_HOME_PROPERTY = "aep.home.dir";
        String CDCHome = System.getProperty(CDC_HOME_PROPERTY, System.getenv("AEP_HOME"));


        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        try {
            configurator.doConfigure(CDCHome + "/conf/logback-aep-dataserver.xml");
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(1);
        }

        All3Test all3Test = new All3Test();
        all3Test.init();
        all3Test.work();
    }
}
