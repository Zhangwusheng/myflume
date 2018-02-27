package com.ctg.aep.kerberostest;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KerberosTests {

    private Map<Integer,KerberosComponent> kerberosComponentMap = new HashMap<>();

    public KerberosTests(){

    }

    public void initComponents(){
        kerberosComponentMap.put(1,new CtgCacheComponent());

        kerberosComponentMap.put(2,new KafkaCacheJaas());
        kerberosComponentMap.put(3,new KafkaKeyTab());

        kerberosComponentMap.put(4,new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(5,new HbaseCacheJassLogin());
        kerberosComponentMap.put(6,new HbaseKeyTabLogin());
        kerberosComponentMap.put(7,new HbaseKeyTabNoLogin());

        CompoundComponent compoundComponent;

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaCacheJaas());
        kerberosComponentMap.put(8,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new CtgCacheComponent());
         kerberosComponentMap.put(9,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaKeyTab());
        kerberosComponentMap.put(10,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(11,compoundComponent);



        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(12,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(13,compoundComponent);



        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        kerberosComponentMap.put(14,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(15,compoundComponent);



        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        kerberosComponentMap.put(16,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(17,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        kerberosComponentMap.put(18,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(19,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(20,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        kerberosComponentMap.put(21,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        kerberosComponentMap.put(22,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        kerberosComponentMap.put(23,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        kerberosComponentMap.put(24,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(25,compoundComponent);



        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        kerberosComponentMap.put(26,compoundComponent);


        compoundComponent = new CompoundComponent();

        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(27,compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(28,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(29,compoundComponent);

        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        kerberosComponentMap.put(30, compoundComponent);


        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(31, compoundComponent);

    }

    public void print(){
        for (Map.Entry<Integer, KerberosComponent> integerKerberosComponentEntry : kerberosComponentMap.entrySet()) {
            System.out.println(integerKerberosComponentEntry.getKey()+":"+integerKerberosComponentEntry.getValue().getName());
        }
    }


    public void execute(int n ) throws Exception{
        if( kerberosComponentMap.containsKey(n)){
            KerberosComponent kerberosComponent = kerberosComponentMap.get(n);
            kerberosComponent.init();
            kerberosComponent.work();
        }else{
            System.out.println("Can not find :"+n);
        }
    }

    public static void main(String[] args) throws Exception{

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

        KerberosTests kerberosTests = new KerberosTests();
        kerberosTests.initComponents();

        if( args.length < 1 ){
            kerberosTests.print();
            return;
        }

        kerberosTests.execute(Integer.parseInt(args[0]));
    }
}
