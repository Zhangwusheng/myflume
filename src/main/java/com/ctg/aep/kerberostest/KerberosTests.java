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
        /**运行正常*/
        kerberosComponentMap.put(1,new CtgCacheComponent());
        /**先kinit可以成功*/
        kerberosComponentMap.put(2,new KafkaCacheJaas());
        /**运行成功*/
        kerberosComponentMap.put(3,new KafkaKeyTab());
        /**先kinit*/
        kerberosComponentMap.put(4,new HbaseCacheJassNoLogin());
        /**运行成功*/
        kerberosComponentMap.put(5,new HbaseCacheJassLogin());
        /**运行成功*/
        kerberosComponentMap.put(6,new HbaseKeyTabLogin());
        /**运行失败，权限问题*/
        kerberosComponentMap.put(7,new HbaseKeyTabNoLogin());

        CompoundComponent compoundComponent;

        /**运行失败，JAAS问题*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaCacheJaas());
        kerberosComponentMap.put(8,compoundComponent);

        /**运行失败，JAAS should use keytab*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new CtgCacheComponent());
         kerberosComponentMap.put(9,compoundComponent);

        /**运行失败，Jaas configuration not found*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new KafkaKeyTab());
        kerberosComponentMap.put(10,compoundComponent);

        /**Kafka成功，CtgCache监听zookeeper失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaKeyTab());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(11,compoundComponent);

        /***CtgCache成功，Hbase失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(12,compoundComponent);

        /***Hbase失败，没有等待CtgCache*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(13,compoundComponent);


        /***成功*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        kerberosComponentMap.put(14,compoundComponent);

        /***成功*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(15,compoundComponent);

        /***成功*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        kerberosComponentMap.put(16,compoundComponent);

        /***成功*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseKeyTabLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(17,compoundComponent);

        /***Hbase失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new CtgCacheComponent());
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        kerberosComponentMap.put(18,compoundComponent);

        /***Hbase失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new HbaseKeyTabNoLogin());
        compoundComponent.addComponent(new CtgCacheComponent());
        kerberosComponentMap.put(19,compoundComponent);

        /***Kafka失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassNoLogin());
        kerberosComponentMap.put(20,compoundComponent);

        /***Kafka失败*/
        compoundComponent = new CompoundComponent();
        compoundComponent.addComponent(new KafkaCacheJaas());
        compoundComponent.addComponent(new HbaseCacheJassLogin());
        kerberosComponentMap.put(21,compoundComponent);

        /***成功**/
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
