/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ctg.aep.dataserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.ctg.aep.data.AEPDataObject;
import com.ctg.aep.node.AEPDataServerConfigurationProvider;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.cli.*;
import org.apache.flume.*;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.node.*;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DataServerApplication {
  
  private static final Logger logger = LoggerFactory
          .getLogger(DataServerApplication.class);
  
//  public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";
  public static final String CONF_MONITOR_PREFIX = "aep.monitoring.";
  
  
  private final List<LifecycleAware> components;
  private final LifecycleSupervisor supervisor;
  private MaterializedConfiguration materializedConfiguration;
  private MonitorService monitorServer;
  private String configPropertiesFile;
  
  public DataServerApplication () {
    this(new ArrayList<LifecycleAware>(0));
  }
  
  public DataServerApplication ( List<LifecycleAware> components) {
    this.components = components;
    supervisor = new LifecycleSupervisor();
  }
  
  public synchronized void start() {
    for (LifecycleAware component : components) {
      supervisor.supervise(component,
              new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    }
  }
  
  @Subscribe
  public synchronized void handleConfigurationEvent(MaterializedConfiguration conf) {
    stopAllComponents();
    startAllComponents(conf);
  }
  
  public synchronized void stop() {
    supervisor.stop();
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }
  
  private void stopAllComponents() {
    if (this.materializedConfiguration != null) {
      logger.info("Shutting down configuration: {}", this.materializedConfiguration);
      for (Entry<String, SourceRunner> entry :
              this.materializedConfiguration.getSourceRunners().entrySet()) {
        try {
          logger.info("Stopping Source " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
      
      for (Entry<String, SinkRunner> entry :
              this.materializedConfiguration.getSinkRunners().entrySet()) {
        try {
          logger.info("Stopping Sink " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
      
      for (Entry<String, Channel> entry :
              this.materializedConfiguration.getChannels().entrySet()) {
        try {
          logger.info("Stopping Channel " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
    }
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }
  
  private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
    logger.info("Starting new configuration:{}", materializedConfiguration);
    
    this.materializedConfiguration = materializedConfiguration;
    
    for (Entry<String, Channel> entry :
            materializedConfiguration.getChannels().entrySet()) {
      try {
        logger.info("Starting Channel " + entry.getKey());
        supervisor.supervise(entry.getValue(),
                new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    /*
     * Wait for all channels to start.
     */
    for (Channel ch : materializedConfiguration.getChannels().values()) {
      while (ch.getLifecycleState() != LifecycleState.START
              && !supervisor.isComponentInErrorState(ch)) {
        try {
          logger.info("Waiting for channel: " + ch.getName() +
                  " to start. Sleeping for 500 ms");
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for channel to start.", e);
          Throwables.propagate(e);
        }
      }
    }
    
    for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
      try {
        logger.info("Starting Sink " + entry.getKey());
        supervisor.supervise(entry.getValue(),
                new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }
    
    for (Entry<String, SourceRunner> entry :
            materializedConfiguration.getSourceRunners().entrySet()) {
      try {
        logger.info("Starting Source " + entry.getKey());
        supervisor.supervise(entry.getValue(),
                new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }
    
    this.loadMonitoring();
  }
  
  @SuppressWarnings("unchecked")
  private void loadMonitoring() {
    
    if( configPropertiesFile == null || configPropertiesFile.isEmpty () ){
      logger.warn("configPropertiesFile is null,failed to loadMonitoring");
      return;
    }
  
    
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader (configPropertiesFile));
      Properties properties = new Properties();
      properties.load(reader);
  
      Set<String> keys = properties.stringPropertyNames();
  
      Class<? extends MonitorService> klass = MonitoringType.HTTP.getMonitorClass ();
      this.monitorServer = klass.newInstance();
      Context context = new Context();
      for (String key : keys) {
        if ( key.startsWith ( CONF_MONITOR_PREFIX ) ) {
          context.put ( key.substring ( CONF_MONITOR_PREFIX.length ( ) ),
                  properties.getProperty ( key ) );
        }
      }
      
    } catch (IOException ex) {
      logger.error("Unable to load file:" + configPropertiesFile
              + " (I/O failure) - Exception follows.", ex);
    }catch (Exception e) {
      logger.warn("Error starting monitoring. "
              + "Monitoring might not be available.", e);
    }finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
          logger.warn( "Unable to close file reader for file: " + configPropertiesFile, ex);
        }
      }
    }
    
//    Properties systemProps = System.getProperties();
//    Set<String> keys = systemProps.stringPropertyNames();
//    try {
//      Class<? extends MonitorService> klass = MonitoringType.HTTP.getMonitorClass ();
//      this.monitorServer = klass.newInstance();
//      Context context = new Context();
//      for (String key : keys) {
//        if (key.startsWith(CONF_MONITOR_PREFIX)) {
//          context.put(key.substring(CONF_MONITOR_PREFIX.length()),
//                  systemProps.getProperty(key));
//        }
//      }
//
//      monitorServer.configure(context);
//      monitorServer.start();
//    } catch (Exception e) {
//      logger.warn("Error starting monitoring. "
//              + "Monitoring might not be available.", e);
//    }
//
  }
  
  public static void main(String[] args) {
    
    try {
      
      Options options = new Options();
      
      Option option = new Option("n", "name", true, "the name of this agent");
      option.setRequired(true);
      options.addOption(option);
      
      option = new Option("f", "conf-file", true,
              "specify a config file (required if -z missing)");
      option.setRequired(false);
      options.addOption(option);
  
      option = new Option("v", "verbose", false, "display verbose text");
      option.setRequired(false);
      options.addOption(option);
      
      option = new Option("h", "help", false, "display help text");
      options.addOption(option);
      
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args);
      
      if (commandLine.hasOption('h')) {
        new HelpFormatter().printHelp("flume-ng agent", options, true);
        return;
      }
  
  
      String CDC_HOME_PROPERTY = "aep.home.dir";
      String CDCHome = System.getProperty ( CDC_HOME_PROPERTY, System.getenv ( "AEP_HOME" ) );
  
  
      LoggerContext lc = ( LoggerContext ) LoggerFactory.getILoggerFactory ( );
      JoranConfigurator configurator = new JoranConfigurator ( );
      configurator.setContext ( lc );
      lc.reset ( );
      try {
        configurator.doConfigure ( CDCHome + "/conf/logback-aep-dataserver.xml" );
      }
      catch ( JoranException e ) {
        e.printStackTrace ( );
        System.exit ( 1 );
      }
      
  
      boolean verbose = commandLine.hasOption("verbose");
      
      String agentName = commandLine.getOptionValue('n');
      
      DataServerApplication application = null;
      
      
      File configurationFile = new File(commandLine.getOptionValue('f'));

        /*
         * The following is to ensure that by default the agent will fail on
         * startup if the file does not exist.
         */
      if (!configurationFile.exists()) {
        throw new ParseException(
                "The specified configuration file does not exist: " + commandLine.getOptionValue('f'));
      }
      
      
      AEPDataServerConfigurationProvider configurationProvider =
              new AEPDataServerConfigurationProvider(agentName, configurationFile,verbose);
      application = new DataServerApplication ();
      application.handleConfigurationEvent(configurationProvider.getConfiguration());
      
      application.start();
      
      
      final DataServerApplication appReference = application;
      Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
        @Override
        public void run() {
          appReference.stop();
        }
      });
      
    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.", e);
    }
  }
}