package com.ctg.aep.http.jersery;

//import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.InetAccessHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JerseryHttpServer {

    private static Logger logger = LoggerFactory.getLogger(JerseryHttpServer.class);
    private Server jettyServer ;
    private ServletHolder servlet;

    public void startup(){
        logger.info("startup Called");
        jettyServer = new Server(8080);
        servlet= new ServletHolder(ServletContainer.class);

        Map<String,String> parameterMap = new HashMap<String, String>();
        //parameterMap.put("jersey.config.server.provider.classnames", "org.glassfish.jersey.server.ResourceConfig");
        parameterMap.put("jersey.config.server.provider.packages", "com.ctg.aep.http.jersery.action");
        parameterMap.put("javax.ws.rs.Application","com.ctg.aep.http.jersery.AEPResourceConfig");

        servlet.setInitParameters(parameterMap);
//        servlet.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
//                "com.sun.jersey.api.core.PackagesResourceConfig");
//
//        servlet.setInitParameter("com.sun.jersey.config.property.packages",
//                "com.ctg.aep.jersery.action");

        InetAccessHandler inetAccessHandler = new InetAccessHandler();
        inetAccessHandler.include("10.142.90.144");
        inetAccessHandler.include("10.142.90.145");
        inetAccessHandler.include("127.0.0.1");

        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
//        ServletContextHandler servletContextHandler = new ServletContextHandler();
        inetAccessHandler.setHandler(servletContextHandler);
        jettyServer.setHandler(inetAccessHandler);
        servletContextHandler.setContextPath("/");
        servletContextHandler.addServlet(servlet, "/*");
//        jettyServer.setHandler(inetAccessHandler);

        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        JerseryHttpServer jettyHttpServer = new JerseryHttpServer();
        jettyHttpServer.startup();
//        final Server server = new Server(8080);
//        ServletHolder servlet = new ServletHolder(ServletContainer.class);
//
//        servlet.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
//                "com.sun.jersey.api.core.PackagesResourceConfig");
//        servlet.setInitParameter("com.sun.jersey.config.property.packages",
//                "com.ctg.aep.jersery.action");
//
//        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
//
//        handler.setContextPath("/");
//
//        handler.addServlet(servlet, "/*");
//        server.setHandler(handler);
//
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//                                                 @Override
//                                                 public void run() {
//                                                     try {
//                                                         System.out.println("exiting");
//                                                         server.stop();
//                                                         server.destroy();
//                                                     } catch (Exception e) {
//                                                         e.printStackTrace();
//                                                     }
//                                                 }
//                                             }
//
//        );
//
//        try {
//            server.start();
//            server.join();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("start...in 82");

    }
}
