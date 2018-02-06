package com.ctg.aep.http.jetty;

//import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.InetAccessHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
//import org.glassfish.jersey.servlet.ServletContainer;

public class JettyHttpServer {
public static final String AEP_VERVLET_CONTEXT_KEY = "AEP_APP";
    private Server jettyServer ;
    public void startup(){
        jettyServer = new Server(8080);
        ServletHolder servlet = new ServletHolder(ServletContainer.class);

        InetAccessHandler inetAccessHandler = new InetAccessHandler();
        inetAccessHandler.include("10.142.90.144");
        inetAccessHandler.include("10.142.90.145");
        inetAccessHandler.include("127.0.0.1");

        ServletContextHandler servletContextHandler = new ServletContextHandler();
        inetAccessHandler.setHandler(servletContextHandler);
        jettyServer.setHandler(inetAccessHandler);

        servletContextHandler.addServlet(new ServletHolder(new HistoryStatusServlet()),"/api/v1/getDeviceStatusHis");
        servletContextHandler.getServletContext().setAttribute(AEP_VERVLET_CONTEXT_KEY,this);
        try{
            jettyServer.start();
            jettyServer.join();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        JettyHttpServer jettyHttpServer = new JettyHttpServer();
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
