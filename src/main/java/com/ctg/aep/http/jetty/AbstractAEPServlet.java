package com.ctg.aep.http.jetty;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAEPServlet extends HttpServlet {

    private JettyHttpServer httpServer;
    public static final String JSON_MIME_TYPE = "application/json";

    private static final String AEP_FAILURE_MESSAGE =
            "aep.failure.message";
    private static final String AEP_SUCCESS_MESSAGE =
            "aep.success.message";

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        httpServer = (JettyHttpServer)config.getServletContext().getAttribute(JettyHttpServer.AEP_VERVLET_CONTEXT_KEY);
        if( httpServer == null)
            throw new IllegalStateException("No Application is defined in the servlet context");
    }

    protected JettyHttpServer getApplication(){
        return httpServer;
    }

    /**
     * Checks for the existance of the parameter in the request
     *
     * @param request
     * @param param
     * @return
     */
    public boolean hasParam (HttpServletRequest request, String param ) {
        return HttpRequestUtils.hasParam ( request, param );
    }

    /**
     * Retrieves the param from the http servlet request. Will throw an exception
     * if not found
     *
     * @param request
     * @param name
     * @return
     * @throws ServletException
     */
    public String getParam (HttpServletRequest request, String name )
            throws ServletException {
        return HttpRequestUtils.getParam ( request, name );
    }

    /**
     * Retrieves the param from the http servlet request.
     *
     * @param request
     * @param name
     * @param defaultVal
     * @return
     */
    public String getParam (HttpServletRequest request, String name,
                            String defaultVal ) {
        return HttpRequestUtils.getParam ( request, name, defaultVal );
    }

    /**
     * Returns the param and parses it into an int. Will throw an exception if not
     * found, or a parse error if the type is incorrect.
     *
     * @param request
     * @param name
     * @return
     * @throws ServletException
     */
    public int getIntParam (HttpServletRequest request, String name )
            throws ServletException {
        return HttpRequestUtils.getIntParam ( request, name );
    }

    public int getIntParam (HttpServletRequest request, String name, int defaultVal ) {
        return HttpRequestUtils.getIntParam ( request, name, defaultVal );
    }

    public long getLongParam (HttpServletRequest request, String name )
            throws ServletException {
        return HttpRequestUtils.getLongParam ( request, name );
    }

    public long getLongParam (HttpServletRequest request, String name,
                              long defaultVal ) {
        return HttpRequestUtils.getLongParam ( request, name, defaultVal );
    }

    public Map< String, String > getParamGroup (HttpServletRequest request,
                                                String groupName ) throws ServletException {
        return HttpRequestUtils.getParamGroup ( request, groupName );
    }

    /**
     * Returns the session value of the request.
     *
     * @param request
     * @param key
     * @param value
     */
    protected void setSessionValue (HttpServletRequest request, String key,
                                    Object value ) {
        request.getSession ( true ).setAttribute ( key, value );
    }

    /**
     * Adds a session value to the request
     *
     * @param request
     * @param key
     * @param value
     */
    @SuppressWarnings ( { "unchecked" , "rawtypes" } )
    protected void addSessionValue (HttpServletRequest request, String key,
                                    Object value ) {
        List l = ( List ) request.getSession ( true ).getAttribute ( key );
        if ( l == null )
            l = new ArrayList( );
        l.add ( value );
        request.getSession ( true ).setAttribute ( key, l );
    }


    /**
     * Sets an error message in azkaban.failure.message in the cookie. This will
     * be used by the web client javascript to somehow display the message
     *
     * @param response
     * @param errorMsg
     */
    protected void setErrorMessageInCookie ( HttpServletResponse response,
                                             String errorMsg ) {
        Cookie cookie = new Cookie( AEP_FAILURE_MESSAGE, errorMsg );
        cookie.setPath ( "/" );
        response.addCookie ( cookie );
    }

    /**
     * Sets a message in azkaban.success.message in the cookie. This will be used
     * by the web client javascript to somehow display the message
     *
     * @param response
     * @param message
     */
    protected void setSuccessMessageInCookie ( HttpServletResponse response,
                                               String message ) {
        Cookie cookie = new Cookie( AEP_SUCCESS_MESSAGE, message );
        cookie.setPath ( "/" );
        response.addCookie ( cookie );
    }

    /**
     * Retrieves a cookie by name. Potential issue in performance if a lot of
     * cookie variables are used.
     *
     * @param request
     * @return
     */
    protected Cookie getCookieByName (HttpServletRequest request, String name ) {
        Cookie[] cookies = request.getCookies ( );
        if ( cookies != null ) {
            for ( Cookie cookie : cookies ) {
                if ( name.equals ( cookie.getName ( ) ) ) {
                    return cookie;
                }
            }
        }

        return null;
    }
    /**
     * Retrieves a success message from a cookie. azkaban.success.message
     *
     * @param request
     * @return
     */
    protected String getSuccessMessageFromCookie ( HttpServletRequest request ) {
        Cookie cookie = getCookieByName ( request, AEP_SUCCESS_MESSAGE );

        if ( cookie == null ) {
            return null;
        }
        return cookie.getValue ( );
    }

    /**
     * Retrieves a success message from a cookie. azkaban.failure.message
     *
     * @param request
     * @return
     */
    protected String getErrorMessageFromCookie ( HttpServletRequest request ) {
        Cookie cookie = getCookieByName ( request, AEP_FAILURE_MESSAGE );
        if ( cookie == null ) {
            return null;
        }

        return cookie.getValue ( );
    }

    /**
     * Writes json out to the stream.
     *
     * @param resp
     * @param obj
     * @throws IOException
     */
    protected void writeJSON (HttpServletResponse resp, Object obj )
            throws IOException {
        writeJSON ( resp, obj, false );
    }

    protected void writeJSON (HttpServletResponse resp, Object obj, boolean pretty )
            throws IOException {
        resp.setContentType ( JSON_MIME_TYPE );
        JSONUtils.toJSON ( obj, resp.getOutputStream ( ), true );
    }


    public static String createJsonResponse ( String status, String message,
                                              String action, Map< String, Object > params ) {
        HashMap< String, Object > response = new HashMap< String, Object > ( );
        response.put ( "status", status );
        if ( message != null ) {
            response.put ( "message", message );
        }
        if ( action != null ) {
            response.put ( "action", action );
        }
        if ( params != null ) {
            response.putAll ( params );
        }

        return JSONUtils.toJSON ( response );
    }
}
