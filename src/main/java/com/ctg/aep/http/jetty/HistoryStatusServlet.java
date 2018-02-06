package com.ctg.aep.http.jetty;

import com.ctg.aep.data.QueryObject;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class HistoryStatusServlet extends AbstractAEPServlet{

    public HistoryStatusServlet(){

    }
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//        super.doGet(req, resp);
        QueryObject queryObject = new QueryObject();
        queryObject.setTenantId("zws");
        queryObject.setDatasetId("dataset");
        queryObject.setProductId("product");
        queryObject.setDeviceId("deviceId");
        writeJSON(resp,queryObject);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(req.getInputStream()));
        String body = IOUtils.toString(req.getInputStream(), Charsets.UTF_8);
//        req.get

        QueryObject queryObject = new QueryObject();
        queryObject.setTenantId("zws");
        queryObject.setDatasetId("dataset");
        queryObject.setProductId("product");
        queryObject.setDeviceId("deviceId");
        writeJSON(resp,queryObject);
//        super.doPost(req, resp);
    }
}
