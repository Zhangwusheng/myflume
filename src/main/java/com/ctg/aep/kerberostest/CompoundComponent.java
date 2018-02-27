package com.ctg.aep.kerberostest;

import java.util.ArrayList;
import java.util.List;

public class CompoundComponent implements KerberosComponent {

    List<KerberosComponent> components = new ArrayList<>();

    public void addComponent(KerberosComponent kerberosComponent){
        components.add(kerberosComponent);
    }

    @Override
    public void init() throws Exception {
        for(int i=0;i<components.size();i++){
            components.get(i).init();
        }
    }

    @Override
    public void work() throws Exception {
        for(int i=0;i<components.size();i++){
            components.get(i).work();
        }
    }

    @Override
    public String getName() {

        StringBuilder sb = new StringBuilder();
        for(int i=0;i<components.size();i++){
            sb.append(components.get(i).getName());
            sb.append(",");
        }

        sb.setLength(sb.length()-1);

        return sb.toString();
    }
}
