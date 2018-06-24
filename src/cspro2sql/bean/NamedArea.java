/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cspro2sql.bean;

import java.util.List;

/**
 *
 * @author josh
 */
public class NamedArea {
    
    private final String name;
    private final List<Integer> codes;
    
    public NamedArea(String name, List<Integer> codes)
    {
        this.name = name;
        this.codes = codes;
    }
    
    public String getName()
    {
        return name;
    }
    
    public List<Integer> getCodes()
    {
        return codes;
    }
    
    public int level()
    {
        return codes.size();
    }
}
