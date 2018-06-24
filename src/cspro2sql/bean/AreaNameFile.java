/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cspro2sql.bean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author josh
 */
public class AreaNameFile {
    
    private final String[] levels;
    private final NamedArea[] areas;
    private final Map<List<Integer>, NamedArea> codeToArea;
    
    public AreaNameFile(String[] levels, NamedArea[] areas)
    {
        this.levels = levels;
        this.areas = areas;
        codeToArea = new HashMap<>();
        for (NamedArea area : areas)
            codeToArea.put(area.getCodes(), area);
    }
    
    public String[] getLevels()
    {
        return levels;
    }
            
    public NamedArea[] getAreas()
    {
        return areas;
    }
    
    public NamedArea lookup(List<Integer> codes)
    {
        return codeToArea.get(codes);
    }
}
