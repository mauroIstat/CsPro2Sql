
package cspro2sql.bean;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class represents a ValueSet defined by the tag [ValueSet] in the CSPro Dictionary
 * 
 * @author Istat Cooperation Unit
 */
public final class ValueSet {

    private String label;    
    private String name;
    private String link;
    private int valueLength;
    private final Map<String,String> values = new LinkedHashMap<>();

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public int getValueLength() {
        return valueLength;
    }

    public void setValueLength(int valueLength) {
        this.valueLength = valueLength;
    }

    public Map<String, String> getValues() {
        return values;
    }
    
    public String getValue(String key) {
        return this.values.get(key);
    }

    public void addValue(String key, String value) {
        this.values.put(key, value);
    }
    
    public boolean isEmpty() {
        return this.values.isEmpty();
    }
    
}
