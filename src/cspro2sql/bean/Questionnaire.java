
package cspro2sql.bean;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Questionnaire {
    
    private final String plainText;
    private final Map<Record, List<List<String>>> microdata = new LinkedHashMap<>();

    private String schema;
    private InputStream guid;

    public Questionnaire(String plainText) {
        this.plainText = plainText;
    }
    
    public void setRecordValues(Record record, List<List<String>> valuesList) {
        microdata.put(record, valuesList);
    }
    
    public List<List<String>> getRecordValues(Record record) {
        return microdata.get(record);
    }
    
    public Set<Map.Entry<Record, List<List<String>>>> getMicrodataSet() {
        return microdata.entrySet();
    }

    public String getPlainText() {
        return plainText;
    }

    public InputStream getGuid() {
        return guid;
    }

    public void setGuid(InputStream guid) {
        this.guid = guid;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
    
}
