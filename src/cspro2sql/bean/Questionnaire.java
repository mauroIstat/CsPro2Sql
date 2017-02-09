package cspro2sql.bean;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Questionnaire {

    private final String plainText;
    private final Map<Record, List<List<Answer>>> microdata = new LinkedHashMap<>();
    private final List<String> checkErrors = new LinkedList<>();

    private String schema;
    private byte[] guid;

    public Questionnaire(String plainText) {
        this.plainText = plainText;
    }

    public void setRecordValues(Record record, List<List<Answer>> valuesList) {
        microdata.put(record, valuesList);
    }

    public List<List<Answer>> getRecordValues(Record record) {
        return microdata.get(record);
    }

    public Set<Map.Entry<Record, List<List<Answer>>>> getMicrodataSet() {
        return microdata.entrySet();
    }

    public String getPlainText() {
        return plainText;
    }

    public byte[] getGuid() {
        return guid;
    }

    public void setGuid(byte[] guid) {
        this.guid = guid;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public boolean checkValueSets() {
        boolean error = false;
        for (Map.Entry<Record, List<List<Answer>>> e : microdata.entrySet()) {
            for (List<Answer> answers : e.getValue()) {
                for (Answer a : answers) {
                    if (!a.validate()) {
                        checkErrors.add(a.getError());
                        error = true;
                    }
                }
            }
        }
        return !error;
    }

    public String getCheckErrors() {
        StringBuilder result = new StringBuilder();
        for (String s : checkErrors) {
            result.append(s).append('\n');
        }
        return result.toString();
    }

}
