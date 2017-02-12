package cspro2sql.sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Record;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TemplateManager {

    private final Map<String, String> params;
    private final String[] ea;
    private final String[] eaName;
    private final String[] eaDescription;
    private final int[] ageRange;

    public TemplateManager(Dictionary dictionary, Properties prop) {
        Record mainRecord = dictionary.getMainRecord();
        String schema = prop.getProperty("db.dest.schema");

        this.ea = prop.getProperty("column.questionnaire.ea").split(",");
        this.eaName = Arrays.copyOf(prop.getProperty("column.questionnaire.ea.name").split(","), ea.length);
        this.eaDescription = Arrays.copyOf(prop.getProperty("column.questionnaire.ea.description").split(","), ea.length);
        for (int i = 0; i < eaDescription.length; i++) {
            if (eaDescription[i] != null && !eaDescription[i].isEmpty()) {
                eaDescription[i] = mainRecord.getValueSetPrefix() + eaDescription[i];
            }
        }
        String[] ageRangeS = prop.getProperty("range.individual.age").split(",");
        this.ageRange = new int[ageRangeS.length];
        for (int i = 0; i < ageRangeS.length; i++) {
            ageRange[i] = Integer.parseInt(ageRangeS[i]);
        }

        this.params = new HashMap<>();
        this.params.put("@SCHEMA", schema);
        this.params.put("@SOURCE_DATA_TABLE", prop.getProperty("db.source.data.table"));
        this.params.put("@QUESTIONNAIRE_TABLE", mainRecord.getTableName());
        this.params.put("@QUESTIONNAIRE_COLUMN_BASE", mainRecord.getName());
        this.params.put("@QUESTIONNAIRE_COLUMN_REGION", ea[0]);
        this.params.put("@INDIVIDUAL_TABLE", mainRecord.getTablePrefix() + prop.getProperty("table.individual"));
        this.params.put("@INDIVIDUAL_COLUMN_SEX", prop.getProperty("column.individual.sex"));
        this.params.put("@INDIVIDUAL_COLUMN_AGE", prop.getProperty("column.individual.age"));
        this.params.put("@INDIVIDUAL_COLUMN_RELIGION", prop.getProperty("column.individual.religion"));
        this.params.put("@INDIVIDUAL_VALUE_SEX_MALE", prop.getProperty("column.individual.sex.value.male"));
        this.params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", prop.getProperty("column.individual.sex.value.female"));
        this.params.put("@VALUESET_REGION", eaDescription[0]);
        this.params.put("@VALUESET_SEX", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.sex"));
        this.params.put("@VALUESET_RELIGION", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.religion"));
    }

    public String getParam(String key) {
        return this.params.get(key);
    }

    public String[] getEa() {
        return ea;
    }

    public String[] getEaName() {
        return eaName;
    }

    public String[] getEaDescription() {
        return eaDescription;
    }

    public int[] getAgeRange() {
        return ageRange;
    }

    public void printTemplate(String template, PrintStream ps) throws IOException {
        try (InputStream in = TemplateManager.class.getResourceAsStream("/cspro2sql/sql/template/" + template + ".sql")) {
            try (InputStreamReader isr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        for (Map.Entry<String, String> e : this.params.entrySet()) {
                            line = line.replace(e.getKey(), e.getValue());
                        }
                        ps.println(line);
                    }
                }
            }
        }
    }

}
