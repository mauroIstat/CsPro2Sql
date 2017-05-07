package cspro2sql.sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Record;
import cspro2sql.bean.Tag;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Copyright 2017 ISTAT
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence. You may
 * obtain a copy of the Licence at:
 *
 * http://ec.europa.eu/idabc/eupl5
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * Licence for the specific language governing permissions and limitations under
 * the Licence.
 *
 * @author Guido Drovandi <drovandi @ istat.it>
 * @author Mauro Bruno <mbruno @ istat.it>
 * @version 0.9.8
 */
public class TemplateManager {

    private static final Pattern PARAM_KEY = Pattern.compile("[\\s`.']@");

    private final Map<String, String> params;
    private final String[] ea;
    private final String[] eaName;
    private final String[] eaDescription;
    private final int[] ageRange;

    public TemplateManager(Dictionary dictionary, Properties prop) {
        Record mainRecord = dictionary.getMainRecord();
        String schema = prop.getProperty("db.dest.schema");

        this.params = new HashMap<>();
        this.params.put("@SCHEMA", schema);
        this.params.put("@SOURCE_DATA_TABLE", prop.getProperty("db.source.data.table"));
        this.params.put("@QUESTIONNAIRE_TABLE", mainRecord.getTableName());
        this.params.put("@QUESTIONNAIRE_COLUMN_BASE", mainRecord.getName());

        if (prop.containsKey("column.questionnaire.ea")
                && prop.containsKey("column.questionnaire.ea.name")
                && prop.containsKey("column.questionnaire.ea.description")) {
            this.ea = prop.getProperty("column.questionnaire.ea").split(",");
            this.eaName = Arrays.copyOf(prop.getProperty("column.questionnaire.ea.name").split(","), ea.length);
            this.eaDescription = Arrays.copyOf(prop.getProperty("column.questionnaire.ea.description").split(","), ea.length);
            for (int i = 0; i < eaDescription.length; i++) {
                if (eaDescription[i] != null && !eaDescription[i].isEmpty()) {
                    eaDescription[i] = mainRecord.getValueSetPrefix() + eaDescription[i];
                }
            }
            this.params.put("@QUESTIONNAIRE_COLUMN_REGION", ea[0]);
            this.params.put("@VALUESET_REGION", eaDescription[0]);
        } else {
            this.ea = null;
            this.eaName = null;
            this.eaDescription = null;
        }

        if (dictionary.hasTagged(Dictionary.TAG_AGE)) {
            Tag tag = dictionary.getTag(Dictionary.TAG_AGE);
            if (tag.getValue() != null) {
                String[] ageRangeS = tag.getValue().split(",");
                this.ageRange = new int[ageRangeS.length];
                for (int i = 0; i < ageRangeS.length; i++) {
                    ageRange[i] = Integer.parseInt(ageRangeS[i]);
                }
            } else {
                this.ageRange = null;
            }
        } else if (prop.containsKey("range.individual.age")) {
            String[] ageRangeS = prop.getProperty("range.individual.age").split(",");
            this.ageRange = new int[ageRangeS.length];
            for (int i = 0; i < ageRangeS.length; i++) {
                ageRange[i] = Integer.parseInt(ageRangeS[i]);
            }
        } else {
            this.ageRange = null;
        }

        if (dictionary.hasTagged(Dictionary.TAG_INDIVIDUAL)) {
            this.params.put("@INDIVIDUAL_TABLE", mainRecord.getTablePrefix() + dictionary.getTaggedRecord(Dictionary.TAG_INDIVIDUAL).getName());
        } else if (prop.containsKey("table.individual")) {
            this.params.put("@INDIVIDUAL_TABLE", mainRecord.getTablePrefix() + prop.getProperty("table.individual"));
        }
        if (dictionary.hasTagged(Dictionary.TAG_SEX)) {
            String itemName = dictionary.getTaggedItem(Dictionary.TAG_SEX).getName();
            this.params.put("@INDIVIDUAL_COLUMN_SEX", itemName);
            this.params.put("@VALUESET_SEX", mainRecord.getValueSetPrefix() + itemName);
        } else if (prop.containsKey("column.individual.sex")) {
            this.params.put("@INDIVIDUAL_COLUMN_SEX", prop.getProperty("column.individual.sex"));
            this.params.put("@VALUESET_SEX", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.sex"));
        }
        if (dictionary.hasTagged(Dictionary.TAG_AGE)) {
            this.params.put("@INDIVIDUAL_COLUMN_AGE", dictionary.getTaggedItem(Dictionary.TAG_AGE).getName());
        } else if (prop.containsKey("column.individual.age")) {
            this.params.put("@INDIVIDUAL_COLUMN_AGE", prop.getProperty("column.individual.age"));
        }
        if (dictionary.hasTagged(Dictionary.TAG_RELIGION)) {
            String itemName = dictionary.getTaggedItem(Dictionary.TAG_RELIGION).getName();
            this.params.put("@INDIVIDUAL_COLUMN_RELIGION", itemName);
            this.params.put("@VALUESET_RELIGION", mainRecord.getValueSetPrefix() + itemName);
        } else if (prop.containsKey("column.individual.religion")) {
            this.params.put("@INDIVIDUAL_COLUMN_RELIGION", prop.getProperty("column.individual.religion"));
            this.params.put("@VALUESET_RELIGION", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.religion"));
        }
        if (dictionary.hasTagged(Dictionary.TAG_MALE)) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_MALE", dictionary.getTaggedValueSetValue(Dictionary.TAG_MALE).getKey());
        } else if (prop.containsKey("column.individual.sex.value.male")) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_MALE", prop.getProperty("column.individual.sex.value.male"));
        }
        if (dictionary.hasTagged(Dictionary.TAG_FEMALE)) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", dictionary.getTaggedValueSetValue(Dictionary.TAG_FEMALE).getKey());
        } else if (prop.containsKey("column.individual.sex.value.female")) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", prop.getProperty("column.individual.sex.value.female"));
        }
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

    public boolean printTemplate(String template, PrintStream ps) throws IOException {
        try (InputStream in = TemplateManager.class.getResourceAsStream("/cspro2sql/sql/template/" + template + ".sql")) {
            try (InputStreamReader isr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    String line;
                    StringBuilder output = new StringBuilder();
                    while ((line = br.readLine()) != null) {
                        for (Map.Entry<String, String> e : this.params.entrySet()) {
                            if (e.getValue() != null) {
                                line = line.replace(e.getKey(), e.getValue());
                            }
                        }
                        output.append(line).append("\n");
                    }
                    line = output.toString();
                    if (PARAM_KEY.matcher(line).find()) {
                        return false;
                    }
                    ps.print(line);
                    return true;
                }
            }
        }
    }

}
