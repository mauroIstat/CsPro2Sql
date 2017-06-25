package cspro2sql.sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import cspro2sql.bean.Tag;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * @version 0.9.12
 */
public class TemplateManager {

    private static final Pattern PARAM_KEY = Pattern.compile("[\\s`.']@");

    private final Dictionary dictionary;
    private final Map<String, String> params;
    private final List<Item> ea;
    private final List<String> eaName;
    private final List<String> eaDescription;
    private final int[] ageRange;

    public TemplateManager(Dictionary dictionary) {
        this.dictionary = dictionary;
        Record mainRecord = dictionary.getMainRecord();
        String schema = dictionary.getSchema();

        this.params = new HashMap<>();
        this.params.put("@SCHEMA", schema);
        this.params.put("@SOURCE_DATA_TABLE", dictionary.getName());
        this.params.put("@QUESTIONNAIRE_TABLE", mainRecord.getTableName());
        this.params.put("@QUESTIONNAIRE_COLUMN_BASE", mainRecord.getName());

        if (dictionary.hasTagged(Dictionary.TAG_TERRITORY)) {
            Iterable<Item> territories = dictionary.getTaggedItems(Dictionary.TAG_TERRITORY);
            this.ea = new ArrayList<>();
            this.eaName = new ArrayList<>();
            this.eaDescription = new ArrayList<>();
            for (Item territory : territories) {
                this.ea.add(territory);
                Tag tag = territory.getTag(Dictionary.TAG_TERRITORY);
                if (tag.getValue() != null) {
                    String[] tagValues = tag.getValue().split(",");
                    this.eaName.add(tagValues[0]);
                    if (tagValues.length > 1 && !tagValues[1].isEmpty()) {
                        this.eaDescription.add(mainRecord.getValueSetPrefix() + tagValues[1]);
                    } else {
                        this.eaDescription.add("");
                    }
                } else {
                    this.eaName.add("");
                    this.eaDescription.add("");
                }
            }
            this.params.put("@QUESTIONNAIRE_COLUMN_REGION", ea.get(0).getName());
            this.params.put("@VALUESET_REGION", eaDescription.get(0));
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
        } else {
            this.ageRange = null;
        }

        if (dictionary.hasTagged(Dictionary.TAG_INDIVIDUAL)) {
            this.params.put("@INDIVIDUAL_TABLE", mainRecord.getTablePrefix() + dictionary.getTaggedRecord(Dictionary.TAG_INDIVIDUAL).getName());
        }
        if (dictionary.hasTagged(Dictionary.TAG_SEX)) {
            String itemName = dictionary.getTaggedItem(Dictionary.TAG_SEX).getName();
            this.params.put("@INDIVIDUAL_COLUMN_SEX", itemName);
            this.params.put("@VALUESET_SEX", mainRecord.getValueSetPrefix() + itemName);
        }
        if (dictionary.hasTagged(Dictionary.TAG_AGE)) {
            this.params.put("@INDIVIDUAL_COLUMN_AGE", dictionary.getTaggedItem(Dictionary.TAG_AGE).getName());
        }
        if (dictionary.hasTagged(Dictionary.TAG_RELIGION)) {
            String itemName = dictionary.getTaggedItem(Dictionary.TAG_RELIGION).getName();
            this.params.put("@INDIVIDUAL_COLUMN_RELIGION", itemName);
            this.params.put("@VALUESET_RELIGION", mainRecord.getValueSetPrefix() + itemName);
        }
        if (dictionary.hasTagged(Dictionary.TAG_MALE)) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_MALE", dictionary.getTaggedValueSetValue(Dictionary.TAG_MALE).getKey());
        }
        if (dictionary.hasTagged(Dictionary.TAG_FEMALE)) {
            this.params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", dictionary.getTaggedValueSetValue(Dictionary.TAG_FEMALE).getKey());
        }
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public String getParam(String key) {
        return this.params.get(key);
    }

    public List<Item> getEa() {
        return ea;
    }

    public List<String> getEaName() {
        return eaName;
    }

    public List<String> getEaDescription() {
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
