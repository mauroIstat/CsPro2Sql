package cspro2sql.writer;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import cspro2sql.bean.Tag;
import cspro2sql.sql.TemplateManager;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

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
 * @author Paolo Giacomi <giacomi @ istat.it>
 * @version 0.9.9
 * @since 0.9.9
 */
public class LinkageWriter {

    private static final String PES_TABLE = "linkage_pes_matching_var";
    private static final String CENSUS_TABLE = "linkage_cens_matching_var";

    public static boolean write(String schema,
            Dictionary censusDictionary, Dictionary pesDictionary,
            TemplateManager tm, PrintStream out) {
        printLinkage(schema, CENSUS_TABLE, censusDictionary, tm, out);
        printLinkage(schema, PES_TABLE, pesDictionary, tm, out);
        try {
            tm.printTemplate("linked_record", out);
            tm.printTemplate("linked_individual", out);
            tm.printTemplate("functions", out);
            tm.printTemplate("proc_linkage_step_1", out);
            tm.printTemplate("proc_linkage_step_2", out);
            tm.printTemplate("proc_linkage_step_3", out);
            tm.printTemplate("proc_linkage_step_4", out);
            tm.printTemplate("proc_linkage_step_5", out);
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

    private static void printLinkage(String schema, String table, Dictionary dictionary, TemplateManager tm, PrintStream out) {
        String[] eas = tm.getEa();

        Record mainRecord = dictionary.getMainRecord();
        Record individual = dictionary.getTaggedRecord(Dictionary.TAG_INDIVIDUAL);

        Map<String, Tag> tags = new LinkedHashMap<>();
        tags.put("NAME1", Dictionary.TAG_FIRSTNAME);
        tags.put("NAME2", Dictionary.TAG_MIDDLENAME);
        tags.put("NAME3", Dictionary.TAG_LASTNAME);
        tags.put("RELAT", Dictionary.TAG_RELATIONSHIP);
        tags.put("SEX", Dictionary.TAG_SEX);
        tags.put("AGE", Dictionary.TAG_AGE);
        tags.put("RELIGION", Dictionary.TAG_RELIGION);
        tags.put("TONGUE", Dictionary.TAG_TONGUE);
        tags.put("MARITAL", Dictionary.TAG_MARITAL);
        tags.put("GRADE", Dictionary.TAG_GRADE);

        Map<String, Tag> normalizedTags = new LinkedHashMap<>();
        normalizedTags.put("FIXED_NAME_1", Dictionary.TAG_FIRSTNAME);
        normalizedTags.put("FIXED_NAME_2", Dictionary.TAG_MIDDLENAME);
        normalizedTags.put("FIXED_NAME_3", Dictionary.TAG_LASTNAME);

        out.println("DROP TABLE IF EXISTS " + schema + "." + table + ";");
        out.println();
        out.println("CREATE TABLE " + schema + "." + table + " AS");
        out.println("(SELECT q.ID ID_QUEST,");
        out.println("    i.ID ID_INDIV,");
        for (Map.Entry<String, Tag> tag : tags.entrySet()) {
            Item item = dictionary.getTaggedItem(tag.getValue());
            String value = dictionary.getTag(tag.getValue()).getValue();
            if (value != null && value.startsWith("FUNC_")) {
                String func = value.replace("FUNC_", "");
                out.println("    " + func + "(i." + item.getName() + ") " + tag.getKey() + ",");
            } else {
                out.println("    i." + item.getName() + " " + tag.getKey() + ",");
            }
        }
        out.print("    concat(" + eas[0]);
        for (int i = 1; i < eas.length; i++) {
            out.print(",'-',q." + eas[i]);
        }
        out.print(") COD_EA");
        for (Map.Entry<String, Tag> tag : normalizedTags.entrySet()) {
            Item item = dictionary.getTaggedItem(tag.getValue());
            out.println(",");
            String value = dictionary.getTag(tag.getValue()).getValue();
            if (value != null && value.startsWith("FUNC_")) {
                String func = value.replace("FUNC_", "");
                out.print("    get_normalized_name(" + func + "(i." + item.getName() + ")) " + tag.getKey());
            } else {
                out.print("    get_normalized_name(i." + item.getName() + ") " + tag.getKey());
            }
        }
        out.println();
        out.println("FROM " + schema + "." + individual.getTableName() + " i INNER JOIN " + schema + "." + mainRecord.getTableName() + " q ON q.ID = i." + mainRecord.getName() + ");");
        out.println();
        out.println("ALTER TABLE " + schema + "." + table + " ADD PRIMARY KEY (`ID_INDIV`);");
        out.println();
    }

}
