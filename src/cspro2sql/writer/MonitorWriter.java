package cspro2sql.writer;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import cspro2sql.sql.TemplateManager;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
public class MonitorWriter {

    private static final String[] TEMPLATES = new String[]{
        "r_questionnaire_info",
        "r_individual_info",
        "r_religion",
        "r_sex_by_age",
        "r_sex_by_region"
    };

    public static boolean write(TemplateManager tm, TemplateManager tmListing, TemplateManager tmExpected, PrintStream out) {
        String schema = tm.getDictionary().getSchema();

        out.println("USE " + schema + ";");
        out.println();

        List<Item> ea = tm.getEa();
        List<String> eaName = tm.getEaName();
        List<String> eaDescription = tm.getEaDescription();
        int[] ageRange = tm.getAgeRange();

        try {
            tm.printTemplate("c_user", out);
            tm.printTemplate("cspro2sql_report", out);
        } catch (IOException ex) {
            return false;
        }

        try {
            for (String template : TEMPLATES) {
                if (tm.printTemplate(template, out)) {
                    printMaterialized(schema, template, out);
                }
            }
        } catch (IOException ex) {
            return false;
        }

        if (ea != null) {
            out.println("CREATE OR REPLACE VIEW " + schema + ".`r_regional_area` AS");
            String groupBy = ea.get(0).getName();
            String name = eaName.get(0);
            out.print("  SELECT '" + name + "' name, COUNT(0) value FROM (SELECT COUNT(0) FROM " + schema + "." + tm.getParam("@QUESTIONNAIRE_TABLE") + " GROUP BY " + groupBy + ") a0");
            for (int i = 1; i < ea.size(); i++) {
                name = eaName.get(i);
                groupBy += "," + ea.get(i).getName();
                out.println(" UNION");
                out.print("  SELECT '" + name + "', COUNT(0) FROM (SELECT COUNT(0) FROM " + schema + "." + tm.getParam("@QUESTIONNAIRE_TABLE") + " GROUP BY " + groupBy + ") a" + i);
            }
            out.println();
            out.println(";");
            printMaterialized(schema, "r_regional_area", out);
        }

        if (ageRange != null && tm.getParam("@INDIVIDUAL_TABLE") != null && tm.getParam("@INDIVIDUAL_COLUMN_SEX") != null
                && tm.getParam("@INDIVIDUAL_VALUE_SEX_MALE") != null && tm.getParam("@INDIVIDUAL_VALUE_SEX_FEMALE") != null
                && tm.getParam("@INDIVIDUAL_COLUMN_AGE") != null) {
            out.println("CREATE OR REPLACE VIEW " + schema + ".`r_sex_by_age_group` AS");
            out.print("  SELECT '" + ageRange[0] + " to " + (ageRange[1] - 1) + "' as 'range', a.male, b.female FROM "
                    + "(SELECT COUNT(0) male FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_MALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[0] + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[1] + ") a,"
                    + "(SELECT COUNT(0) female FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_FEMALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[0] + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[1] + ") b");
            for (int i = 1; i < ageRange.length - 1; i++) {
                out.println(" UNION");
                out.print("  SELECT '" + ageRange[i] + " to " + (ageRange[i + 1] - 1) + "' as 'range', a.male, b.female FROM "
                        + "(SELECT COUNT(0) male FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_MALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[i] + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[i + 1] + ") a,"
                        + "(SELECT COUNT(0) female FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_FEMALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[i] + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[i + 1] + ") b");
            }
            out.println(" UNION");
            out.print("  SELECT '" + ageRange[ageRange.length - 1] + "+' as 'range', a.male, b.female FROM "
                    + "(SELECT COUNT(0) male FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_MALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[ageRange.length - 1] + ") a,"
                    + "(SELECT COUNT(0) female FROM " + schema + "." + tm.getParam("@INDIVIDUAL_TABLE") + " WHERE " + tm.getParam("@INDIVIDUAL_COLUMN_SEX") + " = " + tm.getParam("@INDIVIDUAL_VALUE_SEX_FEMALE") + " AND " + tm.getParam("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[ageRange.length - 1] + ") b");
            out.println();
            out.println(";");
            printMaterialized(schema, "r_sex_by_age_group", out);
        }

        if (ea != null) {
            out.println("CREATE OR REPLACE VIEW " + schema + ".`r_household_by_ea` AS");
            out.print("  SELECT concat(");
            out.print("'" + eaName.get(0) + "'");
            for (int i = 1; i < eaName.size(); i++) {
                out.print(",'#','" + eaName.get(i) + "'");
            }
            out.println(") as name, null as household");
            out.println("  UNION");
            out.print("  SELECT concat(");
            if (eaDescription.get(0) == null || eaDescription.get(0).isEmpty()) {
                out.print("h." + ea.get(0).getName());
            } else {
                out.print("vs0.value");
            }
            for (int i = 1; i < ea.size(); i++) {
                if (eaDescription.get(i) == null || eaDescription.get(i).isEmpty()) {
                    out.print(",'#',h." + ea.get(i).getName());
                } else {
                    out.print(",'#',vs" + i + ".value");
                }
            }
            out.println(") as name, COUNT(0) AS `household`");
            out.println("  FROM " + schema + "." + tm.getParam("@QUESTIONNAIRE_TABLE") + " `h`");
            for (int i = 0; i < ea.size(); i++) {
                if (eaDescription.get(i) != null && !eaDescription.get(i).isEmpty()) {
                    out.println("    JOIN " + schema + "." + eaDescription.get(i) + " vs" + i + " ON `h`.`" + ea.get(i).getName() + "` = vs" + i + ".`ID`");
                }
            }
            out.print("  GROUP BY ");
            out.print("`h`." + ea.get(0).getName());
            for (int i = 1; i < ea.size(); i++) {
                out.print(", `h`." + ea.get(i).getName());
            }
            out.println(";");
            printMaterialized(schema, "r_household_by_ea", out);
        }

        if (ea != null) {
            printAuxTable(tm, tm, "aux_household_returned", "returned", out);
            printAuxTable(tm, tmListing, "aux_listing_returned", "returned", out);
            printAuxTable(tm, tmExpected, "aux_ea_expected", "expected", out);

            int upTo = 1;
            for (String name : eaName) {
                printExpectedReport(tm, "r_household_expected_by_" + name.toLowerCase(), upTo++, out);
                printMaterialized(schema, "r_household_expected_by_" + name.toLowerCase(), out);
            }
        }

        return true;
    }

    private static void printMaterialized(String schema, String name, PrintStream out) {
        out.println("DROP TABLE IF EXISTS " + schema + ".m" + name + ";");
        out.println("SELECT @ID := 0;");
        out.println("CREATE TABLE " + schema + ".m" + name + " (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, " + name + ".* FROM " + schema + "." + name + ";");
        out.println("INSERT INTO " + schema + ".`cspro2sql_report` VALUES ('" + name + "');");
        out.println();
    }

    private static void printAuxTable(TemplateManager mainTm, TemplateManager tm, String auxName, String columnName, PrintStream out) {
        if (tm != null && tm.getEa() != null) {
            Set<Record> records = new LinkedHashSet<>();
            List<Item> eaExpected = tm.getEa();
            out.println("CREATE VIEW " + tm.getDictionary().getSchema() + "." + auxName + " AS");
            out.println("    SELECT ");
            for (int i = 0; i < eaExpected.size(); i++) {
                Item item = eaExpected.get(i);
                out.println("        " + item.getColunmFullName() + " AS " + mainTm.getEa().get(i).getName() + ",");
                records.add(item.getRecord());
            }
            Record[] recArray = records.toArray(new Record[0]);
            Item expected = tm.getDictionary().getTaggedItem(Dictionary.TAG_EXPECTED_QUESTIONNAIRES);
            if (expected == null) {
                out.println("        COUNT(0) AS `returned`");
            } else {
                out.println("        SUM(" + expected.getColunmFullName() + ") AS `expected`");
            }
            out.println("    FROM");
            out.println("        " + recArray[0].getMainRecord().getFullTableName());
            for (Record record : recArray) {
                if (!record.isMainRecord()) {
                    out.println("            JOIN " + record.getFullTableName() + " ON " + record.getMainRecord().getTableName() + ".ID = " + record.getTableName() + "." + record.getMainRecord().getName());
                }
            }
            out.println("    GROUP BY");
            for (int i = 0; i < eaExpected.size() - 1; i++) {
                Item item = eaExpected.get(i);
                out.println("        " + item.getColunmFullName() + ",");
            }
            Item item = eaExpected.get(eaExpected.size() - 1);
            out.println("        " + item.getColunmFullName() + ";");
        } else {
            out.println("CREATE VIEW " + tm.getDictionary().getSchema() + ".`aux_ea_expected` AS");
            out.println("    SELECT ");
            for (Item item : mainTm.getEa()) {
                out.println("        '" + item.getName() + "' AS `" + item.getName() + "`,");
            }
            out.println("        -1 AS `" + columnName + "`;");
        }
        out.println();
    }

    private static void printExpectedReport(TemplateManager tm, String reportName, int upTo, PrintStream out) {
        String schema = tm.getDictionary().getSchema();
        List<Item> ea = tm.getEa();
        List<String> eaName = tm.getEaName();
        List<String> eaDescription = tm.getEaDescription();

        out.println("CREATE VIEW `" + reportName + "` AS");
        out.println("    SELECT ");
        out.print("        '" + eaName.get(0));
        for (int i = 1; i < upTo; i++) {
            out.print("#" + eaName.get(i));
        }
        out.println("' AS `name`,");
        out.println("        NULL AS `field`,");
        out.println("        NULL AS `freshlist`,");
        out.println("        NULL AS `expected`,");
        out.println("        NULL AS `field_freshlist`,");
        out.println("        NULL AS `field_expected`,");
        out.println("        NULL AS `freshlist_expected`");
        out.println("    ");
        out.println("    UNION SELECT ");
        out.print("        CONCAT(");
        int vsCounter = 0;
        if (eaDescription.get(0) != null && !eaDescription.get(0).isEmpty()) {
            out.print("`vs" + (vsCounter++) + "`.`value`");
        } else {
            out.print("`h`.`" + ea.get(0).getName() + "`");
        }
        for (int i = 1; i < upTo; i++) {
            Item item = ea.get(i);
            if (eaDescription.get(i) != null && !eaDescription.get(i).isEmpty()) {
                out.print(",'#',`vs" + (vsCounter++) + "`.`value`");
            } else {
                out.print(",'#',`h`.`" + item.getName() + "`");
            }
        }
        out.println(") AS `name`,");
        out.println("        SUM(`h`.`returned`) AS `returned`,");
        out.println("        SUM(`l`.`returned`) AS `returned`,");
        out.println("        SUM(`e`.`expected`) AS `expected`,");
        out.println("        ((SUM(`h`.`returned`) / SUM(`l`.`returned`)) * 100) AS `field_freshlist`,");
        out.println("        ((SUM(`h`.`returned`) / SUM(`e`.`expected`)) * 100) AS `field_expected`,");
        out.println("        ((SUM(`l`.`returned`) / SUM(`e`.`expected`)) * 100) AS `freshlist_expected`");
        out.println("    FROM");
        printSubTable(tm, "aux_household_returned", "returned", upTo, out);
        out.println("        `h`");
        vsCounter = 0;
        for (int i = 0; i < upTo; i++) {
            Item item = ea.get(i);
            if (eaDescription.get(i) != null && !eaDescription.get(i).isEmpty()) {
                String vs = "`vs" + (vsCounter++) + "`";
                out.println("        JOIN " + schema + "." + eaDescription.get(i) + " " + vs + " ON " + vs + ".`id` = `h`.`" + item.getName() + "`");
            }
        }
        out.println("        JOIN ");
        printSubTable(tm, "aux_listing_returned", "returned", upTo, out);
        out.println("            `l` ON");
        out.println("            (`h`.`" + ea.get(0).getName() + "` = `l`.`" + ea.get(0).getName() + "`)");
        for (int i = 1; i < upTo; i++) {
            Item item = ea.get(i);
            out.println("            AND (`h`.`" + item.getName() + "` = `l`.`" + item.getName() + "`)");
        }
        out.println("        JOIN");
        printSubTable(tm, "aux_ea_expected", "expected", upTo, out);
        out.println("        `e` ON");
        out.println("            (`h`.`" + ea.get(0).getName() + "` = `e`.`" + ea.get(0).getName() + "`)");
        for (int i = 1; i < upTo; i++) {
            Item item = ea.get(i);
            out.println("            AND (`h`.`" + item.getName() + "` = `e`.`" + item.getName() + "`)");
        }
        out.println("            GROUP BY `name`;");
        out.println();
    }

    private static void printSubTable(TemplateManager tm, String tableName, String columnName, int upTo, PrintStream out) {
        String schema = tm.getDictionary().getSchema();
        List<Item> ea = tm.getEa();

        out.println("        (SELECT");
        for (int i = 0; i < upTo; i++) {
            Item item = ea.get(i);
            out.println("                `" + tableName + "`." + item.getName() + " AS " + item.getName() + ",");
        }
        out.println("            SUM(`" + tableName + "`.`" + columnName + "`) AS `" + columnName + "`");
        out.println("            FROM `" + schema + "`.`" + tableName + "`");
        out.println("            GROUP BY");
        out.print("                `" + tableName + "`." + ea.get(0).getName());
        for (int i = 0; i < upTo; i++) {
            Item item = ea.get(i);
            out.print(",\n                `" + tableName + "`." + item.getName());
        }
        out.println();
        out.println("            )");
    }
}
