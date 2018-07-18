package cspro2sql.writer;

import cspro2sql.bean.AreaNameFile;
import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.NamedArea;
import cspro2sql.bean.Record;
import cspro2sql.bean.Territory;
import cspro2sql.bean.TerritoryItem;
import cspro2sql.sql.TemplateManager;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Formatter;
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
 * @version 0.9.18.1
 */
public class MonitorWriter {

    private static final String[] TEMPLATES = new String[]{
        "r_questionnaire_info",
        "r_individual_info",
        "r_religion",
        "r_sex_by_age",
        "r_first_level_geography",
        "r_sex_by_age_group_region"
    };

    private static int reportCount = 0;

    public static boolean write(TemplateManager tm, TemplateManager tmListing, 
            TemplateManager tmExpected, TemplateManager tmEaStatus, 
            AreaNameFile areaNames, boolean gisEnabled, PrintStream out) {
        
        String schema = tm.getDictionary().getSchema();

        tm.addParam("@LISTING", tmListing == null ? "0" : "1");
        tm.addParam("@EXPECTED", tmExpected == null ? "0" : "1");
        tm.addParam("@EA_STATUS", tmEaStatus == null ? "0" : "1");
        tm.addParam("@GIS", gisEnabled ? "1" : "0");
        
        out.println("USE " + schema + ";");
        out.println();

        Territory territory = tm.getTerritory();
        int[] ageRange = tm.getAgeRange();

        if (!territory.isEmpty()) {
            try {
                printTerritoryTable(territory, areaNames, out);
                
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                return false;
            }
        }
               
        try {
            tm.printTemplate("c_user", out);
            tm.printTemplate("cspro2sql_report", out);
            tm.printTemplate("dashboard_info", out);
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

        if (!territory.isEmpty()) {
        try {
            if (tm.printTemplate("r_questionnaire_info_region", out))
                printMaterialized(schema, "r_questionnaire_info_region", out);
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                return false;
            }
        }

        if (!territory.isEmpty()) {
            TerritoryItem firstTerritoryItem = territory.getFirst();
            out.println("CREATE OR REPLACE VIEW " + schema + ".`r_regional_area` AS");
            String groupBy = firstTerritoryItem.getItemName();
            String name = firstTerritoryItem.getName();
            String fromClause = territory.getFromClause();

            out.print("  SELECT '" + name + "' name, COUNT(0) value FROM (SELECT COUNT(0) FROM " + fromClause + " GROUP BY " + groupBy + ") a0");
            for (int i = 1; i < territory.size(); i++) {
                TerritoryItem territoryItem = territory.get(i);
                name = territoryItem.getName();
                groupBy += "," + territoryItem.getItemName();
                out.println(" UNION");
                out.print("  SELECT '" + name + "', COUNT(0) FROM (SELECT COUNT(0) FROM " + fromClause + " GROUP BY " + groupBy + ") a" + i);
            }
            out.println();
            out.println(";");
            printMaterialized(schema, "r_regional_area", out);
        }

        if (ageRange != null && tm.hasParam("@INDIVIDUAL_TABLE") && tm.hasParam("@INDIVIDUAL_COLUMN_SEX")
                && tm.hasParam("@INDIVIDUAL_VALUE_SEX_MALE") && tm.hasParam("@INDIVIDUAL_VALUE_SEX_FEMALE")
                && tm.hasParam("@INDIVIDUAL_COLUMN_AGE")) {
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

        if (!territory.isEmpty()) {
            TerritoryItem territoryItem = territory.getFirst();
            out.println("CREATE OR REPLACE VIEW " + schema + ".`r_household_by_ea` AS");
            out.print("  SELECT concat(");
            out.print("'" + territoryItem.getName() + "'");
            for (int i = 1; i < territory.size(); i++) {
                territoryItem = territory.get(i);
                out.print(",'#','" + territoryItem.getName() + "'");
            }
            out.println(") as name, null as household");
            out.println("  UNION");
            out.print("  SELECT concat(");
            territoryItem = territory.getFirst();
            out.print(territoryItem.selectDescription());
            for (int i = 1; i < territory.size(); i++) {
                territoryItem = territory.get(i);
                out.print(",'#'," + territoryItem.selectDescription());
            }
            out.println(") as name, COUNT(0) AS `household`");
            out.print("  FROM (SELECT " + territory.getFirst().getItem().getColunmFullName());
            for (int i = 1; i < territory.size(); i++) {
                out.print(", " + territory.get(i).getItem().getColunmFullName());
            }
            
            out.println(" FROM " + territory.getFromClause() + ") `h`");
            out.print("  GROUP BY ");
            territoryItem = territory.getFirst();
            out.print("`h`." + territoryItem.getItemName());
            for (int i = 1; i < territory.size(); i++) {
                territoryItem = territory.get(i);
                out.print(", `h`." + territoryItem.getItemName());
            }
            out.println(";");
            printMaterialized(schema, "r_household_by_ea", out);
        }

        if (!territory.isEmpty()) {
            try {
                printAuxTable(tm, tm, "aux_household_returned", "returned", out);
                printAuxTable(tm, (tmListing != null) ? tmListing : tm, "aux_listing_returned", "returned", out);
                printAuxTable(tm, (tmExpected != null) ? tmExpected : tm, "aux_household_expected", "expected", out);

                boolean createEaTables = tmEaStatus != null && tmExpected != null && tmEaStatus.hasParam("@EA_STATUS_VALUE_COMPLETE");
                
                if (createEaTables) {
                    printAuxEaStatusTable(tm, tmExpected, "aux_ea_expected", "expected", out);
                    printAuxEaStatusTable(tm, tmEaStatus, "aux_ea_completed", "completed", out);
                }

                for (int i = 0; i < territory.size(); i++) {
                    final int upTo = i + 1;
                
                    TerritoryItem territoryItem = territory.get(i);
                    printExpectedReport(tm, "r_household_expected_by_" + territoryItem.getName().toLowerCase(), upTo, out);
                    printMaterialized(schema, "r_household_expected_by_" + territoryItem.getName().toLowerCase(), out);
                    
                    if (createEaTables && i < territory.size() - 1) {
                        printExpectedEaReport(tm, "r_ea_expected_by_" + territoryItem.getName().toLowerCase(), upTo, out);
                        printMaterialized(schema, "r_ea_expected_by_" + territoryItem.getName().toLowerCase(), out);
                    }
                }

                printTotalReport(tm, (tmListing != null) ? tmListing : tm, out);
                printMaterialized(schema, "r_total", out);
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                return false;
            }
        }
                
        return true;
    }

    private static void printTerritoryTable(Territory territory, AreaNameFile areaNames, PrintStream out) throws IOException {
        out.println("CREATE TABLE IF NOT EXISTS `territory` (");
        String idx = "";
        for (int i = 0; i < territory.size(); i++) {
            TerritoryItem territoryItem = territory.get(i);
            String name = territoryItem.getItemName();
            out.println("    `" + name + "_NAME` text COLLATE utf8mb4_unicode_ci,");
            out.println("    `" + name + "` int(11) DEFAULT NULL,");
            if (i > 0) {
                idx += ",";
            }
            idx += "`" + name + "`";
        }
        out.println("    `TERRITORY_CODE` text COLLATE utf8mb4_unicode_ci,");
        out.println("    KEY `idx_territory` (" + idx + ")");
        out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        out.println();
        
        if (areaNames != null) {
            final int numLevels = areaNames.getLevels().length;
            if (territory.size() != numLevels) {
                throw new IOException("Area names file does not match #territory items in dictionary");
            }
            
            out.println("TRUNCATE TABLE `territory`;");
            
            StringBuilder insertPreambleBuilder = new StringBuilder();
            insertPreambleBuilder.append("INSERT INTO `territory` (");
            for (int i = 0; i < territory.size(); i++) {
                TerritoryItem territoryItem = territory.get(i);
                String name = territoryItem.getItemName();
                insertPreambleBuilder.append("`");
                insertPreambleBuilder.append(name);
                insertPreambleBuilder.append("_NAME`,`");
                insertPreambleBuilder.append(name);
                insertPreambleBuilder.append("`");
                if (i != territory.size() - 1)
                    insertPreambleBuilder.append(",");
            }
            insertPreambleBuilder.append(",`TERRITORY_CODE`) VALUES (");
            final String insertPreamble = insertPreambleBuilder.toString();
            String[] formatSpecs = new String[numLevels];
            for (int i = 0; i < numLevels; i++) {
                Item territoryDictItem = territory.get(i).getItem();
                if (territoryDictItem.isZeroFill())
                    formatSpecs[i] = "%0" + territoryDictItem.getLength() + "d";
                else
                    formatSpecs[i] = "%" + territoryDictItem.getLength() + "d";
            }
            for (NamedArea area : areaNames.getAreas()) {
                
                // Ignore higher level entities (province, district)
                // only insert lowest level entities (EA)
                if (area.level() == numLevels) {
                    out.print(insertPreamble);
                    for (int i = 0; i < numLevels - 1; i++) {
                        List<Integer> ancestorCode = area.getCodes().subList(0, i + 1);
                        NamedArea ancestor = areaNames.lookup(ancestorCode);
                        String ancestorName = ancestor != null ? ancestor.getName().replace("'", "''") : String.valueOf(area.getCodes().get(i));
                        out.print("'" + ancestorName + "'," + area.getCodes().get(i) +  ",");
                    }
                    
                    StringBuilder fullCodeBuilder = new StringBuilder();
                    Formatter fmt = new Formatter(fullCodeBuilder);
                    for (int i = 0; i < numLevels; i++) {
                        fmt.format(formatSpecs[i], area.getCodes().get(i));
                    }
                    
                    out.println("'" + area.getName().replace("'", "''")   + "'," + area.getCodes().get(numLevels - 1) +  ",'" + fullCodeBuilder.toString() + "');");
                }
            }
        }
    }
    
    private static void printMaterialized(String schema, String name, PrintStream out) {
        out.println("DROP TABLE IF EXISTS " + schema + ".m" + name + ";");
        out.println("SELECT 0 INTO @ID;");
        out.println("CREATE TABLE " + schema + ".m" + name + " (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, " + name + ".* FROM " + schema + "." + name + ";");
        out.println("INSERT INTO " + schema + ".`cspro2sql_report` VALUES ('" + name + "', " + (reportCount++) + ");");
        out.println();
    }

    private static void printAuxTable(TemplateManager mainTm, TemplateManager tm, String auxName, String columnName, PrintStream out) throws IOException {
        Set<Record> records = new LinkedHashSet<>();
        Territory territory = tm.getTerritory();
        Territory mainTerritory = mainTm.getTerritory();
        if (territory.size() != mainTerritory.size()) {
            throw new IOException("Number of territories in " + tm.getDictionary().getName() + 
                    " is different from number of territories in " + 
                    mainTm.getDictionary().getName() +
                    ". Fix #territory tags in dictionaries so that they match");            
        }
        out.println("CREATE OR REPLACE VIEW " + tm.getDictionary().getSchema() + "." + auxName + " AS");
        out.println("    SELECT ");
        for (int i = 0; i < territory.size(); i++) {
            TerritoryItem territoryItem = territory.get(i);
            TerritoryItem mainTerritoryItem = mainTerritory.get(i);
            Item item = territoryItem.getItem();
            out.println("        " + item.getColunmFullName() + " AS " + mainTerritoryItem.getItemName() + ",");
            records.add(item.getRecord());
        }
        Item expected = tm.getDictionary().getTaggedItem(Dictionary.TAG_EXPECTED_QUESTIONNAIRES);
        if (expected == null) {
            out.println("        COUNT(0) AS `" + columnName + "`");
        } else {
            out.println("        SUM(" + expected.getColunmFullName() + ") AS `" + columnName + "`");
            records.add(expected.getRecord());
        }
        Record[] recArray = records.toArray(new Record[0]);
        out.println("    FROM");
        out.println("        " + recArray[0].getMainRecord().getFullTableName());
        for (Record record : recArray) {
            if (!record.isMainRecord()) {
                out.println("            JOIN " + record.getFullTableName() + " ON " + record.getMainRecord().getTableName() + ".ID = " + record.getTableName() + "." + record.getMainRecord().getName());
            }
        }
        out.println("    WHERE");
        for (int i = 0; i < territory.size() - 1; i++) {
            Item item = territory.get(i).getItem();
            out.println("        " + item.getColunmFullName() + " IS NOT NULL AND");
        }
        out.println("        " + territory.get(territory.size() - 1).getItem().getColunmFullName() + " IS NOT NULL");
        out.println("    GROUP BY");
        for (int i = 0; i < territory.size() - 1; i++) {
            Item item = territory.get(i).getItem();
            out.println("        " + item.getColunmFullName() + ",");
        }
        Item item = territory.get(territory.size() - 1).getItem();
        out.println("        " + item.getColunmFullName() + ";");
        out.println();
    }

    private static void printAuxEaStatusTable(TemplateManager mainTm, TemplateManager tm, String auxName, String columnName, PrintStream out) throws IOException {
        Set<Record> records = new LinkedHashSet<>();
        Territory territory = tm.getTerritory();
        Territory mainTerritory = mainTm.getTerritory();
        if (territory.size() != mainTerritory.size()) {
            throw new IOException("Number of territories in " + tm.getDictionary().getName() + 
                    " is different from number of territories in " + 
                    mainTm.getDictionary().getName() +
                    ". Fix #territory tags in dictionaries so that they match");            
        }
        out.println("CREATE OR REPLACE VIEW " + tm.getDictionary().getSchema() + "." + auxName + " AS");
        out.println("    SELECT ");
        for (int i = 0; i < territory.size() - 1; i++) {
            TerritoryItem territoryItem = territory.get(i);
            TerritoryItem mainTerritoryItem = mainTerritory.get(i);
            Item item = territoryItem.getItem();
            out.println("        " + item.getColunmFullName() + " AS " + mainTerritoryItem.getItemName() + ",");
            records.add(item.getRecord());
        }
        Item expected = tm.getDictionary().getTaggedItem(Dictionary.TAG_EXPECTED_QUESTIONNAIRES);
        Item completed = tm.getDictionary().getTaggedItem(Dictionary.TAG_EA_COMPLETION_STATUS);
        
        if (expected  != null) {
            out.println("        COUNT(0) AS `" + columnName + "`");
            records.add(expected.getRecord());
        } else if (completed != null) {
            out.println("        SUM(" + completed.getColunmFullName() + " = " + tm.getParam("@EA_STATUS_VALUE_COMPLETE") + ") AS `" + columnName + "`");
            records.add(completed.getRecord());
        }
        Record[] recArray = records.toArray(new Record[0]);
        out.println("    FROM");
        out.println("        " + recArray[0].getMainRecord().getFullTableName());
        for (Record record : recArray) {
            if (!record.isMainRecord()) {
                out.println("            JOIN " + record.getFullTableName() + " ON " + record.getMainRecord().getTableName() + ".ID = " + record.getTableName() + "." + record.getMainRecord().getName());
            }
        }
        out.println("    WHERE");
        for (int i = 0; i < territory.size() - 1; i++) {
            Item item = territory.get(i).getItem();
            out.println("        " + item.getColunmFullName() + " IS NOT NULL AND");
        }
        out.println("        " + territory.get(territory.size() - 1).getItem().getColunmFullName() + " IS NOT NULL");
        out.println("    GROUP BY");
        for (int i = 0; i < territory.size() - 2; i++) {
            Item item = territory.get(i).getItem();
            out.println("        " + item.getColunmFullName() + ",");
        }
        Item item = territory.get(territory.size() - 2).getItem();
        out.println("        " + item.getColunmFullName() + ";");
        out.println();
    }
    
    private static void printExpectedReport(TemplateManager tm, String reportName, int upTo, PrintStream out) {
        String schema = tm.getDictionary().getSchema();
        Territory territory = tm.getTerritory();

        out.println("CREATE OR REPLACE VIEW " + schema + ".`" + reportName + "` AS");
        out.println("    SELECT ");
        out.print("        _utf8mb4 '" + territory.getFirst().getName());
        for (int i = 1; i < upTo; i++) {
            out.print("#" + territory.get(i).getName());
        }
        out.println("' COLLATE utf8mb4_unicode_ci AS `name`,");
        for (int i = 0; i < upTo; i++) {
            out.println("        NULL AS `" + territory.get(i).getItemName() + "`,");
        }
        out.println("        NULL AS `field`,");
        out.println("        NULL AS `freshlist`,");
        out.println("        NULL AS `expected`,");
        out.println("        NULL AS `field_freshlist`,");
        out.println("        NULL AS `field_expected`,");
        out.println("        NULL AS `freshlist_expected`");
        out.println("    ");
        out.println("    UNION SELECT ");
        out.print("        CONCAT(");
        TerritoryItem territoryItem = territory.getFirst();
        out.print(territoryItem.selectDescription());
        for (int i = 1; i < upTo; i++) {
            territoryItem = territory.get(i);
            out.print(",'#'," + territoryItem.selectDescription());
        }
        out.println(") AS `name`,");
        for (int i = 0; i < upTo; i++) {
            out.println("        `h`." + territory.get(i).getItemName() + " AS `" + territory.get(i).getItemName() + "`,");
        }
        out.println("        SUM(`h`.`returned`) AS `returned`,");
        out.println("        SUM(`l`.`returned`) AS `returned`,");
        out.println("        SUM(`e`.`expected`) AS `expected`,");
        out.println("        ((SUM(`h`.`returned`) / SUM(`l`.`returned`)) * 100) AS `field_freshlist`,");
        out.println("        ((SUM(`h`.`returned`) / SUM(`e`.`expected`)) * 100) AS `field_expected`,");
        out.println("        ((SUM(`l`.`returned`) / SUM(`e`.`expected`)) * 100) AS `freshlist_expected`");
        out.println("    FROM");
        printSubTable(tm, "aux_household_returned", "returned", upTo, out);
        out.println("        `h`");
        out.println("        JOIN ");
        printSubTable(tm, "aux_listing_returned", "returned", upTo, out);
        out.println("            `l` ON");
        territoryItem = territory.getFirst();
        out.println("            (`h`.`" + territoryItem.getItemName() + "` = `l`.`" + territoryItem.getItemName() + "`)");
        for (int i = 1; i < upTo; i++) {
            territoryItem = territory.get(i);
            out.println("            AND (`h`.`" + territoryItem.getItemName() + "` = `l`.`" + territoryItem.getItemName() + "`)");
        }
        out.println("        JOIN");
        printSubTable(tm, "aux_household_expected", "expected", upTo, out);
        out.println("        `e` ON");
        territoryItem = territory.getFirst();
        out.println("            (`h`.`" + territoryItem.getItemName() + "` = `e`.`" + territoryItem.getItemName() + "`)");
        for (int i = 1; i < upTo; i++) {
            territoryItem = territory.get(i);
            out.println("            AND (`h`.`" + territoryItem.getItemName() + "` = `e`.`" + territoryItem.getItemName() + "`)");
        }
        out.print("    GROUP BY `name`");
        for (int i = 1; i < upTo; i++) {
            out.print(", `h`.`" + territory.get(i).getItemName() + "`");
        }
        out.println(";");
        out.println();
    }

    private static void printExpectedEaReport(TemplateManager tm, String reportName, int upTo, PrintStream out) {
        String schema = tm.getDictionary().getSchema();
        Territory territory = tm.getTerritory();

        out.println("CREATE OR REPLACE VIEW " + schema + ".`" + reportName + "` AS");
        out.println("    SELECT ");
        out.print("        _utf8mb4 '" + territory.getFirst().getName());
        for (int i = 1; i < upTo; i++) {
            out.print("#" + territory.get(i).getName());
        }
        out.println("' COLLATE utf8mb4_unicode_ci AS `name`,");
        for (int i = 0; i < upTo; i++) {
            out.println("        NULL AS `" + territory.get(i).getItemName() + "`,");
        }
        out.println("        NULL AS `completed`,");
        out.println("        NULL AS `expected`,");
        out.println("        NULL AS `completed_expected`");
        out.println("    ");
        out.println("    UNION SELECT ");
        out.print("        CONCAT(");
        TerritoryItem territoryItem = territory.getFirst();
        out.print(territoryItem.selectDescription());
        for (int i = 1; i < upTo; i++) {
            territoryItem = territory.get(i);
            out.print(",'#'," + territoryItem.selectDescription());
        }
        out.println(") AS `name`,");
        for (int i = 0; i < upTo; i++) {
            out.println("        `h`." + territory.get(i).getItemName() + " AS `" + territory.get(i).getItemName() + "`,");
        }
        out.println("        COALESCE((`c`.`completed`), 0) AS `completed`,");
        out.println("        SUM(`h`.`expected`) AS `expected`,");
        out.println("        ((COALESCE((`c`.`completed`), 0) / SUM(`h`.`expected`)) * 100) AS `completed_expected`");
        out.println("    FROM");
        printSubTable(tm, "aux_ea_expected", "expected", upTo, out);
        out.println("        `h`");
        out.println("        LEFT JOIN ");
        printSubTable(tm, "aux_ea_completed", "completed", upTo, out);
        out.println("        `c` ON");
        territoryItem = territory.getFirst();
        out.println("            (`h`.`" + territoryItem.getItemName() + "` = `c`.`" + territoryItem.getItemName() + "`)");
        for (int i = 1; i < upTo; i++) {
            territoryItem = territory.get(i);
            out.println("            AND (`h`.`" + territoryItem.getItemName() + "` = `c`.`" + territoryItem.getItemName() + "`)");
        }
        out.print("    GROUP BY `name`");
        for (int i = 1; i < upTo; i++) {
            out.print(", `h`.`" + territory.get(i).getItemName() + "`");
        }
        out.println(";");
        out.println();
    }
    
    private static void printSubTable(TemplateManager tm, String tableName, String columnName, int upTo, PrintStream out) {
        String schema = tm.getDictionary().getSchema();
        Territory territory = tm.getTerritory();

        out.println("        (SELECT");
        for (int i = 0; i < upTo && i < territory.size(); i++) {
            TerritoryItem territoryItem = territory.get(i);
            Item item = territoryItem.getItem();
            out.println("                `" + tableName + "`." + item.getName() + " AS " + item.getName() + ",");
        }
        out.println("            SUM(`" + tableName + "`.`" + columnName + "`) AS `" + columnName + "`");
        out.println("            FROM `" + schema + "`.`" + tableName + "`");
        out.println("            GROUP BY");
        out.print("                `" + tableName + "`." + territory.getFirst().getItemName());
        for (int i = 1; i < upTo && i < territory.size(); i++) {
            TerritoryItem territoryItem = territory.get(i);
            out.print(",\n                `" + tableName + "`." + territoryItem.getItemName());
        }
        out.println();
        out.println("            )");
    }

    private static void printTotalReport(TemplateManager tm, TemplateManager tmListing, PrintStream out) {
        String schema = tm.getDictionary().getSchema();

        out.println("CREATE OR REPLACE VIEW `" + schema + "`.`r_total` AS");
        out.println("    SELECT ");
        out.println("        (SELECT ");
        out.println("                COUNT(0)");
        out.println("            FROM");
        printSubTable(tm, "aux_household_returned", "returned", 1000, out);
        out.println("            `a`) AS `ea_fieldwork`,");
        out.println("        (SELECT ");
        out.println("                COUNT(0)");
        out.println("            FROM");
        printSubTable(tm, "aux_listing_returned", "returned", 1000, out);
        out.println("            `a`) AS `ea_freshlist`,");
	out.println("        (SELECT SUM(completed) FROM aux_ea_completed) AS `ea_completed`,");
        out.println("        (SELECT ");
        out.println("                COUNT(0)");
        out.println("            FROM");
        printSubTable(tm, "aux_household_expected", "expected", 1000, out);
        out.println("            `a`) AS `ea_expected`,");
        out.println("        (SELECT ");
        out.println("                COUNT(0)");
        out.println("            FROM");
        out.println("                " + tm.getDictionary().getMainRecord().getFullTableName() + ") AS `household_fieldwork`,");
        out.println("        (SELECT ");
        out.println("                COUNT(0)");
        out.println("            FROM");
        out.println("                " + tmListing.getDictionary().getMainRecord().getFullTableName() + ") AS `household_freshlist`,");
        out.println("        (SELECT ");
        out.println("                SUM(`a`.`expected`)");
        out.println("            FROM");
        printSubTable(tm, "aux_household_expected", "expected", 1000, out);
        out.println("                `a`) AS `household_expected`;");
    }

}
