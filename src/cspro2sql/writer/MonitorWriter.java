package cspro2sql.writer;

import cspro2sql.MonitorEngine;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;

/**
 *
 * @author Istat Cooperation Unit
 */
public class MonitorWriter {

    private static final String[] TEMPLATES = new String[]{
        "r_questionnaire_info",
        "r_individual_info",
        "r_religion",
        "r_sex_by_age",
        "r_sex_by_region"
    };

    public static boolean write(String schema, String[] ea, String[] eaName, int[] ageRange, Map<String, String> params, PrintStream out) {
        out.println("USE " + schema + ";");
        out.println();

        out.println("CREATE TABLE " + schema + ".`c_user` (\n"
                + "  `ID` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `FIRSTNAME` varchar(45) DEFAULT NULL,\n"
                + "  `MIDDLENAME` varchar(45) DEFAULT NULL,\n"
                + "  `LASTNAME` varchar(45) DEFAULT NULL,\n"
                + "  `EMAIL` varchar(256) DEFAULT NULL,\n"
                + "  `PASSWORD` varchar(64) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`ID`),\n"
                + "  UNIQUE KEY `EMAIL_UNIQUE` (`EMAIL`(255))\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        out.println();

        out.println("CREATE TABLE " + schema + ".`c_user_roles` (\n"
                + "  `EMAIL` varchar(256) NOT NULL,\n"
                + "  `ROLE` varchar(45) DEFAULT NULL,\n"
                + "  UNIQUE KEY `EMAIL_UNIQUE` (`EMAIL`(256), `ROLE`(45))\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        out.println();

        out.println("CREATE TABLE " + schema + ".`cspro2sql_stats` (\n"
                + "  `NAME` varchar(256) NOT NULL,\n"
                + "  PRIMARY KEY (`name`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        out.println();

        try {
            for (String template : TEMPLATES) {
                printTemplate(template, params, out);
                printMaterialized(schema, template, out);
            }
        } catch (IOException ex) {
            return false;
        }

        out.println("CREATE VIEW " + schema + ".`r_regional_area` AS");
        String groupBy = ea[0];
        String name = eaName[0];
        out.print("  SELECT '" + name + "' name, COUNT(0) value FROM (SELECT COUNT(0) FROM " + schema + "." + params.get("@QUESTIONNAIRE_TABLE") + " GROUP BY " + groupBy + ") a0");
        for (int i = 1; i < ea.length; i++) {
            name = eaName[i];
            groupBy += "," + ea[i];
            out.println(" UNION");
            out.print("  SELECT '" + name + "', COUNT(0) FROM (SELECT COUNT(0) FROM " + schema + "." + params.get("@QUESTIONNAIRE_TABLE") + " GROUP BY " + groupBy + ") a" + i);
        }
        out.println();
        out.println(";");
        printMaterialized(schema, "r_regional_area", out);

        out.println("CREATE VIEW " + schema + ".`r_sex_by_age_group` AS");
        out.print("  SELECT '" + ageRange[0] + " to " + (ageRange[1] - 1) + "' as 'range', a.male, b.female FROM "
                + "(SELECT COUNT(0) male FROM " + schema + "." + params.get("@INDIVIDUAL_TABLE") + " WHERE " + params.get("@INDIVIDUAL_COLUMN_SEX") + " = " + params.get("@INDIVIDUAL_VALUE_SEX_MALE") + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[0] + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[1] + ") a,"
                + "(SELECT COUNT(0) female FROM " + schema + "." + params.get("@INDIVIDUAL_TABLE") + " WHERE " + params.get("@INDIVIDUAL_COLUMN_SEX") + " = " + params.get("@INDIVIDUAL_VALUE_SEX_FEMALE") + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[0] + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[1] + ") b");
        for (int i = 1; i < ageRange.length - 1; i++) {
            out.println(" UNION");
            out.print("  SELECT '" + ageRange[i] + " to " + (ageRange[i + 1] - 1) + "' as 'range', a.male, b.female FROM "
                    + "(SELECT COUNT(0) male FROM " + schema + "." + params.get("@INDIVIDUAL_TABLE") + " WHERE " + params.get("@INDIVIDUAL_COLUMN_SEX") + " = " + params.get("@INDIVIDUAL_VALUE_SEX_MALE") + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[i] + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[i + 1] + ") a,"
                    + "(SELECT COUNT(0) female FROM " + schema + "." + params.get("@INDIVIDUAL_TABLE") + " WHERE " + params.get("@INDIVIDUAL_COLUMN_SEX") + " = " + params.get("@INDIVIDUAL_VALUE_SEX_FEMALE") + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " >= " + ageRange[i] + " AND " + params.get("@INDIVIDUAL_COLUMN_AGE") + " < " + ageRange[i + 1] + ") b");
        }
        out.println();
        out.println(";");
        printMaterialized(schema, "r_sex_by_age_group", out);

        out.println("CREATE VIEW " + schema + ".`r_household_by_ea` AS");
        out.print("  SELECT reg.value as " + eaName[0]);
        for (int i = 1; i < ea.length; i++) {
            out.print(",  h." + ea[i] + " as " + eaName[i]);
        }
        out.println(", COUNT(0) AS `household`");
        out.println("  FROM " + schema + "." + params.get("@QUESTIONNAIRE_TABLE") + " `h` JOIN " + schema + "." + params.get("@VALUESET_REGION") + " `reg` ON `h`.`" + params.get("@QUESTIONNAIRE_COLUMN_REGION") + "` = `reg`.`ID`");
        out.print("  GROUP BY ");
        out.print("`h`." + ea[0]);
        for (int i = 1; i < ea.length; i++) {
            out.print(", `h`." + ea[i]);
        }
        out.println();
        out.println("  ORDER BY `reg`.`VALUE`;");
        printMaterialized(schema, "r_household_by_ea", out);

        return true;
    }

    private static void printTemplate(String template, Map<String, String> params, PrintStream ps) throws IOException {
        try (InputStream in = MonitorEngine.class.getResourceAsStream("/cspro2sql/sql/" + template + ".sql")) {
            try (InputStreamReader isr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        for (Map.Entry<String, String> e : params.entrySet()) {
                            line = line.replace(e.getKey(), e.getValue());
                        }
                        ps.println(line);
                    }
                }
            }
        }
    }

    private static void printMaterialized(String schema, String name, PrintStream out) {
        out.println("SELECT @ID := 0;");
        out.println("CREATE TABLE " + schema + ".m" + name + " AS SELECT @ID := @ID + 1 ID, " + name + ".* FROM " + schema + "." + name + ";");
        out.println("INSERT INTO " + schema + ".`cspro2sql_stats` VALUES ('" + name + "');");
        out.println();
    }

}
