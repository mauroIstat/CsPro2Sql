package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MonitorEngine {

    static final String[] TEMPLATES = new String[]{
        "r_questionnaire_info",
        "r_individual_info",
        "r_religion",
        "r_sex_by_age"
    };

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream in = MonitorEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            Dictionary dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
            execute(dictionary, prop, System.out);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Dictionary dictionary, Properties prop, PrintStream out) {
        String schema = prop.getProperty("db.dest.schema");

        Map<String, String> params = new HashMap<>();
        params.put("@SCHEMA", schema);
        params.put("@QUESTIONNAIRE_TABLE", prop.getProperty("table.questionnaire"));
        params.put("@QUESTIONNAIRE_COLUMN", dictionary.getMainRecord().getName());
        params.put("@INDIVIDUAL_TABLE", prop.getProperty("table.individual"));
        params.put("@INDIVIDUAL_COLUMN_SEX", prop.getProperty("column.individual.sex"));
        params.put("@INDIVIDUAL_COLUMN_AGE", prop.getProperty("column.individual.age"));
        params.put("@INDIVIDUAL_COLUMN_RELIGION", prop.getProperty("column.individual.religion"));
        params.put("@INDIVIDUAL_VALUE_SEX_MALE", prop.getProperty("value.individual.sex.male"));
        params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", prop.getProperty("value.individual.sex.female"));
        
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

        try {
            for (String template : TEMPLATES) {
                printTemplate(template, params, out);
                out.println("SELECT @ID := 0;");
                out.println("CREATE TABLE m" + template + " AS SELECT @ID := @ID + 1 ID, " + template + ".* FROM " + template + ";");
                out.println();
            }
        } catch (IOException ex) {
            return false;
        }

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

}
