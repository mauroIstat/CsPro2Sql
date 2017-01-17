package cspro2sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

public class MonitorEngine {

    static boolean execute(String propertiesFile, String schema, PrintStream out) {
        Properties prop = new Properties();
        boolean isLocalFile = new File(propertiesFile).exists();
        //Load property file
        try (InputStream in
                = (isLocalFile
                        ? new FileInputStream(propertiesFile)
                        : LoaderEngine.class.getResourceAsStream(propertiesFile))) {
            prop.load(in);
        } catch (IOException ex) {
        }
        String individualTable = prop.getProperty("table.individual");
        String individualSex = prop.getProperty("column.individual.sex");
        String individualAge = prop.getProperty("column.individual.age");
        String sexMale = prop.getProperty("value.individual.sex.male");
        String sexFemale = prop.getProperty("value.individual.sex.female");
        
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

        out.println("CREATE VIEW " + schema + ".`r_individual_count` AS\n"
                + "    SELECT COUNT(0) AS `individual`\n"
                + "    FROM " + schema + "." + individualTable + ";");
        out.println();

        out.println("CREATE VIEW " + schema + ".`r_questionnaire_total` AS\n"
                + "    SELECT COUNT(0) AS `total`\n"
                + "    FROM " + schema + "." + prop.getProperty("table.listing_questionnaires") + ";");
        out.println();

        out.println("CREATE VIEW " + schema + ".`r_questionnaire_returned` AS\n"
                + "    SELECT COUNT(0) AS `returned`\n"
                + "    FROM " + schema + "." + prop.getProperty("table.returned_questionnaires") + ";");
        out.println();

        out.println("CREATE VIEW " + schema + ".`r_sex_by_age` AS\n"
                + "    SELECT \n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 0)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 4))) AS `MALE_0_4`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 0)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 4))) AS `FEMALE_0_4`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 5)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 10))) AS `MALE_5_10`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 5)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 10))) AS `FEMALE_5_10`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 11)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 16))) AS `MALE_11_16`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 11)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 16))) AS `FEMALE_11_16`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 17)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 25))) AS `MALE_17_25`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 17)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 25))) AS `FEMALE_17_25`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 26)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 34))) AS `MALE_26_34`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 26)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 34))) AS `FEMALE_26_34`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 35)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 45))) AS `MALE_35_45`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 35)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 45))) AS `FEMALE_35_45`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 46)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 55))) AS `MALE_46_55`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 46)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 55))) AS `FEMALE_46_55`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 56)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 65))) AS `MALE_56_65`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 56)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 65))) AS `FEMALE_56_65`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 66)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 75))) AS `MALE_66_75`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 66)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 75))) AS `FEMALE_66_75`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexMale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 76)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 99))) AS `MALE_76_99`,\n"
                + "        (SELECT \n"
                + "                COUNT(0)\n"
                + "            FROM\n"
                + "                " + schema + "." + individualTable + "\n"
                + "            WHERE\n"
                + "                ((" + schema + "." + individualTable + "." + individualSex + " = " + sexFemale + ")\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " >= 76)\n"
                + "                    AND (" + schema + "." + individualTable + "." + individualAge + " <= 99))) AS `FEMALE_76_99`;");
        out.println();

        return false;
    }

}
