package cspro2sql.writer;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import cspro2sql.bean.ValueSet;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class writes the SQL scripts to create a MySQL DB schema parsing the
 * data structure representing the CSPro Dictionary
 *
 * @author Istat Cooperation Unit
 */
public class SchemaWriter {

    public static void write(String schema, boolean foreignKeys, Dictionary dictionary, PrintStream ps) {
        ps.println("CREATE SCHEMA IF NOT EXISTS " + schema + ";");
        ps.println();
        ps.println("USE " + schema + ";");
        ps.println();

        printSystemTables(schema, ps);

        for (Record record : dictionary.getRecords()) {
            for (Item item : record.getItems()) {
                printValueSet(schema, item, ps);
            }
        }

        for (Record record : dictionary.getRecords()) {
            ps.println("CREATE TABLE " + schema + "." + record.getTableName() + " (");
            ps.println("    ID INT(9) UNSIGNED AUTO_INCREMENT,");
            if (!record.isMainRecord()) {
                ps.println("    " + record.getMainRecord().getName() + " INT(9) UNSIGNED NOT NULL,");
                ps.println("    COUNTER INT(9) UNSIGNED NOT NULL,");
            }
            for (Item item : record.getItems()) {
                printItem(schema, foreignKeys, item, ps);
            }
            if (!record.isMainRecord()) {
                ps.println("    INDEX (" + record.getMainRecord().getName() + "),");
                ps.println("    FOREIGN KEY (" + record.getMainRecord().getName() + ") REFERENCES " + schema + "." + record.getMainRecord().getTableName() + "(id),");
            }
            ps.println("    PRIMARY KEY (ID)");
            ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
            ps.println();
        }
    }

    private static void printItem(String schema, boolean foreignKeys, Item item, PrintStream ps) {
        String name = item.getName();
        int length = item.getLength();
        switch (item.getDataType()) {
            case Dictionary.ITEM_ALPHA:
                ps.println("    " + name + " CHAR(" + length + "),");
                break;
            case Dictionary.ITEM_DECIMAL:
                ps.println("    " + name + " INT(" + length + "),");
                break;
        }
        if (foreignKeys && item.hasValueSets()) {
            ps.println("    FOREIGN KEY (" + name + ") REFERENCES " + schema + "." + item.getValueSetName() + "(ID),");
        }
        for (Item subItem : item.getSubItems()) {
            printItem(schema, foreignKeys, subItem, ps);
        }
    }

    private static void printValueSet(String schema, Item item, PrintStream ps) {
        if (item.hasValueSets() && item.getValueSets().get(0).isNotCreated()) {
            ps.println("CREATE TABLE " + schema + "." + item.getValueSetName() + " (");
            switch (item.getDataType()) {
                case Dictionary.ITEM_ALPHA:
                    ps.println("    ID CHAR(" + item.getLength() + "),");
                    break;
                case Dictionary.ITEM_DECIMAL:
                    ps.println("    ID INT(" + item.getLength() + "),");
                    break;
            }
            ps.println("    VALUE CHAR(" + item.getValueSetsValueLength() + "),");
            ps.println("    PRIMARY KEY (ID)");
            ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
            ps.println();
            boolean first = true;
            Set<String> keys = new HashSet<>();
            ps.println("INSERT INTO " + schema + "." + item.getValueSetName() + "(ID,VALUE) VALUES ");
            for (int i = 0; i < item.getValueSets().size(); i++) {
                ValueSet valueSet = item.getValueSets().get(i);
                valueSet.setCreated();
                for (Map.Entry<String, String> e : valueSet.getValues().entrySet()) {
                    if (keys.contains(e.getKey())) {
                        continue;
                    }
                    if (!first) {
                        ps.println(",");
                    }
                    ps.print("    (\"" + e.getKey() + "\",\"" + e.getValue() + "\")");
                    keys.add(e.getKey());
                    first = false;
                }
            }
            ps.println(";");
            ps.println();
        }
        for (Item subItem : item.getSubItems()) {
            printValueSet(schema, subItem, ps);
        }
    }

    private static void printSystemTables(String schema, PrintStream ps) {
        ps.println("CREATE TABLE IF NOT EXISTS " + schema + ".CSPRO2SQL_DICTIONARY (");
        ps.println("    `ID` int(11) NOT NULL AUTO_INCREMENT,");
        ps.println("    `NAME` varchar(200) NOT NULL,");
        ps.println("    `REVISION` int(11) NOT NULL DEFAULT '0',");
        ps.println("    PRIMARY KEY (`ID`)");
        ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        ps.println();
        ps.println("CREATE TABLE IF NOT EXISTS " + schema + ".CSPRO2SQL_ERRORS (");
        ps.println("    `ID` int(11) NOT NULL AUTO_INCREMENT,");
        ps.println("    `DICTIONARY` int(11) NOT NULL,");
        ps.println("    `ERROR` longtext COLLATE utf8mb4_unicode_ci NOT NULL,");
        ps.println("    `DATE` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,");
        ps.println("    `CSPRO_GUID` binary(16) NOT NULL,");
        ps.println("    `QUESTIONNAIRE` longtext COLLATE utf8mb4_unicode_ci NOT NULL,");
        ps.println("    `SQL_SCRIPT` longtext COLLATE utf8mb4_unicode_ci NOT NULL,");
        ps.println("    PRIMARY KEY (`ID`),");
        ps.println("    KEY `dictionary_idx` (`DICTIONARY`),");
        ps.println("    CONSTRAINT `dictionary` FOREIGN KEY (`DICTIONARY`) REFERENCES " + schema + ".`cspro2sql_dictionary` (`ID`) ON DELETE NO ACTION ON UPDATE NO ACTION");
        ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        ps.println();
    }

}
