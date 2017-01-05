
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

    public static void write(String schema, Dictionary dictionary, PrintStream ps) {
        ps.println("CREATE SCHEMA " + schema + ";");
        ps.println();
        
        printSystemTables(schema,ps);

        for (Record record : dictionary.getRecords()) {
            for (Item item : record.getItems()) {
                printValueSet(schema,item,ps);
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
                printItem(schema,item,ps);
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

    private static void printItem(String schema, Item item, PrintStream ps) {
        String name = item.getName();
        int length = item.getLength();
        switch (item.getDataType()) {
            case Dictionary.ITEM_ALPHA:
                ps.println("    " + name + " CHAR(" + length + "),");
                break;
            case "Number":
                ps.println("    " + name + " INT(" + length + "),");
                break;
        }
        if (item.hasValueSets()) {
            ps.println("    FOREIGN KEY (" + name + ") REFERENCES " + schema + "." + item.getValueSetName() + "(ID),");
        }
        for (Item subItem : item.getSubItems()) {
            printItem(schema,subItem,ps);
        }
    }
    
    private static void printValueSet(String schema, Item item, PrintStream ps) {
        if (item.hasValueSets()) {
            ps.println("CREATE TABLE " + schema + "." + item.getValueSetName() + " (");
            switch (item.getDataType()) {
                case Dictionary.ITEM_ALPHA:
                    ps.println("    ID CHAR(" + item.getLength()+ "),");
                    break;
                case "Number":
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
            for (int i=0; i<item.getValueSets().size(); i++) {
                ValueSet valueSet = item.getValueSets().get(i);
                int j=0;
                for (Map.Entry<String, String> e : valueSet.getValues().entrySet()) {
                	if (keys.contains(e.getKey())) {
                		continue;
                	}
                	if (!first) {
                    	ps.println(",");
                	}
                    ps.print("    (" + e.getKey() + ",\"" + e.getValue() + "\")");
            		keys.add(e.getKey());
                    first = false;
                }
            }
            ps.println(";");
            ps.println();
        }
        for (Item subItem : item.getSubItems()) {
            printValueSet(schema,subItem,ps);
        }
    }
    
    private static void printSystemTables(String schema, PrintStream ps) {
        ps.println("CREATE TABLE " + schema + ".CSPRO2SQL_LASTUPDATE (");
        ps.println("    LAST_UPDATE TIMESTAMP NULL DEFAULT NULL");
        ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        ps.println();
        ps.println("CREATE TABLE " + schema + ".CSPRO2SQL_ERRORS (");
        ps.println("    ID INT(11) NOT NULL AUTO_INCREMENT,");
        ps.println("    ERROR VARCHAR(2048) NOT NULL,");
        ps.println("    DATE TIMESTAMP NOT NULL,");
        ps.println("    CSPRO_GUID BINARY(16) NOT NULL,");
        ps.println("    QUESTIONNAIRE LONGTEXT NOT NULL,");
        ps.println("    PRIMARY KEY (ID)");
        ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
        ps.println();
    }

}
