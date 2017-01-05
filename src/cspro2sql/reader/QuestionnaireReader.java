
package cspro2sql.reader;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class parse a plain text CSPro questionnaire
 * 
 * @author Istat Cooperation Unit
 */
public class QuestionnaireReader {

    //Parse a questionnaire (CSPro plain text file) according to its dictionary 
    public static Map<Record, List<List<String>>> parse(Dictionary dictionary, String questionnaire) {
        Map<Record, List<List<String>>> result = new LinkedHashMap<>();
        String[] rows = questionnaire.split(Dictionary.DICT_NEWLINE_REGEXP);
        Record record = dictionary.getMainRecord();
        List<List<String>> valuesList = new LinkedList<>();
        result.put(record, valuesList);
        List<String> values = new LinkedList<>();
        valuesList.add(values);
        for (Item item : record.getItems()) {
            parseItem(item, rows[0], values);
        }
        for (String row : rows) {
            record = dictionary.getRecord(row.charAt(0));
            valuesList = result.get(record);
            if (valuesList == null) {
                valuesList = new LinkedList<>();
                result.put(record, valuesList);
            }
            values = new LinkedList<>();
            valuesList.add(values);
            for (Item item : record.getItems()) {
                parseItem(item, row, values);
            }
        }
        return result;
    }

    private static void parseItem(Item item, String row, List<String> values) {
        String v = row.substring(item.getStart() - 1, item.getStart() - 1 + item.getLength());
        if (v.trim().isEmpty()) {
            values.add(null);
        } else {
            switch (item.getDataType()) {
                case "Number":
                    if (item.getDecimal() > 0 && !item.hasDecimalChar()) {
                        String head = v.substring(0, v.length() - item.getDecimal()).trim();
                        if (head.isEmpty()) {
                            head = "0";
                        }
                        String tail = v.substring(v.length() - item.getDecimal()).trim();
                        if (tail.isEmpty()) {
                            tail = "0";
                        }
                        values.add(head + "." + tail);
                    } else {
                        values.add(v);
                    }
                    break;
                case "Alpha":
                    values.add("\"" + v + "\"");
                    break;
            }
        }
        for (Item subItem : item.getSubItems()) {
            parseItem(subItem, row, values);
        }
    }

}
