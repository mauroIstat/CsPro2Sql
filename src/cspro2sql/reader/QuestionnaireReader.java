package cspro2sql.reader;

import cspro2sql.bean.Answer;
import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Questionnaire;
import cspro2sql.bean.Record;
import java.util.LinkedList;
import java.util.List;

/**
 * This class parse a plain text CSPro questionnaire
 *
 * @author Istat Cooperation Unit
 */
public class QuestionnaireReader {

    //Parse a questionnaire (CSPro plain text file) according to its dictionary 
    public static Questionnaire parse(Dictionary dictionary, String questionnaire) {
        Questionnaire result = new Questionnaire(questionnaire);
        String[] rows = questionnaire.split(Dictionary.DICT_NEWLINE_REGEXP);
        Record record = dictionary.getMainRecord();
        List<List<Answer>> valuesList = new LinkedList<>();
        result.setRecordValues(record, valuesList);
        List<Answer> values = new LinkedList<>();
        valuesList.add(values);
        for (Item item : record.getItems()) {
            parseItem(item, rows[0], values);
        }
        for (String row : rows) {
            record = dictionary.getRecord(row.charAt(0));
            valuesList = result.getRecordValues(record);
            if (valuesList == null) {
                valuesList = new LinkedList<>();
                result.setRecordValues(record, valuesList);
            }
            values = new LinkedList<>();
            valuesList.add(values);
            for (Item item : record.getItems()) {
                parseItem(item, row, values);
            }
        }
        return result;
    }

    private static void parseItem(Item item, String row, List<Answer> values) {
        if (row.length() < item.getStart() - 1 + item.getLength()) {
            values.add(new Answer(item, null));
        } else {
            String v = row.substring(item.getStart() - 1, item.getStart() - 1 + item.getLength());
            if (v.trim().isEmpty()) {
                values.add(new Answer(item, null));
            } else {
                switch (item.getDataType()) {
                    case Dictionary.ITEM_DECIMAL:
                        if (item.getDecimal() > 0 && !item.hasDecimalChar()) {
                            String head = v.substring(0, v.length() - item.getDecimal()).trim();
                            if (head.isEmpty()) {
                                head = "0";
                            }
                            String tail = v.substring(v.length() - item.getDecimal()).trim();
                            if (tail.isEmpty()) {
                                tail = "0";
                            }
                            values.add(new Answer(item, head + "." + tail));
                        } else {
                            values.add(new Answer(item, v));
                        }
                        break;
                    case Dictionary.ITEM_ALPHA:
                        values.add(new Answer(item, v));
                        break;
                    default:
                }
            }
        }
        for (Item subItem : item.getSubItems()) {
            parseItem(subItem, row, values);
        }
    }

}
