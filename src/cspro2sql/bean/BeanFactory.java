package cspro2sql.bean;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * The factory to create CsPro Dictionary beans parsing a dictionary file
 *
 * @author Istat Cooperation Unit
 */
public final class BeanFactory {

    public static Record createRecord(BufferedReader br, String tablePrefix) throws IOException {
        String line;
        Record record = new Record(tablePrefix);
        while ((line = br.readLine()) != null) {
            if (line.startsWith(Dictionary.DICT_NAME)) {
                record.setName(getValue(line));
            } else if (line.startsWith(Dictionary.RECORD_TYPE)) {
                record.setRecordTypeValue(getValue(line).replaceAll("'", ""));
            } else if (line.startsWith(Dictionary.RECORD_REQUIRED)) {
                record.setRequired(Dictionary.DICT_YES.equals(getValue(line)));
            } else if (line.startsWith(Dictionary.RECORD_MAX)) {
                record.setMax(Integer.parseInt(getValue(line)));
            } else if (line.startsWith(Dictionary.RECORD_LEN)) {
                record.setLength(Integer.parseInt(getValue(line)));
            } else if (line.isEmpty()) {
                break;
            }
        }
        return record;
    }

    public static Item createItem(BufferedReader br) throws IOException {
        String line;
        Item item = new Item();
        while ((line = br.readLine()) != null) {
            if (line.startsWith(Dictionary.DICT_NAME)) {
                item.setName(getValue(line));
            } else if (line.startsWith(Dictionary.ITEM_DATATYPE)) {
                item.setDataType(getValue(line));
            } else if (line.startsWith(Dictionary.ITEM_LEN)) {
                item.setLength(Integer.parseInt(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_ITEMTYPE)) {
                item.setSubItem(Dictionary.ITEM_SUBITEM.equals(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_START)) {
                item.setStart(Integer.parseInt(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_ZEROFILL)) {
                item.setZeroFill(Dictionary.DICT_YES.equals(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_OCCS)) {
                item.setOccurrences(Integer.parseInt(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_DECCHAR)) {
                item.setDecimalChar("Yes".equals(getValue(line)));
            } else if (line.startsWith(Dictionary.ITEM_DECIMAL)) {
                item.setDecimal(Integer.parseInt(getValue(line)));
            } else if (line.isEmpty()) {
                break;
            }
        }
        return item;
    }

    public static ValueSet createValueSet(BufferedReader br) throws IOException {
        String line;
        ValueSet valueSet = new ValueSet();
        while ((line = br.readLine()) != null) {
            if (line.startsWith(Dictionary.DICT_LABEL)) {
                valueSet.setLabel(getValue(line));
            } else if (line.startsWith(Dictionary.DICT_NAME)) {
                valueSet.setName(getValue(line));
            } else if (line.startsWith(Dictionary.VALUE_LINK)) {
                valueSet.setLink(getValue(line));
            } else if (line.startsWith(Dictionary.VALUE_VALUE)) {
                if (!addValueSetValues(valueSet, line)) {
                    return null;
                }
            } else if (line.isEmpty()) {
                break;
            }
        }
        return valueSet;
    }

    private static String getValue(String s) {
        return s.split("=")[1];
    }

    private static boolean addValueSetValues(ValueSet valueSet, String s) {
        String[] ss = getValue(s).split(";");
        if (ss[0].matches("^'.*'$")) {
            ss[0] = ss[0].substring(1, ss[0].length() - 1);
        }
        ss[0] = ss[0].trim();
        if (ss.length == 1) {
            ss = new String[]{ss[0], ss[0]};
        } else {
            ss[1] = ss[1].split("[" + Dictionary.DICT_LABEL_LANGUAGE_SEPARATOR + "]")[0];
        }
        if (ss[0].contains(":")) {
            try {
                int a = Integer.parseInt(ss[0].split(":")[0]);
                int b = Integer.parseInt(ss[0].split(":")[1]);
                for (; a <= b; a++) {
                    valueSet.addValue("" + a, ss[1]);
                    if (valueSet.size() > 1000) {
                        return false;
                    }
                }
            } catch (NumberFormatException ex) {
                return false;
            }
        } else {
            valueSet.addValue(ss[0], ss[1]);
            if (valueSet.size() > 1000) {
                return false;
            }
        }
        valueSet.setValueLength(Math.max(valueSet.getValueLength(), ss[1].length()));
        return true;
    }

}
