package cspro2sql.bean;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * @version 0.9.7
 */
public final class BeanFactory {

    private static final Pattern TAGS = Pattern.compile("(#[^\\s#]+(\\[[.*\\[\\#\\]]+\\])?)");
    private static final Pattern TAG_VALUE = Pattern.compile("^(#.*)\\[(.+)\\]$");

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
            } else if (line.startsWith(Dictionary.DICT_NOTE)) {
                parseNote(record, getValue(line));
            } else if (line.isEmpty()) {
                break;
            }
        }
        return record;
    }

    public static Item createItem(BufferedReader br, Set<String> multipleResponse) throws IOException {
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
            } else if (line.startsWith(Dictionary.DICT_NOTE)) {
                parseNote(item, getValue(line));
            } else if (line.isEmpty()) {
                break;
            }
        }
        if (multipleResponse.contains(item.getName())) {
            item.addTag(Dictionary.TAG_MULTIPLE);
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
            } else if (line.startsWith(Dictionary.DICT_NOTE)) {
                parseNote(valueSet, getValue(line));
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
        if (ss[0].matches("^[\"'].*[\"']$")) {
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

    private static void parseNote(Taggable obj, String note) {
        Matcher matcher = TAGS.matcher(note);
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                String name = matcher.group(i);
                if (name != null) {
                    Matcher valueMatcher = TAG_VALUE.matcher(name);
                    if (valueMatcher.find()) {
                        obj.addTag(new Tag(valueMatcher.group(1), valueMatcher.group(2)));
                    } else {
                        obj.addTag(new Tag(name));
                    }
                }
            }
        }
    }

}
