
package cspro2sql.bean;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a Record defined by the tag [Record] in the CSPro Dictionary
 * 
 * @author Istat Cooperation Unit
 */
public final class Record {
    
    private final String tablePrefix;
    private String name;
    private String recordTypeValue;
    private boolean required;
    private int max;
    private int length;
    
    private Record mainRecord;
    private boolean isMainRecord = false;
    private final List<Item> items = new LinkedList<>();

    public Record(String tablePrefix) {
        if (tablePrefix==null || tablePrefix.isEmpty()) {
            this.tablePrefix = "";
        } else {
            this.tablePrefix = tablePrefix + "_";
        }
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return (tablePrefix + name).toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRecordTypeValue() {
        return recordTypeValue;
    }

    public void setRecordTypeValue(String recordTypeValue) {
        this.recordTypeValue = recordTypeValue;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
    
    public void addItem(Item it) {
        items.add(it);
    }
    
    public void addItems(List<Item> its) {
        items.addAll(its);
    }
    
    public List<Item> getItems() {
        return items;
    }

    public void replaceItemWithSplit(Item item, List<Item> split) {
        int i = items.indexOf(item);
        for (Item it : split) {
            items.add(i++, it);
        }
        items.remove(i);
    }
    
    public Record getMainRecord() {
        return mainRecord;
    }

    public void setMainRecord(Record mainRecord) {
        this.mainRecord = mainRecord;
    }

    public boolean isMainRecord() {
        return isMainRecord;
    }

    public void setIsMainRecord(boolean isMainRecord) {
        this.isMainRecord = isMainRecord;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.name);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Record other = (Record) obj;
        return Objects.equals(this.name, other.name);
    }

}
