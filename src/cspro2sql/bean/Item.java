package cspro2sql.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents an Item defined by the tag [Item] in the CSPro
 * Dictionary
 *
 * @author Istat Cooperation Unit
 */
public final class Item {

    private Record record;
    private String name;
    private String valueSetName;
    private String dataType = "Number";
    private int start;
    private int length;
    private int occurrences;
    private int decimal;
    private boolean subItem;
    private boolean zeroFill;
    private boolean decimalChar;
    private final List<Item> subItems = new ArrayList<>();
    private final List<ValueSet> valueSets = new ArrayList<>();

    public void setRecord(Record record) {
        this.record = record;
    }

    public void setValueSetName(String valueSetName) {
        this.valueSetName = valueSetName;
    }

    public String getValueSetName() {
        if (valueSetName != null) {
            return record.getValueSetPrefix() + valueSetName;
        }
        return record.getValueSetPrefix() + name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(int occurrences) {
        this.occurrences = occurrences;
    }

    public int getDecimal() {
        return decimal;
    }

    public void setDecimal(int decimal) {
        this.decimal = decimal;
    }

    public boolean isSubItem() {
        return subItem;
    }

    public void setSubItem(boolean subItem) {
        this.subItem = subItem;
    }

    public boolean isZeroFill() {
        return zeroFill;
    }

    public void setZeroFill(boolean zeroFill) {
        this.zeroFill = zeroFill;
    }

    public boolean hasDecimalChar() {
        return decimalChar;
    }

    public void setDecimalChar(boolean decimalChar) {
        this.decimalChar = decimalChar;
    }

    public List<Item> getSubItems() {
        return subItems;
    }

    public void addSubItem(Item subItem) {
        this.subItems.add(subItem);
        subItem.setRecord(record);
    }

    public List<ValueSet> getValueSets() {
        return valueSets;
    }

    public void addValueSet(ValueSet valueSet) {
        this.valueSets.add(valueSet);
    }

    public boolean hasValueSets() {
        return !this.valueSets.isEmpty();
    }

    public int getValueSetsValueLength() {
        int len = 0;
        for (ValueSet vs : this.valueSets) {
            len = Math.max(len, vs.getValueLength());
        }
        return len;
    }

    public List<Item> splitIntoColumns(ValueSet values) {
        List<Item> items = new ArrayList<>(getLength());
        for (int i = 0; i < getLength(); i++) {
            Item c = clone();
            c.addValueSet(values);
            c.setName(c.getName() + "_" + i);
            c.setStart(c.getStart() + i);
            c.valueSetName = this.name;
            c.setLength(1);
            items.add(c);
        }
        return items;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 43 * hash + Objects.hashCode(this.name);
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
        final Item other = (Item) obj;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public Item clone() {
        Item clone = new Item();
        clone.dataType = this.dataType;
        clone.decimal = this.decimal;
        clone.decimalChar = this.decimalChar;
        clone.length = this.length;
        clone.name = this.name;
        clone.occurrences = this.occurrences;
        clone.start = this.start;
        clone.subItem = this.subItem;
        clone.zeroFill = this.zeroFill;
        return clone;
    }

}
