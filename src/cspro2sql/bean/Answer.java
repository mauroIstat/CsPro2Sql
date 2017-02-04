package cspro2sql.bean;

public class Answer {

    private final Item item;
    private final String value;

    public String error;

    public Answer(Item item, String value) {
        this.item = item;
        this.value = (value == null) ? null : value.trim();
    }

    public Item getItem() {
        return item;
    }

    public String getValue() {
        return value;
    }

    public boolean validate() {
        if (value == null) {
            return true;
        }
        try {
            String v = value;
            if (Dictionary.ITEM_DECIMAL.equals(item.getDataType())) {
                v = "" + Integer.parseInt(v);
            }
            if (item.hasValueSets()) {
                boolean found = false;
                for (ValueSet vs : item.getValueSets()) {
                    if (vs.containsKey(v)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    error = "[Col: " + item.getName() + " Val: " + value + "]";
                    return false;
                }
            }
        } catch (Exception ex) {
            error = "[Col: " + item.getName() + " Val: " + value + " (" + ex.getMessage() + ")]";
            return false;
        }
        return true;
    }

    public String getError() {
        return error;
    }

}
