package cspro2sql.bean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
 * @version 0.9.18
 * @since 0.9.18
 */
public class Territory {

    private final List<TerritoryItem> itemsList;

    public Territory() {
        this.itemsList = new LinkedList<>();
    }
    
    public void addItems(Iterable<Item> dictItems) throws IOException {

        ArrayList<Item> dictItemList = new ArrayList<>();
        final Map<Item, String> dictItemToParentName = new LinkedHashMap<>();
        final Map<Item, String> dictItemToName = new LinkedHashMap<>();
        for (Item dictItem : dictItems) {
            dictItemList.add(dictItem);
            Tag tag = dictItem.getTag(Dictionary.TAG_TERRITORY);
            if (tag.getValue() != null) {
                String[] tagValues = tag.getValue().split(",");
                dictItemToName.put(dictItem, tagValues[0].trim());
                if (tagValues.length > 1 && !tagValues[1].isEmpty()) {
                    dictItemToParentName.put(dictItem, tagValues[1].trim());
                }
            }
        }

        int i = 0;
        while (i < dictItemList.size())
        {
            Item dictItem = dictItemList.get(i);
            String parentName = dictItemToParentName.getOrDefault(dictItem, null);
            String prevName = i == 0 ? null : dictItemList.get(i-1).getName();
            if (parentName == null || parentName.compareTo(prevName) == 0) {
                ++i;
            } else {
                dictItemList.remove(i);
                int parentPos = 0;
                for (; parentPos < dictItemList.size() && dictItemList.get(parentPos).getName().compareTo(parentName) != 0; ++parentPos)
                {    
                }
                if (parentPos == dictItemList.size()) {
                    throw new IOException("Territory " + dictItem.getName() + " has parent " + parentName + " that is NOT in found in territories");
                }
                dictItemList.add(parentPos + 1, dictItem);
            }
        }
            
        Map<String, TerritoryItem> nameToTerritoryItem = new LinkedHashMap<>();
        for (Item dictItem : dictItemList)
        {   
            String parentName = dictItemToParentName.getOrDefault(dictItem, null);
            TerritoryItem parent = parentName != null ? nameToTerritoryItem.get(parentName) : null;
            TerritoryItem territoryItem = new TerritoryItem(dictItem, parent, dictItemToName.get(dictItem));
            nameToTerritoryItem.put(dictItem.getName(), territoryItem);
            itemsList.add(territoryItem);
        }
    }

    public boolean isEmpty() {
        return this.itemsList.isEmpty();
    }

    public TerritoryItem getFirst() {
        return this.itemsList.get(0);
    }

    public TerritoryItem get(int i) {
        return this.itemsList.get(i);
    }

    public int size() {
        return this.itemsList.size();
    }

    public Iterable<Record> getItemRecords()
    {
        LinkedHashSet<Record> itemRecords = new LinkedHashSet<>();
        for (int i = 0; i < itemsList.size(); i++) {
            itemRecords.add(itemsList.get(i).getItem().getRecord());
        }
        return itemRecords;
    }
    
    public String getFromClause()
    {
        if (itemsList.isEmpty())
            return null;
        
        Record mainRecord = itemsList.get(0).getItem().getRecord().getMainRecord();
        String from = mainRecord.getTableName();
        
        for (Record record : getItemRecords()) {
            if (!record.isMainRecord())
                from += " JOIN " + record.getTableName() + " ON " + mainRecord.getTableName() + ".ID = " + record.getTableName() + "." + mainRecord.getName();
        }
        
        return from;
    }    
}
