package util.joins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Stack;
import net.sf.jsqlparser.schema.Column;
import util.DataSet;

/**
 * Outer table deve ser a que possui a chave primaria participando do JOIN
 *
 * @author geomar
 */
public class HashJoin extends InMemoryJoins {

    @Override
    public DataSet join(DataSet inner, DataSet outer, Stack join) {
        DataSet result = new DataSet();
        LinkedList<String> cols = new LinkedList();
        for (String c : inner.getColumns()) {
            cols.add(inner.getTableName()+"."+c);
        }
        for (String c : outer.getColumns()) {
            cols.add(outer.getTableName()+"."+c);
        }
        result.setColumns(cols);

        Stack joinS = (Stack) join.clone();
        Object item;
        String outerColumn = "", innerColumn = "";
        while (!joinS.empty()) {
            item = joinS.pop();
            if (item instanceof Column) {
                if (((Column) item).getTable().getName().equals(outer.getTableName())) {
                    outerColumn = ((Column) item).getTable().getName() + "." + ((Column) item).getColumnName();
                    //break;
                } else {
                    innerColumn = ((Column) item).getTable().getName() + "." + ((Column) item).getColumnName();
                }
            }
        }
        HashMap<String, ArrayList<String[]>> hash = new HashMap();
        String key;
        int index = outer.getColumns().indexOf(outerColumn.split("\\.")[1]);
        for (String[] tOuter : outer.getData()) {
            key = tOuter[index];
            if (!hash.containsKey(key)) {
                ArrayList<String[]> array = new ArrayList();
                array.add(tOuter);
                hash.put(key, array);
            } else {
                ((ArrayList) hash.get(key)).add(tOuter);
            }
        }

        index = inner.getColumns().indexOf(innerColumn.split("\\.")[1]);
        for (String[] tInner : inner.getData()) {
            key = tInner[index];
            if (hash.containsKey(key)) {
                for (String[] tOuter : hash.get(key)) {
                    String[] tuple = new String[cols.size()];
                    int i = 0;
                    for (int j = 0; j < inner.getColumns().size(); j++, i++) {
                        tuple[i] = tInner[j];
                    }
                    for (int j = 0; j < outer.getColumns().size(); j++, i++) {
                        tuple[i] = tOuter[j];
                    }

                    result.getData().add(tuple);
                }
            }
        }
        return result;
    }

}
