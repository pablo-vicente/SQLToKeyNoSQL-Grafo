package util.joins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import util.DataSet;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public class NestedLoop extends InMemoryJoins {

    @Override
    public DataSet join(DataSet inner, DataSet outer, Stack join) {
        DataSet result = new DataSet();
        LinkedList<String> cols = new LinkedList();
        for (String c : inner.getColumns()) {
            cols.add(c);
        }
        for (String c : outer.getColumns()) {
            cols.add(c);
        }
        result.setColumns(cols);
        for (String[] tInner : inner.getData()) {
            for (String[] tOuter : outer.getData()) {
                if (applyFilterR((Stack)join.clone(), inner.getColumns(), outer.getColumns(), tInner, tOuter)){
                    String[] tuple = new String[cols.size()];
                    int i=0;
                    for (int j=0; j<inner.getColumns().size();j++,i++) {
                        tuple[i]=tInner[j];
                    }
                    for (int j=0; j<outer.getColumns().size();j++,i++) {
                        tuple[i]=tOuter[j];
                    }
                    
                    result.getData().add(tuple);                    
                }
            }
        }
        return result;
    }
    

}
