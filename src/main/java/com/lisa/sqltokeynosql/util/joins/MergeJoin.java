package com.lisa.sqltokeynosql.util.joins;

import java.util.Stack;
import com.lisa.sqltokeynosql.util.DataSet;

/**
 *
 * @author geomar
 */
public class MergeJoin extends InMemoryJoins{

    @Override
    public DataSet join(DataSet inner, DataSet outer, Stack join) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
