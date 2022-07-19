package com.lisa.sqltokeynosql.util.operations;

/**
 *
 * @author geomar
 */
public class Equal extends Operator{

    @Override
    public boolean compare(Object a, Object b) {
        return a.toString().equals(b.toString());
    }
    
}
