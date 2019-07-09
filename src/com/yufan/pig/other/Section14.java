package com.yufan.pig.other;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

//判断记录是14字段
public class Section14 extends EvalFunc<Tuple> {
    public Tuple exec(Tuple input) throws IOException {
        // TODO Auto-generated method stub

        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            String SplitPoint = ",";
            String AllSection = (String) input.get(0);
            String[] Input = AllSection.split(SplitPoint);
            if (Input.length == 14) {
                TupleFactory newTuple = TupleFactory.getInstance();
                Tuple tt = newTuple.newTuple(1);
                tt.set(0, AllSection);
                return tt;
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        return null;
    }
}