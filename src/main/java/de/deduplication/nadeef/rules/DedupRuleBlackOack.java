package de.deduplication.nadeef.rules;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by visenger on 06/12/16.
 */
public class DedupRuleBlackOack extends PairTupleRule {
    //todo: finish names
    private String tableName = "inputDB";
    private String colName1 = "";
    private String colName2 = "";
    private String columnName = "brand_name";

    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        return table.groupOn(columnName);
    }

    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        ArrayList<Violation> result = Lists.newArrayList();
        Tuple left = tuplePair.getLeft();
        Tuple right = tuplePair.getRight();


        if (isTupleDuplicate(tuplePair)) {
            Violation violation = new Violation(getRuleName());
            violation.addTuple(left);
            violation.addTuple(right);
            result.add(violation);
        }
        return result;
    }

    private boolean isTupleDuplicate(TuplePair tuplePair) {
        boolean result = false;
        int isLeft = 1;
        int isRight = 0;
        String leftValue = getValue(tuplePair, tableName, colName1, isLeft);
        String rightValue = getValue(tuplePair, tableName, colName1, isRight);
        result = true && Metrics.getEqual(leftValue, rightValue) == 1;


        return result;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        ArrayList<Fix> result = Lists.newArrayList();
        return result;
    }

    private String getValue(TuplePair pair, String tableName, String column, int isLeft) {
        Tuple left = pair.getLeft();
        Tuple right = pair.getRight();
        String result;
        if (isLeft == 0) {
            if (left.isFromTable(tableName)) {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            }
        } else {
            if (right.isFromTable(tableName)) {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            }
        }
        return result;
    }
}
