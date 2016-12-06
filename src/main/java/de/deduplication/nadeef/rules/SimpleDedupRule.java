package de.deduplication.nadeef.rules;

import com.google.common.collect.Lists;
import org.simmetrics.StringMetric;
import org.simmetrics.builders.StringMetricBuilder;
import org.simmetrics.metrics.JaroWinkler;
import org.simmetrics.simplifiers.Simplifiers;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by visenger on 06/12/16.
 */
public class SimpleDedupRule extends PairTupleRule {

    private String tableName = "inputDB";
    private String colName1 = "a";
    private String colName2 = "b";
    private String colName3 = "c";


    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
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
        //boolean result = true;
        int isLeft = 1;
        int isRight = 0;

        StringMetric stringMetric = StringMetricBuilder.with(new JaroWinkler())
                .simplify(Simplifiers.toLowerCase())
                .simplify(Simplifiers.replaceNonWord())
                .build();


        List<String> columns = Arrays.asList(colName1, colName2, colName3);

        Stream<Boolean> allComparison = columns.stream().map(c -> {
            String leftValue = getValue(tuplePair, tableName, c, isLeft);
            String rightValue = getValue(tuplePair, tableName, c, isRight);
            boolean r = stringMetric.compare(leftValue, rightValue) > 0.9;
            return r;
        });

        boolean result = allComparison.reduce(true, (a, b) -> a && b);

//        String leftValue = getValue(tuplePair, tableName, colName1, isLeft);
//        String rightValue = getValue(tuplePair, tableName, colName1, isRight);
//        result = result && Metrics.getEqual(leftValue, rightValue) == 1;


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
