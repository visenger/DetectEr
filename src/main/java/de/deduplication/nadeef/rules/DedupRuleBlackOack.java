package de.deduplication.nadeef.rules;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.simmetrics.StringMetric;
import org.simmetrics.builders.StringMetricBuilder;
import org.simmetrics.metrics.JaroWinkler;
import org.simmetrics.simplifiers.Simplifiers;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * NADEEF UDF: Matching rule to spot duplicates
 *
 * 1.step: put this udf to nadeef directory
 * 2.step: compile java class:
 * javac blackoak/DedupRuleBlackOack.java -cp out/bin/*:
 */
public class DedupRuleBlackOack extends PairTupleRule {
    //todo: finish names
    private String tableName = "inputDB";
    private String colName1 = "FirstName";
    private String colName2 = "LastName";
    private String colName3 = "Address";

    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        return table.groupOn(colName1);
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
            leftValue = Strings.nullToEmpty(leftValue);

            String rightValue = getValue(tuplePair, tableName, c, isRight);
            rightValue = Strings.nullToEmpty(rightValue);

            boolean r = stringMetric.compare(leftValue, rightValue) > 0.9;
            return r;
        });

        boolean result = allComparison.reduce(true, (a, b) -> a && b);

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
