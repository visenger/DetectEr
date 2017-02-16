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
 * Hosp schema: oid,prno,hospitalname,address,city,state,zip,countryname,phone,hospitaltype,hospitalowner,emergencyservice,condition,mc,measurename,score,sample,stateavg
 * <p>
 * NADEEF UDF: Matching rule to spot duplicates
 * <p>
 * 1.step: put this udf to nadeef directory
 * 1.a make sure the .java file does not contain the package declaration.
 * 2.step: compile java class:
 * javac blackoak/DedupRuleBlackOack.java -cp out/bin/*:
 */
public class DedupRuleHosp extends PairTupleRule {

    private String tableName = "tb_dirty_hosp_10k_with_rowid";
    private String colName1 = "prno";
    private String colName2 = "city";
    private String colName3 = "address";
    private String colName4 = "hospitalname";

    private List<String> simiColumns = Arrays.asList(colName2, colName3, colName4);
    private List<String> equiColumns = Arrays.asList(colName1);

    @Override
    public void initialize(String ruleName, List<String> tableNames) {
        super.initialize(ruleName, tableNames);
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
        int isLeft = 1;
        int isRight = 0;

        StringMetric stringMetric = StringMetricBuilder.with(new JaroWinkler())
                .simplify(Simplifiers.toLowerCase())
                .simplify(Simplifiers.replaceNonWord())
                .build();


        Stream<Boolean> simiComparison = simiColumns.stream().map(c -> {
            String leftValue = getValue(tuplePair, tableName, c, isLeft);
            leftValue = Strings.nullToEmpty(leftValue);

            String rightValue = getValue(tuplePair, tableName, c, isRight);
            rightValue = Strings.nullToEmpty(rightValue);

            boolean simiResult = stringMetric.compare(leftValue, rightValue) > 0.9;
            return simiResult;
        });

        boolean simiResult = simiComparison.reduce(true, (a, b) -> a && b);


        Stream<Boolean> equiComparison = this.equiColumns.stream().map(c -> {
            String leftValue = getValue(tuplePair, tableName, c, isLeft);
            leftValue = Strings.nullToEmpty(leftValue);

            String rightValue = getValue(tuplePair, tableName, c, isRight);
            rightValue = Strings.nullToEmpty(rightValue);
            boolean equiResult = leftValue.equals(rightValue);
            return equiResult;
        });

        boolean equiResult = equiComparison.reduce(true, (a, b) -> a && b);

        return simiResult && equiResult;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        //we want to identify errors only.
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
