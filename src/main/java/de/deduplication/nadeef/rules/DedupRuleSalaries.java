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
 * UDF for duplicates detection on SALARIES dataset;
 * <p>
 * schema:
 * <p>
 * oid varchar(55), id varchar(55), employeename varchar(255), jobtitle varchar(255), basepay  varchar(55), overtimepay varchar(55), otherpay varchar(55), benefits varchar(55), totalpay varchar(55), totalpaybenefits varchar(55), year varchar(55), notes varchar(55), agency varchar(55), status varchar(255)
 */
public class DedupRuleSalaries extends PairTupleRule {

    //todo: finish names
    private String tableName = "tb_dirty_salaries_with_id_nadeef_version";
    private String colName1 = "id";
    private String colName2 = "employeename";
    private String colName3 = "jobtitle";
    private String colName4 = "totalpay";

    private List<String> simiColumns = Arrays.asList(colName4, colName2);
    private List<String> equiColumns = Arrays.asList(colName3);

    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        return table.groupOn(colName3);
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
