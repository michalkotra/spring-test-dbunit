package com.github.springtestdbunit.assertion.custom;


import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.dbunit.DatabaseUnitException;
import org.dbunit.assertion.Difference;
import org.dbunit.assertion.FailureHandler;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementation of DbUnit assertions, based on the original methods present
 * at {@link org.dbunit.Assertion}
 *
 * @author Felipe Leme (dbunit@felipeal.net)
 * @author gommma (gommma AT users.sourceforge.net)
 * @version $Revision$ $Date$
 * @since 2.4.0
 */
public class DbUnitAssert extends org.dbunit.assertion.DbUnitAssert
{
 
    
    //should be a part of external configuration 
    private static final List<String> ROW_MATCHERS = Arrays.asList("_ID", "_CIF", "_CURRENCY_CODE");
    
    private static final Logger logger = LoggerFactory.getLogger(DbUnitAssert.class);

    @Override
    public void assertEquals(ITable expectedTable, ITable actualTable,
                             FailureHandler failureHandler) throws DatabaseUnitException
    {
        logger.trace("assertEquals(expectedTable, actualTable, failureHandler) - start");
        logger.debug("assertEquals: expectedTable={}", expectedTable);
        logger.debug("assertEquals: actualTable={}", actualTable);
        logger.debug("assertEquals: failureHandler={}", failureHandler);

        // Do not continue if same instance
        if (expectedTable == actualTable) {
            logger.debug(
                    "The given tables reference the same object. Will return immediately. (Table={})",
                    expectedTable);
            return;
        }

        if (failureHandler == null) {
            logger.debug("FailureHandler is null. Using default implementation");
            failureHandler = getDefaultFailureHandler();
        }

        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        ITableMetaData actualMetaData = actualTable.getTableMetaData();
        String expectedTableName = expectedMetaData.getTableName();

        // Put the columns into the same order
        Column[] expectedColumns = Columns.getSortedColumns(expectedMetaData);
        Column[] actualColumns = Columns.getSortedColumns(actualMetaData);
        
        
        // Verify columns
        Columns.ColumnDiff columnDiff =
                Columns.getColumnDiff(expectedMetaData, actualMetaData);
        if (columnDiff.hasDifference()) {
            String message = columnDiff.getMessage();
            Error error =
                    failureHandler.createFailure(message, Columns
                            .getColumnNamesAsString(expectedColumns), Columns
                            .getColumnNamesAsString(actualColumns));
            logger.error(error.toString());
            throw error;
        }

        // Get the datatypes to be used for comparing the sorted columns
        org.dbunit.assertion.DbUnitAssert.ComparisonColumn[] comparisonCols = getComparisonColumns(expectedTableName,
                expectedColumns, actualColumns, failureHandler);

        // Finally compare the data
        compareData(expectedTable, actualTable, comparisonCols, failureHandler);
    }


    @Override
    protected void compareData(ITable expectedTable, ITable actualTable,
                               ComparisonColumn[] comparisonCols, FailureHandler failureHandler)
            throws DataSetException
    {
        logger.debug("compareData(expectedTable={}, actualTable={}, "
                        + "comparisonCols={}, failureHandler={}) - start",
                new Object[] {expectedTable, actualTable, comparisonCols,
                        failureHandler});

        if (expectedTable == null) {
            throw new NullPointerException(
                    "The parameter 'expectedTable' must not be null");
        }
        if (actualTable == null) {
            throw new NullPointerException(
                    "The parameter 'actualTable' must not be null");
        }
        if (comparisonCols == null) {
            throw new NullPointerException(
                    "The parameter 'comparisonCols' must not be null");
        }
        if (failureHandler == null) {
            throw new NullPointerException(
                    "The parameter 'failureHandler' must not be null");
        }
        
        
        // iterate over all rows
        for (int i = 0; i < expectedTable.getRowCount(); i++) {
            // iterate over all columns of the current row
            for (int j = 0; j < comparisonCols.length; j++) {
                                
                ComparisonColumn compareColumn = comparisonCols[j];

                String columnName = compareColumn.getColumnName();
                DataType dataType = compareColumn.getDataType();

                Object expectedValue = expectedTable.getValue(i, columnName);
                Object actualValue = null;

                for (String rowMatcher : ROW_MATCHERS) {
                    String uniqueColumn = comparisonCols[0].getColumnName().substring(0,3) + rowMatcher;
                    if (checkColumnName(comparisonCols, uniqueColumn)) {
                        Object expectedPrimary = expectedTable.getValue(i, uniqueColumn);
                        for (int k = 0; k < actualTable.getRowCount(); k++) {
                            Object actualPrimary = actualTable.getValue(k, uniqueColumn);
                            if (actualPrimary.toString().equals(expectedPrimary.toString())) {
                                actualValue = actualTable.getValue(k, columnName);
                                break;
                            }
                        }
                    }
                }
                
                
                // Compare the values
                if (skipCompare(columnName, expectedValue, actualValue)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace( "ignoring comparison " + expectedValue + "=" +
                                actualValue + " on column " + columnName);
                    }
                    continue;
                }

                if (dataType.compare(expectedValue, actualValue) != 0) {

                    Difference diff = new Difference(
                            expectedTable, actualTable,
                            i, columnName,
                            expectedValue, actualValue);

                    // Handle the difference (throw error immediately or something else)
                    failureHandler.handle(diff);
                }
            }
        }
    }

    private boolean checkColumnName(ComparisonColumn[] comparisonCols, String columnName) {
        for (ComparisonColumn column : comparisonCols) {
            if (column.getColumnName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }
}
