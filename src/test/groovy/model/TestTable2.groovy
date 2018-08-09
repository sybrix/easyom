package model;

/**
 * TestTable2 <br/>
 *
 * @author David Lee
 */
public class TestTable2 {
        private def dynamicProperties = []  // dynamicProperties is list of the names of each property changed before a persistence method invocation.
                                            // it ensures than when an update, or insert is called, that only the columns changed are updated.
        private static primaryKeys = ['pkColumn']
        //static columns = [testMapColumn:'column1', boolColumn:'boolean_column'] // map of propertyName:columnName
        static tableName = 'tblTestTable'   // the name of the table, when the class name is not a match

        Integer pkColumn
        Integer smallIntColumn
        Integer integerColumn
        Float floatColumn
        Double doublePrecisionColumn
        Double numericColumn
        Float decimalColumn
        Date dateColumn
        Date timestampColumn
        String charColumn;
        String varcharColumn = "defaultValue"
        Object blobColumn
        Boolean booleanColumn
        String column1   // there is no column called testMapColumn, this property maps to column1
}
