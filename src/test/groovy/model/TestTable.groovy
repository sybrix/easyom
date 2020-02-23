package model

import sybrix.easyom.Blob


class TestTable extends ParentTable {

        private def dynamicProperties = []  // dynamicProperties is list of the names of each property changed before a persistence method invocation.
                                            // it ensures than when an update, or insert is called, that only the columns changed are updated.
        private static List<String> primaryKey = ['pkColumn']

        static columns = [testMapColumn:'column1', boolColumn:'boolean_column'] // map of propertyName:columnName
        //static tableName = 'tblTestTable'   // the name of the table, when the class name is not a match

        //private Map sequence = ["pkColumn":"id_seq"]

        Integer pkColumn
        Integer smallIntColumn

        Float floatColumn
        Double doublePrecisionColumn
        Double numericColumn
        Float decimalColumn
        Date dateColumn
        Date timestampColumn
        String charColumn;
        String varcharColumn = "defaultValue"
        Blob blobColumn
        Boolean boolColumn
        String testMapColumn   // there is no column called testMapColumn, this property maps to column1
}
