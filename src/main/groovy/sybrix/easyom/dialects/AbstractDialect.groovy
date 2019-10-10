package sybrix.easyom.dialects

import org.codehaus.groovy.runtime.GStringImpl
import org.codehaus.groovy.runtime.metaclass.ThreadManagedMetaBeanProperty
import sybrix.easyom.DbServerFunction
import sybrix.easyom.SelectColumnsAndAliasMap
import sybrix.easyom.SelectSqlStatement
import sybrix.easyom.SqlStatement
import sybrix.easyom.WhereClauseAndValues
import sybrix.easyom.WhereClauseParameters

import java.beans.BeanInfo
import java.beans.Introspector
import java.beans.PropertyDescriptor
import java.sql.Blob
import java.sql.Connection
import java.util.logging.Logger


abstract class AbstractDialect implements Dialect {
        private static final Logger logger = Logger.getLogger(AbstractDialect.class.name)

        private Boolean tableNameCamelCased = false
        private Boolean columnsNamesCamelCased = true
        private Boolean useTablePrefix = false
        private String tablePrefix = ""


        public void init(Properties properties) {
                tableNameCamelCased = Boolean.parseBoolean(properties.getProperty("camel.case.table.name", "false"))
                columnsNamesCamelCased = Boolean.parseBoolean(properties.getProperty("camel.case.column.name", "true"))
                useTablePrefix = Boolean.parseBoolean(properties.getProperty("use.table.prefix", "false"))
                tablePrefix = properties.getProperty("table.prefix")
        }

        SelectSqlStatement createSelectStatement(Class modelClass, WhereClauseParameters whereClauseParameters) {
                def selectPart = new StringBuilder()
                def fromPart = new StringBuilder()
                def sql = new StringBuilder()

                def orderBy
                def page
                def pageSize
                def countColumn

                String tbl = getTableName(modelClass)

                orderBy = whereClauseParameters?.getOrderBy()
                page = whereClauseParameters?.getPage()
                pageSize = whereClauseParameters?.getPageSize()
                countColumn = whereClauseParameters?.getCountColumn()

                selectPart << "SELECT "

                // prolly don't need this
                selectPart.append(createPagingAfterSelect(page, pageSize))

                SelectColumnsAndAliasMap selectColumnsAndAliasMap = createSelectColumnsAndAliasMap(modelClass)

                selectPart.append(selectColumnsAndAliasMap.selectClause.toString())
                selectPart.replace(selectPart.size() - 2, selectPart.size(), ' ')

                fromPart << "FROM $tbl "

                WhereClauseAndValues whereClauseAndValues

                if (whereClauseParameters.size() > 0) {
                        fromPart << "WHERE "
                        whereClauseAndValues = createWhereClause(modelClass, whereClauseParameters)
                }

                sql.append(selectPart)
                sql.append(fromPart)
                sql.append(whereClauseAndValues?.whereClause ?: "")
                sql.append(createOrderByClause(orderBy, modelClass))

                GString sqlGstring = new GStringImpl(whereClauseAndValues?.values?.toArray() ?: [].toArray(), sql.toString().trim().split('\\?'))

                if (whereClauseParameters.getPage()) {
                        String pagingAfterOrderBy = createPagingAfterOrderBy(page, pageSize, orderBy)
                        sql.append(pagingAfterOrderBy)

                        StringBuilder countQuery = new StringBuilder()
                        countQuery.append("SELECT count(").append(whereClauseParameters.getCountColumn()).append(") ct ").append(fromPart).append(whereClauseAndValues?.whereClause ?: "")

                        GString gstring = new GStringImpl(whereClauseAndValues?.values?.toArray() ?: [].toArray(), countQuery.toString().trim().split('\\?'))
                        SqlStatement countStatement = new SqlStatement(sql: countQuery.toString(), sqlGString: gstring, values: whereClauseAndValues?.values)

                        new SelectSqlStatement(sql: fromPart.toString(), sqlGString: sqlGstring, values: whereClauseAndValues?.values, countStatement: countStatement, selectColumnsAndAliasMap: selectColumnsAndAliasMap)

                } else {
                        new SelectSqlStatement(sql: sql.toString(), sqlGString: sqlGstring, values: whereClauseAndValues?.values, selectColumnsAndAliasMap: selectColumnsAndAliasMap)
                }
        }

        abstract GString createPagingStringQuery(GString sql, Integer page, Integer pageSize)

        abstract String createPagingAfterSelect(Integer page, Integer pageSize)

        abstract String createPagingAfterOrderBy(Integer page, Integer pageSize, String orderBy)

        abstract String createRecordCountStringQuery(String sql)

        abstract Blob createBlob(Connection connection, InputStream inputStream)

        SqlStatement createUpdateStatement(Object modelInstance) {
                Class clazz = modelInstance.getClass()
                String tableName = getTableName(clazz)

                def values = []
                def sql = new StringBuilder()
                def columnName

                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                sql << "UPDATE $tableName SET "

                modelInstance.dynamicProperties.each {
                        if (hasColumnsProperty && modelInstance?.columns.containsKey(it))
                                columnName = unCamelCaseColumn(modelInstance?.columns[it])
                        else
                                columnName = unCamelCaseColumn(it)

                        def _type = getType(clazz, it)
                        def v = getValue(_type, modelInstance?."$it")

                        if (v instanceof DbServerFunction) {
                                sql << "$columnName =  ${v.function()}, "
                        } else if (v != null && v?.toString().startsWith("function:")) {
                                sql << "$columnName =  ${v?.toString().substring(9).trim()}, "

                        } else {
                                sql << "$columnName = ?, "
                                values << v
                        }
                }

                sql.replace(sql.size() - 2, sql.size(), '')

                sql << " WHERE "

                clazz.primaryKey.each {
                        columnName = unCamelCaseColumn(it)
                        sql << "$columnName = ? and "
                        def val = modelInstance?."$it"
                        values << getValue(val?.class, val)
                }

                sql.replace(sql.size() - 6, sql.size(), '')
                GString gstring = new GStringImpl(values?.toArray() ?: [].toArray(), sql.toString().trim().split('\\?'))

                new SqlStatement(sql: sql.toString(), sqlGString: gstring, values: values)
        }

        SqlStatement createInsertStatement(Object instance, Boolean insertUpdatedColumnsOnly) {
                Class clazz = instance.class
                String tableName = getTableName(clazz)

                def values = []
                StringBuilder s = new StringBuilder()

                s.append "INSERT INTO $tableName ("
                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                def allColumns = getAllColumns(clazz, this)

                def properties = allColumns
                if (insertUpdatedColumnsOnly)
                        properties = this.dynamicProperties

                boolean manualPrimaryKeys
                boolean sequence = false
                if (isProperty(clazz, "sequence")) {
                        sequence = true
                }


                instance?.primaryKey.each {
                        if (instance?.dynamicProperties.contains(it)) {
                                manualPrimaryKeys = true
                        } else {
                                manualPrimaryKeys = false
                        }
                }

                if (!manualPrimaryKeys) {
                        properties.removeAll(instance.primaryKey)
                }

                if (isProperty(clazz, 'exclude')) {
                        properties.removeAll(instance.exclude)
                }

                properties.each {
                        if (hasColumnsProperty && instance.columns?.containsKey(it)) {
                                s << instance.columns[it]
                        } else {
                                s << unCamelCase(it)
                        }

                        s << ', '
                }
                if (s.toString().endsWith("', '")) {
                        s.replace(s.size() - 2, s.size(), '')
                }

                s << ') VALUES ('

                properties.each {

                        def val = instance."$it"


                        if (val instanceof DbServerFunction) {
                                s << "${val.function()}, "
                        } else if (val?.toString().startsWith("function:")) {
                                s << "${val?.toString().substring(9).trim()}, "
                        } else {
                                s << '?,'
                                values << getValue(val?.class, val)
                        }
                        /*
                        if (val == null){
                                values << val
                        } else if (val instanceof java.sql.Timestamp) {
                                values << val
                        } else if (val instanceof java.util.Date || val instanceof java.sql.Date) {
                                values << new java.sql.Timestamp(val.time)
                        } else if (val instanceof java.lang.Boolean || val.class == boolean.class) {
                                values << (val == true ? '1'.toCharacter() : '0'.toCharacter())
                        } else {
                                values << val
                        }
                        */
                }
                if (properties.size() > 0) {
                        s.replace(s.size() - 1, s.size(), '')
                }
                s << ')'


                GString gString = new GStringImpl(values?.toArray() ?: [].toArray(), s.toString().trim().split('\\?'))
                new SqlStatement(sqlGString: gString)
        }

        public WhereClauseAndValues createWhereClause(Class modelClass, WhereClauseParameters whereMap) {
                WhereClauseAndValues whereClauseAndValues = new WhereClauseAndValues()

                StringBuilder sql = new StringBuilder()
                String operator = "AND"
                String columnName
                boolean hasColumnsProperty = isProperty(modelClass, 'columns')

                if (whereMap.getOperator() != null) {
                        if (whereMap.getOperator().toUpperCase() == 'OR' || whereMap.getOperator().toUpperCase() == 'AND') {
                                operator = whereMap.getOperator().toUpperCase()
                        }
                }

                whereMap.each {
                        def filterOperator
                        def key = it.key
                        def _value = it.value

                        if (it.key.toString().endsWith(">")) {
                                key = it.key.toString().substring(0, it.key.toString().length() - 2).trim()
                                filterOperator = ">"
                        } else if (it.key.toString().endsWith("<")) {
                                key = it.key.toString().substring(0, it.key.toString().length() - 2).trim()
                                filterOperator = "<"
                        } else if (it.key.toString().endsWith("<=")) {
                                key = it.key.toString().substring(0, it.key.toString().length() - 3).trim()
                                filterOperator = "<="
                        } else if (it.key.toString().endsWith(">=")) {
                                key = it.key.toString().substring(0, it.key.toString().length() - 3).trim()
                                filterOperator = ">="
                        } else if (it.key.toString().endsWith("<>")) {
                                key = it.key.toString().substring(0, it.key.toString().length() - 3).trim()
                                filterOperator = "<>"
                        }

                        if (hasColumnsProperty && modelClass.columns?.containsKey(key)) {
                                columnName = modelClass.columns[key]
                        } else {
                                columnName = unCamelCaseColumn(key)
                        }

                        if (_value == null) {
                                sql << "$columnName IS NULL $operator "
                        } else if (filterOperator) {
                                if (_value instanceof List) {
                                        _value.each { param ->
                                                sql << "$columnName $filterOperator ?  OR "
                                                whereClauseAndValues.values << getValue(param?.class, param)
                                        }

                                } else {
                                        sql << "$columnName $filterOperator ?  $operator "
                                        whereClauseAndValues.values << getValue(_value?.class, _value)
                                }
                        } else {
                                if (_value instanceof List) {
                                        _value.each { param ->
                                                sql << "$columnName = ?  OR "
                                                whereClauseAndValues.values << getValue(param?.class, param)
                                        }
                                }else {
                                        sql << "$columnName = ?  $operator "
                                        whereClauseAndValues.values << getValue(_value?.class, _value)
                                }
                        }
                }

                sql.replace(sql.size() - 4, sql.size(), '')

                whereClauseAndValues.whereClause = sql.toString()
                whereClauseAndValues
        }

        public SqlStatement createDeleteStatement(Class clazz, WhereClauseParameters whereMap) {

                def sql = new StringBuilder()

                def tbl = getTableName(clazz)


                sql << "DELETE FROM $tbl WHERE "
                WhereClauseAndValues whereClauseAndValues = createWhereClause(clazz, whereMap)
                sql.append(whereClauseAndValues.whereClause)

                GString gstring = new GStringImpl(whereClauseAndValues?.values?.toArray() ?: [].toArray(), sql.toString().trim().split('\\?'))
                new SqlStatement(sql: sql.toString(), sqlGString: gstring, values: whereClauseAndValues.values)

        }

        public void setProperty(String name, Object value) {
                MetaProperty metaProperty = this.class.metaClass.getMetaProperty(name)

                if (metaProperty instanceof org.codehaus.groovy.runtime.metaclass.ThreadManagedMetaBeanProperty) {
                        metaProperty.setThreadBoundPropertyValue(this, name, value)
                } else {
                        metaProperty.setProperty(this, value)
                        if (!this.dynamicProperties.contains(name))
                                this.dynamicProperties << name
                        logger.finest("setProperty $name")
                }
        }

        Object getValue(Object obj, Object val) {
                if (val == null)
                        return null

                if (obj == java.sql.Date.class || obj == java.util.Date.class) {
                        return new java.sql.Timestamp(val.time)
                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
                        return (val == true ? '1' : '0')
                } else {
                        return val
                }
        }

        protected String getTableName(Class clazz) {

                String tableName

                if (isProperty(clazz, 'tableName')) {
                        def metaProperty = clazz.metaClass.getMetaProperty('tableName')
                        if (metaProperty instanceof ThreadManagedMetaBeanProperty) return metaProperty.initialValue else
                                return metaProperty.getProperty('tableName')
                } else {
                        if (clazz.name.lastIndexOf('.') > -1) {
                                tableName = clazz.name.substring(clazz.name.lastIndexOf('.') + 1)
                        } else {
                                tableName = clazz.name
                        }
                }

                if (tableNameCamelCased) {
                        tableName = unCamelCase(tableName)
                }

                if (useTablePrefix) {
                        return tablePrefix + tableName
                } else {
                        return tableName
                }
        }

        protected List<String> getAllColumns(clazz, thisObject) {
                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                def properties = []

                BeanInfo sourceInfo = Introspector.getBeanInfo(clazz)
                PropertyDescriptor[] sourceDescriptors = sourceInfo.getPropertyDescriptors()

                for (int x = 0; x < sourceDescriptors.length; x++) {
                        try {
                                if (sourceDescriptors[x].getReadMethod() != null && sourceDescriptors[x].getWriteMethod() != null) {
                                        def property = sourceDescriptors[x].getName()
                                        def prop = clazz.metaClass.getMetaProperty(property)

                                        if (clazz.metaClass.getMetaProperty(property) && !property.equals('metaClass') && (prop instanceof MetaBeanProperty)
                                                && !property.equals('_dataSource')) {

                                                if (prop.field == null || !prop.field.isStatic()) {
                                                        properties << property
                                                }
                                        }
                                }
                        } catch (Exception e) {
                                throw e
                        }
                }


                return properties
        }

        protected Map<String, String> createColumnAliasMap(Class clazz, Object thisObject) {

                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                Map<String, String> columns = [:]

                clazz.declaredFields.each {
                        def prop = clazz.metaClass.getMetaProperty(it.name)
                        def exclude = false;

                        if (isProperty(clazz, 'exclude')) {
                                exclude = thisObject.exclude.contains(it.name)
                        }

                        //logger.fine(it.name + " in exclude = " + exclude)
                        if (exclude != true) {
                                if (clazz.metaClass.getMetaProperty(it.name) && !it.name.equals('metaClass') && (prop instanceof MetaBeanProperty)) {
                                        if (!prop.field.isStatic()) {
                                                if (hasColumnsProperty && thisObject.columns?.containsKey(it.name)) {
                                                        columns[it.name] = thisObject.columns[it.name]
                                                } else {
                                                        columns[it.name] = unCamelCaseColumn(it.name)
                                                }
                                        }
                                }
                        }
                }

                columns
        }

        public SelectColumnsAndAliasMap createSelectColumnsAndAliasMap(Class clazz) {
                Map<String, String> columns = [:]
                if (clazz == null) {
                        return columns
                }

                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                StringBuilder sql = new StringBuilder()


                clazz.declaredFields.each {
                        def prop = clazz.metaClass.getMetaProperty(it.name)
                        def exclude = false;

                        if (isProperty(clazz, 'exclude')) {
                                exclude = clazz.exclude.contains(it.name)
                        }

                        //logger.fine(it.name + " in exclude = " + exclude)
                        if (exclude != true) {
                                if (clazz.metaClass.getMetaProperty(it.name) && !it.name.equals('metaClass') && (prop instanceof MetaBeanProperty)) {
                                        if (!prop.field.isStatic()) {
                                                if (hasColumnsProperty && clazz.columns?.containsKey(it.name)) {
                                                        sql << clazz.columns[it.name] << ' as ' << it.name
                                                        columns[it.name] = clazz.columns[it.name]
                                                } else {
                                                        sql << unCamelCaseColumn(it.name) << ' as ' << it.name
                                                        columns[it.name] = unCamelCaseColumn(it.name)
                                                }
                                                sql << ', '

                                        }
                                }
                        }
                }

                new SelectColumnsAndAliasMap(selectClause: sql.toString(), columnAliasMap: columns)
        }


        protected boolean isDynamicProperty(Class clazz, String propertyName) {
                def metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                if (metaProperty instanceof ThreadManagedMetaBeanProperty) {
                        return true
                } else {
                        return false
                }
        }

        protected boolean isProperty(Class clazz, String propertyName) {
                def metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                if (metaProperty instanceof MetaBeanProperty) {
                        return true
                } else {
                        return false
                }
        }


        protected boolean isProperty(Object obj, String propertyName) {
                def metaProperty = obj.class.metaClass.getMetaProperty(propertyName)
                if (metaProperty instanceof MetaBeanProperty) {
                        return true
                } else {
                        return false
                }
        }

        protected Class getType(Class clazz, String propertyName) {
                MetaBeanProperty metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                return metaProperty.getSetter().getNativeParameterTypes()[0]
        }

        protected String getColumnName(Class clazz, String propertyName) {

                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                if (hasColumnsProperty) {
                        def prop = clazz.metaClass.getMetaProperty(propertyName)
                        def columnsMap = clazz?.columns

                        if (clazz.metaClass.getMetaProperty(propertyName) && (prop instanceof MetaBeanProperty)) {
                                if (prop.field != null && !prop.field.isStatic()) {
                                        if (hasColumnsProperty && columnsMap?.containsKey(propertyName)) {
                                                return columnsMap[propertyName]
                                        } else {
                                                return unCamelCaseColumn(propertyName)
                                        }
                                }
                        }
                }

                return unCamelCaseColumn(propertyName)
        }

        protected String createOrderByClause(String orderBy, Class modelClass) {
                StringBuilder sql = new StringBuilder("")

                def orderByAry

                if (orderBy != null) {
                        sql << " ORDER BY "
                        orderByAry = orderBy.split(',')

                        orderByAry.each {
                                def orderByPart = it.trim().split(' ')
                                sql << getColumnName(modelClass, orderByPart[0])
                                if (orderByPart.size() > 1) sql << ' ' << orderByPart[1]
                                sql << ','
                        }

                        sql.setLength(sql.length() - 1)
                }

                sql.toString()
        }

        protected Integer getSkip(Integer pageSize, Integer page) {
                if (page >= 1) {
                        return (pageSize * (page - 1))
                } else {
                        return 0
                }
        }

        protected def getSelectValue(obj, val) {
                if (val == null) return

                if (obj == java.util.Date.class && val.class == java.sql.Timestamp) {
                        return new java.util.Date(val.time)
                } else if (obj == java.sql.Date.class) {
                        return new java.util.Date(val.time)
                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
                        def v = val.toString().toLowerCase().trim()

                        return (v == '1' || v == 1 || v == 'true' || v == 't' || v == 'y') ? true : false
                } else {
                        return val
                }
        }

        protected String camelCase(String column) {
                StringBuffer newColumn = new StringBuffer()
                boolean underScoreFound = false
                int index = -1
                int currentPosition = 0
                while ((index = column.indexOf('_', currentPosition)) > -1) {
                        newColumn.append(column.substring(currentPosition, index).toLowerCase())
                        newColumn.append(column.substring(index + 1, index + 2).toUpperCase())

                        currentPosition = index + 2
                        underScoreFound = true
                }

                if (underScoreFound == false) {
                        return column
                } else {
                        newColumn.append(column.substring(currentPosition, column.length()).toLowerCase())
                }

                return newColumn.toString()

        }

        protected String unCamelCase(String column) {
                StringBuffer newColumn = new StringBuffer()
                for (int i = 0; i < column.length(); i++) {
                        if (Character.isLetter(column.charAt(i)) && Character.isUpperCase(column.charAt(i))) {
                                if (i > 0) newColumn.append("_")

                                newColumn.append(Character.toLowerCase(column.charAt(i)))
                        } else {
                                newColumn.append(column.charAt(i))
                        }
                }

                return newColumn.toString()
        }

        protected String unCamelCaseColumn(String column) {
                if (columnsNamesCamelCased == true) {
                        return unCamelCase(column)
                } else {
                        return column
                }
        }

        protected String parseOrderBy(StringBuffer sql, String orderBy, Class clazz) {
                def orderByAry
                if (orderBy != null) {
                        sql << "ORDER BY "
                        orderByAry = orderBy.split(',')

                        orderByAry.each {
                                def orderByPart = it.trim().split(' ')
                                sql << getColumnName(clazz, orderByPart[0])
                                if (orderByPart.size() > 1)
                                        sql << ' ' << orderByPart[1]
                                sql << ','
                        }

                        sql.setLength(sql.length() - 1)
                }
        }
}





