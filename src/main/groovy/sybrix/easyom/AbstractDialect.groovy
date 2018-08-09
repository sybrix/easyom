package sybrix.easyom

import org.codehaus.groovy.runtime.GStringImpl
import org.codehaus.groovy.runtime.InvokerHelper
import org.codehaus.groovy.runtime.metaclass.ThreadManagedMetaBeanProperty

import java.beans.BeanInfo
import java.beans.Introspector
import java.beans.PropertyDescriptor
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.logging.Logger

class AbstractDialect {
        private static final Logger logger = Logger.getLogger(AbstractDialect.class.name)

        private Boolean tableNameCamelCased = false
        private Boolean columnsNamesCamelCased = true
        private Boolean useTablePrefix = false
        private String tablePrefix = ""

        class PagingAndSortByOptions extends HashMap<String, Object> {
                PagingAndSortByOptions(String orderBy) {
                        this.put("orderBy", orderBy)
                }

                PagingAndSortByOptions(String orderBy, int page, int pageSize) {
                        this.put("orderBy", orderBy)
                        this.put("page", page)
                        this.put("pageSize", pageSize)
                }

                PagingAndSortByOptions(String orderBy, int page, int pageSize, String countColumn) {
                        this.put("orderBy", orderBy)
                        this.put("page", page)
                        this.put("pageSize", pageSize)
                        this.put("countColumn", countColumn)
                }
        }

        class WhereClauseParameters extends HashMap<String, Object> {
                String operator = "AND"

                WhereClauseParameters() {
                        put("operator", operator)
                }

                def addParameter(String parameterName, Object value) {
                        this.put(parameterName, value)
                }

                void addPage(int page){
                        this.put("page", page)
                }

                void addPaging(String orderBy, int page, int pageSize, String countColumn) {
                        this.put("orderBy", orderBy)
                        this.put("page", page)
                        this.put("pageSize", pageSize)
                        this.put("countColumn", countColumn)
                }

                String getCountColumn(){
                        this.get("countColumn")
                }

                int getPage(){
                        this.get("page")
                }

                int getPageSize(){
                        this.get("pageSize")
                }

                String getOrderBy(){
                        this.get("orderBy")
                }
        }

        class WhereClauseAndValues {
                String whereClause
                List<Object> values = []
        }

        class UpdateColumnsAndValues {
                String selectClause
                List<Object> values = []
        }

        class SelectColumnsAndAliasMap {
                String selectClause
                Map<String, String> columnAliasMap = [:]
        }

        //public Object list(String cls, PagingAndSortByOptions optionsMap) {
        String createSelectStatement(Class modelClass, PagingAndSortByOptions optionsMap) {
                def sql = new StringBuilder()

                def values = []
                def orderBy
                def page
                def pageSize

                String tbl = getTableName(modelClass)

                if (optionsMap instanceof Map) {
                        orderBy = optionsMap.remove("orderBy")
                        page = optionsMap.remove("page")
                        pageSize = optionsMap.remove("pageSize")
                }


                sql << "SELECT "

                sql.append(createPagingAfterSelect(page, pageSize, orderBy))

                SelectColumnsAndAliasMap selectColumnsAndAliasMap = createSelectList(modelClass, modelClass)

                sql.append(selectColumnsAndAliasMap.selectClause.toString())
                sql.replace(sql.size() - 2, sql.size(), ' ')

                sql << "FROM $tbl "

                String orderBySQL = createOrderByClause(orderBy, selectColumnsAndAliasMap.columnAliasMap)
                sql.append(orderBySQL)


                return doPageSelect(pageSize, page, sql, values, orderBySQL, tbl, modelClass, selectColumnsAndAliasMap.columnAliasMap, null)

        }

        private String createPagingAfterSelect(Integer page, Integer pageSize, String orderBy) {
                StringBuilder sql = new StringBuilder()

                if (page != null && pageSize != null) {
                        if (orderBy == null)
                                throw new RuntimeException('orderBy: [columnName] required')

                        sql.append("FIRST ").append(pageSize).append(" SKIP ").append(getSkip(pageSize, page))
                }

                sql.toString()
        }

        private String createPagingAfterOrderBy(Integer page, Integer pageSize, String orderBy) {
                StringBuilder sql = new StringBuilder()

                if (page != null && pageSize != null) {
                        if (orderBy == null)
                                throw new RuntimeException('orderBy: [columnName] required')

                        def skip = getSkip(pageSize, page)
                        sql.append(" LIMIT $pageSize OFFSET ${skip}")

                }

                sql.toString()
        }

        UpdateColumnsAndValues createUpdateColumns(Object modelInstance) {
                Class clazz = modelInstance.getClass()
                String tableName = getTableName(clazz)

                def values = []
                def sql = new StringBuilder()
                def columnName

                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                sql << "UPDATE $tableName SET "

                this.dynamicProperties.each {
                        if (hasColumnsProperty && this?.columns.containsKey(it)) columnName = unCamelCaseColumn(this?.columns[it]) else
                                columnName = unCamelCaseColumn(it)

                        sql << "$columnName = ?, "

                        def _type = getType(clazz, it)
                        values << getValue(_type, this?."$it")
                }

                sql.replace(sql.size() - 2, sql.size(), '')

                sql << " WHERE "

                clazz.primaryKeys.each {
                        columnName = unCamelCaseColumn(it)
                        sql << "$columnName = ? and "
                        def val = this?."$it"
                        values << getValue(val?.class, val)
                }

                sql.replace(sql.size() - 6, sql.size(), '')

                //logger.finer sql.toString()

                def db = EasyOM.getSqlInstance(null)
                db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))

        }

        public Object findAll(String cls, Map whereMap) {
                Class clazz = Class.forName(cls, false, clsLoader)

                def values = []
                def sql = new StringBuilder()
                String columnName
                def tbl = getTableName(clazz)
                def orderBy
                def page
                def pageSize
                def operator = "AND"
                def countQuery


                if (whereMap instanceof Map) {
                        orderBy = whereMap.remove("orderBy")
                        page = whereMap.remove("page")
                        pageSize = whereMap.remove("pageSize")
                }

                sql << "SELECT "
                Map columns = [:]
                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                createSelectList(clazz, hasColumnsProperty, sql, columns, clazz)

                sql.replace(sql.size() - 2, sql.size(), ' ')

                def start = sql.length()

                sql << "FROM $tbl WHERE "
                WhereClauseAndValues whereClauseAndValues = createWhereClause(clazz,whereMap)
                sql.append(whereClauseAndValues.whereClause)

                countQuery = "SELECT count(*) " + sql.toString().substring(start)
                logger.fine("countQuery: ${countQuery}, values: ${whereClauseAndValues.values}")
                createOrderByClause(sql, orderBy, clazz)

                //logger.finer sql.toString()

                return doPageSelect(pageSize, page, sql, whereClauseAndValues.values, orderBy, tbl, clazz, columns, countQuery)

        }

        private WhereClauseAndValues createWhereClause(Class modelClass, Map whereMap) {
                WhereClauseAndValues whereClauseAndValues = new WhereClauseAndValues()

                StringBuilder sql = new StringBuilder()
                String operator = "AND"
                String columnName
                boolean hasColumnsProperty = isProperty(modelClass, 'columns')

                if (whereMap.containsKey('operator')) {
                        if (whereMap.operator.toUpperCase() == 'OR' || whereMap.operator.toUpperCase() == 'AND') {
                                operator = whereMap.remove('operator').toUpperCase()
                        }
                }

                whereMap.each {
                        def filterOperator
                        def key = it.key
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

                        if (it.value == null) {
                                sql << "$columnName IS NULL $operator "
                        } else if (filterOperator) {
                                sql << "$columnName $filterOperator ?  $operator "
                                whereClauseAndValues.values << getValue(it?.value?.class, it.value)
                        } else {
                                sql << "$columnName = ?  $operator "
                                whereClauseAndValues.values << getValue(it?.value?.class, it.value)
                        }
                }

                sql.replace(sql.size() - 4, sql.size(), '')

                whereClauseAndValues.whereClause = sql.toString()
                whereClauseAndValues
        }

        public Object find(String cls, Map whereMap) {
                Class clazz = Class.forName(cls, false, clsLoader)

                def values = []
                def sql = new StringBuilder()
                String columnName
                def tbl = getTableName(clazz)
                def operator = "AND"

                if (whereMap.containsKey('operator'))
                        if (whereMap.operator.toUpperCase() == 'OR' || whereMap.operator.toUpperCase() == 'AND')
                                operator = whereMap.remove('operator').toUpperCase()

                sql << "SELECT "
                Map columns = [:]
                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                createSelectList(clazz, hasColumnsProperty, sql, columns, clazz)

                sql.replace(sql.size() - 2, sql.size(), ' ')

                sql << "FROM $tbl WHERE "

                whereMap.each {
                        def filterOperator
                        def key = it.key
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

                        if (hasColumnsProperty && clazz.columns?.containsKey(key)) {
                                columnName = clazz.columns[key]
                        } else {
                                columnName = unCamelCaseColumn(key)
                        }

                        if (it.value == null) {
                                sql << "$columnName IS NULL $operator "
                        } else if (filterOperator) {
                                sql << "$columnName $filterOperator ?  $operator "
                                values << getValue(it?.value?.class, it.value)
                        } else {
                                sql << "$columnName = ?  $operator "
                                values << getValue(it?.value?.class, it.value)
                        }

//                                sql << "$columnName = ?  $operator "
                        //                                values << getValue(it?.value?.class,it?.value)
                }

                sql.replace(sql.size() - 4, sql.size(), '')

                //logger.finer sql.toString()

                def db = EasyOM.getSqlInstance(null)
                List results = new ArrayList()

                db.eachRow(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?'))) { rs ->
                        def row = clazz.newInstance()
                        columns.each { col ->
                                row."$col.key" = getSelectValue(getType(clazz, "$col.key"), rs."$col.key")
                        }
                        row.clearDynamicProperties()
                        results << row
                }

                if (results.size() == 0)
                        return null

                results.get(0)
        }


        public Object delete(String cls, Map whereMap) {
                Class clazz = Class.forName(cls, false, clsLoader)

                def values = []
                def sql = new StringBuilder()
                def columnName
                def tbl = getTableName(clazz)
                def operator = "AND"

                if (whereMap.containsKey('operator')) if (whereMap.operator.toUpperCase() == 'OR' || whereMap.operator.toUpperCase() == 'AND') operator = whereMap.remove('operator').toUpperCase()

                sql << "DELETE FROM $tbl WHERE "
                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                whereMap.each {
                        if (hasColumnsProperty && clazz.columns?.containsKey(it.key)) {
                                columnName = clazz.columns[it.key]
                        } else {
                                columnName = unCamelCaseColumn(it.key)
                        }

                        sql << "$columnName = ?  $operator "
                        values << getValue(it?.value?.class, it.value)
                }

                sql.replace(sql.size() - 6, sql.size(), '')

                //logger.finer sql.toString()

                def db = EasyOM.getSqlInstance(null)
                db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))
        }

        public void setProperty(String name, Object value) {
                MetaProperty metaProperty = this.class.metaClass.getMetaProperty(name)

                if (metaProperty instanceof org.codehaus.groovy.runtime.metaclass.ThreadManagedMetaBeanProperty) {
                        metaProperty.setThreadBoundPropertyValue(this, name, value)
                } else {
                        metaProperty.setProperty(this, value)
                        if (!this.dynamicProperties.contains(name)) this.dynamicProperties << name
                        logger.finest("setProperty $name")
                }
        }

        def properties(Map params) {
                populateBean(this, params)
        }

        def populateBean(Object obj, Map parameters) {

                try {
                        Object value = null;
                        String property = null;

                        BeanInfo sourceInfo = Introspector.getBeanInfo(obj.getClass());
                        PropertyDescriptor[] sourceDescriptors = sourceInfo.getPropertyDescriptors();

                        for (int x = 0; x < sourceDescriptors.length; x++) {
                                try {

                                        if (sourceDescriptors[x].getReadMethod() != null && sourceDescriptors[x].getWriteMethod() != null) {

                                                property = sourceDescriptors[x].getName();
                                                def params = sourceDescriptors[x].getWriteMethod().getParameterTypes();

                                                String val = parameters.get(property);
                                                if (val != null) {
                                                        value = castToType(val, Class.forName(params[0].getName()));

                                                        if (obj instanceof GroovyObject) {
                                                                MetaClass metaClass = InvokerHelper.getMetaClass(obj);
                                                                metaClass.setProperty(obj, sourceDescriptors[x].getName(), value);
                                                        } else {
                                                                sourceDescriptors[x].getWriteMethod().invoke(obj, value);
                                                        }
                                                }
                                        }
                                } catch (Exception e) {
                                        logger.warn("BeanUtil.populate failed. method=" + property + ", value=" + value, e);
                                }
                        }

                } catch (Exception e) {
                        logger.error("Error occurred populating object from request", e);
                }
                return obj;
        }


        def delete() {
                Class clazz = this.getClass()
                String tableName = getTableName(clazz)

                def values = []
                def sql = new StringBuilder()
                def columnName

                sql << "DELETE FROM $tableName WHERE "

                clazz.primaryKeys.each {
                        columnName = unCamelCaseColumn(it)
                        sql << "$columnName = ?, and "
                        def val = this?."$it"
                        values << getValue(val?.class, val)
                }

                sql.replace(sql.size() - 6, sql.size(), '')

                //logger.finer sql.toString()

                def db = EasyOM.getSqlInstance(null)
                db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))
        }

        def update() {
                Class clazz = this.getClass()
                String tableName = getTableName(clazz)

                def values = []
                def sql = new StringBuilder()
                def columnName

                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                sql << "UPDATE $tableName SET "

                this.dynamicProperties.each {
                        if (hasColumnsProperty && this?.columns.containsKey(it)) columnName = unCamelCaseColumn(this?.columns[it]) else
                                columnName = unCamelCaseColumn(it)

                        sql << "$columnName = ?, "

                        def _type = getType(clazz, it)
                        values << getValue(_type, this?."$it")
                }

                sql.replace(sql.size() - 2, sql.size(), '')

                sql << " WHERE "

                clazz.primaryKeys.each {
                        columnName = unCamelCaseColumn(it)
                        sql << "$columnName = ? and "
                        def val = this?."$it"
                        values << getValue(val?.class, val)
                }

                sql.replace(sql.size() - 6, sql.size(), '')

                //logger.finer sql.toString()

                def db = EasyOM.getSqlInstance(null)
                db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))

        }


        def insert(Boolean insertUpdatedColumnsOnly) {
                Class clazz = this.getClass()
                String tableName = getTableName(clazz)

                def columnName
                def values = []
                StringBuilder s = new StringBuilder()

                s.append "INSERT INTO $tableName ("
                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                def allColumns = getAllColumns(clazz, this)

                def properties = allColumns
                if (insertUpdatedColumnsOnly) properties = this.dynamicProperties

                boolean manualPrimaryKeys

                this.primaryKeys.each {
                        if (this.dynamicProperties.contains(it)) {
                                manualPrimaryKeys = true
                        } else {
                                manualPrimaryKeys = false
                        }
                }

                if (!manualPrimaryKeys) {
                        properties.removeAll(this.primaryKeys)
                }

                if (isProperty(clazz, 'exclude')) {
                        properties.removeAll(this.exclude)
                }

                properties.each {
                        if (hasColumnsProperty && this.columns?.containsKey(it)) {
                                s << this.columns[it]
                        } else {
                                s << unCamelCase(it)
                        }

                        s << ', '
                }
                s.replace(s.size() - 2, s.size(), '')

                s << ') VALUES ('

                properties.each {
                        s << '?,'
                        def val = this."$it"
                        values << getValue(val?.class, val)
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
                s.replace(s.size() - 1, s.size(), '')
                s << ')'

                //logger.finer s.toString()

                def db = EasyOM.getSqlInstance(this._dataSource)
                db.executeUpdate(new GStringImpl(values.toArray(), s.toString().trim().split('\\?')))
        }

        private def getValue(obj, val) {
                if (val == null) return null

                if (obj == java.sql.Date.class || obj == java.util.Date.class) {
                        return new java.sql.Timestamp(val.time)
                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
                        return (val == true ? '1' : '0')
                } else {
                        return val
                }
        }

        private String getTableName(Class clazz) {

                String tableName

                if (propertyExist(clazz, 'tableName')) {
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

        private def getAllColumns(clazz, thisObject) {
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

        public def getAllProperties(Class clazz) {
                def properties = []

                try {
                        Object value = null
                        String property = null

                        BeanInfo sourceInfo = Introspector.getBeanInfo(clazz)
                        PropertyDescriptor[] sourceDescriptors = sourceInfo.getPropertyDescriptors()

                        for (int x = 0; x < sourceDescriptors.length; x++) {
                                try {
                                        if (sourceDescriptors[x].getReadMethod() != null && sourceDescriptors[x].getWriteMethod() != null) {
                                                properties << sourceDescriptors[x].getName()
                                        }
                                } catch (Exception e) {
                                        throw e
                                }
                        }

                } catch (Throwable e) {
                        throw e
                }

                return properties
        }

        private Map<String, String> createColumnAliasMap(Class clazz, Object thisObject) {

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

        private SelectColumnsAndAliasMap createSelectList(Class clazz, Object thisObject) {
                boolean hasColumnsProperty = isProperty(clazz, 'columns')
                StringBuilder sql = new StringBuilder()
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
                                                        sql << thisObject.columns[it.name] << ' as ' << it.name
                                                        columns[it.name] = thisObject.columns[it.name]
                                                } else {
                                                        sql << unCamelCaseColumn(it.name) << ' as ' << it.name
                                                        columns[it.name] = unCamelCaseColumn(it.name)
                                                }
                                                sql << ', '

                                        }
                                }
                        }
                }

                new SelectColumnsAndAliasMap(selectClause: sql.toString(), createColumnAliasMap: columns)
        }


        boolean isDynamicProperty(Class clazz, String propertyName) {
                def metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                if (metaProperty instanceof ThreadManagedMetaBeanProperty) {
                        return true
                } else {
                        return false
                }
        }

        boolean isProperty(Class clazz, String propertyName) {
                def metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                if (metaProperty instanceof MetaBeanProperty) {
                        return true
                } else {
                        return false
                }
        }

        boolean propertyExist(Class clazz, String propertyName) {
                MetaBeanProperty metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                if (metaProperty != null) {
                        return true
                } else {
                        return false
                }
        }

        Class getType(Class clazz, String propertyName) {
                MetaBeanProperty metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                return metaProperty.getSetter().getNativeParameterTypes()[0]
        }

        def getColumnName(Class clazz, String propertyName) {

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

                return Model.unCamelCaseColumn(propertyName)
        }

        private def createOrderByClause(String orderBy, Class modelClass) {
                StringBuilder sql = new StringBuilder()

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

        private def doPageSelect(pageSize, page, def sql, List values, orderBy, tbl, clazz, Map columns, countQuery) {
                def db = EasyOM.getSqlInstance(null)
                List results = new ArrayList()

                def totalCountQuery = countQuery
                if (countQuery == null) totalCountQuery = "SELECT count(*) FROM $tbl".toString()

                def pagedResults

                if (page != null && pageSize != null) {
                        if (orderBy == null) throw new RuntimeException('orderBy: [columnName] required')
                        logger.fine("doPageSelect countQuery: ${countQuery}, values: ${values}")
                        pagedResults = doPagedResults(sql.toString(), totalCountQuery, page, pageSize, (countQuery == null ? null : values))

                        sql = pagedResults.sql
                }

                try {
                        db.eachRow(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?'))) { rs ->
                                def row = clazz.newInstance()
                                columns.each { col ->
                                        row."$col.key" = getSelectValue(getType(clazz, "$col.key"), rs."$col.key")
                                }
                                row.clearDynamicProperties()
                                results << row
                        }
                } catch (Exception e) {
                        e.printStackTrace()
                }

                if (page != null && pageSize != null) {
                        pagedResults.results = results
                        return pagedResults
                }

                return results
        }

        def doPagedResults(def sql, def totalCountQuery, def page, int pageSize, def parameterValues) {
                def newSQL = createPagingQuery(sql, page, pageSize)
                logger.fine("countQuery parameters: ${parameterValues}")

                def totalCount = totalCountQuery.executeScalar(parameterValues)
                def pageCount = (int) Math.ceil(totalCount / pageSize)

                logger.finer """doPagingQuery: $newSQL
                                recordCount:  $totalCount
                                totalNumberOfPages: $pageCount
                                page: $page"""

                [recordCount: totalCount, 'sql': newSQL, pageCount: pageCount, page: page]
        }

        def createPagingQuery(def sql, def page, int pageSize) {

                if (propertiesFile.getProperty('database.driver').indexOf('firebird') > -1) {
                        Object[] gs = new Object[0];
                        return new GStringImpl(gs, sql.replaceFirst("SELECT", "SELECT FIRST " + pageSize + " SKIP " + getSkip(pageSize, page)).split("/?"))
                } else if (propertiesFile.getProperty('database.driver', '').indexOf('mysql') > -1) {
                        def skip = getSkip(pageSize, page)
                        return sql.plus(" LIMIT $pageSize OFFSET ${skip}")
                }

        }

        def getSkip(Integer pageSize, Integer page) {
                if (page >= 1) {
                        return (pageSize * (page - 1))
                } else {
                        return 0
                }
        }

        String createRecordCountQuery(def sql) {
                def newSQL = sql.toString().replaceAll("\n", " ").replaceAll("\t", " ")

                def index = newSQL.indexOf(" FROM ")
                def orderByIndex = newSQL.toLowerCase().indexOf(" order by ")

                if (index == -1) {
                        throw new RuntimeException("What?, a paging query must have a \"FROM\" section. The \"FROM\" in the main FROM section must be capitalized.  Don't use all capitals with in other from clauses.")
                }

                if (orderByIndex == -1) {
                        throw new RuntimeException("What?, a paging query must have an \"ORDER BY\" section.")
                        orderByIndex = sql.length()
                }

                return "SELECT COUNT(*) " + newSQL.substring(index, orderByIndex)

        }


        private def getSelectValue(obj, val) {
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

        private Object castToType(String valFromRequestParameter, Class clazz) {
                try {
                        if (clazz == Object.class) {
                                return valFromRequestParameter;
                        } else if (clazz.getName().equals("char")) {
                                return new Character(valFromRequestParameter.toCharArray()[0]);
                        } else if (clazz.getName().equals("int")) {
                                return new Integer(valFromRequestParameter);
                        } else if (clazz.getName().equals("long")) {
                                return new Long(valFromRequestParameter);
                        } else if (clazz.getName().equals("double")) {
                                return new Double(valFromRequestParameter);
                        } else if (clazz.getName().equals("float")) {
                                return new Float(valFromRequestParameter);
                        } else if (clazz.getName().equals("short")) {
                                return new Short(valFromRequestParameter);
                        } else if (clazz.getName().equals("boolean")) {
                                return new Boolean(valFromRequestParameter);
                        }

                        Constructor c = clazz.getConstructor(String.class);
                        return c.newInstance(valFromRequestParameter);

                } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                } catch (InvocationTargetException e) {
                        e.printStackTrace();
                } catch (InstantiationException e) {
                        e.printStackTrace();
                } catch (IllegalAccessException e) {
                        e.printStackTrace();
                }
                return null;
        }

        String camelCase(String column) {
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

        String unCamelCase(String column) {
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


        String unCamelCaseColumn(String column) {
                if (columnsNamesCamelCased == 'true') {
                        return unCamelCase(column)
                } else {
                        return column
                }
        }
}
