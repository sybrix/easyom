package sybrix.easyom

import groovy.sql.Sql

import java.beans.BeanInfo
import java.beans.Introspector
import java.beans.PropertyDescriptor
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import java.sql.Connection
import java.util.logging.Logger
import javax.naming.Context
import javax.sql.DataSource
import org.codehaus.groovy.runtime.GStringImpl
import org.codehaus.groovy.runtime.metaclass.ThreadManagedMetaBeanProperty
import static java.util.logging.Level.FINE

/**
 Copyright 2009, David Lee

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 * */

class EasyOM {
        private static final Logger logger = Logger.getLogger(EasyOM.class.getName())

        private Context envCtx
        private Properties prop
        private DataSource dataSource
        private Boolean dbConnectionTested = false
        private String databaseDialect

        private final ThreadLocal<Sql> sqlThreadLocal = new ThreadLocal<Sql>() {
                @Override
                protected Sql initialValue() {
                        return null
                }
        };

        EasyOM(Properties propertiesFile) {
                initializeDBProperties(propertiesFile)
        }

//        public void init(Properties propertiesFile, def app) {
//
//                initializeDBProperties(propertiesFile)
//
//                def path = app.appPath + File.separator + 'WEB-INF' + File.separator + app['model.package'].replace(".", File.separator)
//                def srcRoot = app.appPath + File.separator + 'WEB-INF' + File.separator
//
//                loadModelClasses(path, srcRoot, app)
//        }

        public synchronized void injectMethods(Class clazz) {
                addDAOProperties(clazz)
                overrideSetProperty(clazz)
                addClearMethod(clazz)
                addSaveMethod(clazz)
                addInsertMethod(clazz)
                addUpdateMethod(clazz)
                addDeleteMethod(clazz)

                addStaticDeleteMethod(clazz)
                addStaticFindMethod(clazz)
                addStaticFindAllMethod(clazz)
                addStaticListMethod(clazz)

                logger.info('EasyOM loaded class:' + clazz.name)
        }

        private def initializeDBProperties(Properties propertiesFile) {
                def db
                prop = propertiesFile

                if (!dbConnectionTested){
                        try {
                                db = newSqlInstance(null)
                                dbConnectionTested = true
                        } catch (Throwable e) {
                                //e.printStackTrace()
                                logger.log(FINE, "newSqlInstance() failed", e)
                        } finally {
                                if (db != null)
                                        db.close()
                        }
                }


                addStringExecuteQuery()
                addStringExecuteUpdate()
                addGStringExecuteUpdate()
                addStringExecuteScalar()
        }

        public void addStringExecuteScalar() {
                GString.metaClass.executeScalar = {

                        def sql = delegate

                        def db = getSqlInstance(null)
                        List results

//                        if (sql instanceof String)
//                                def result = easyom.db.rows(new GStringImpl(values?.toArray() ?: [].toArray(), sql.toString().trim().split('\\?')))
//                        else
                        def result = db.rows(delegate)

                        if (result.size > 0)
                                return result[0].getAt(0)

                        return null
                }

                String.metaClass.executeScalar = { args ->
//                        String[] values = new String[0]
//                        String[] sql = new String[1]
                        def values = []
                        if (args instanceof List) {
                                args.each {
                                        values.add(it)
                                }
                        } else {
                                if (args != null)
                                        values << args
                        }
                        String sql = delegate


                        GString gs = new GStringImpl(values.toArray(), sql.trim().split('\\?'))
                        gs.executeScalar()

                }

        }

        public void addStringExecuteQuery() {

                GString.metaClass.executeQuery = { Object[] args ->
                        GString sql = delegate
                        def values = delegate.values
                        def clazz
                        def pageSize
                        def page

                        if (args.size() == 1 && args[0] instanceof Map) {
                                clazz = args[0].resultClass
                                page = args[0].page
                                pageSize = args[0].pageSize

                        } else if (args.size() == 1 && args[0] instanceof Class) {
                                clazz = args[0]
                        }

                        def val = []
                        def db = getSqlInstance(null)
                        List results = new ArrayList()

                        def totalCountQuery
                        def pagedResults

                        if (page != null && pageSize != null) {
                                totalCountQuery = createRecordCountQuery(sql)
                                pagedResults = doPagedResults(sql, totalCountQuery, page, pageSize, null)
                                sql = pagedResults.sql
                        }

                        logger.finer delegate.toString()

                        if (clazz == null) {
                                results = db.rows(sql)
                        } else {


                                Map columns = [:]
                                boolean hasColumnsProperty = isProperty(clazz, 'columns')

                                clazz.declaredFields.each {
                                        def prop = clazz.metaClass.getMetaProperty(it.name)

                                        Object cls = clazz.newInstance()
                                        if (clazz.metaClass.getMetaProperty(it.name) && !it.name.equals('metaClass') && (prop instanceof MetaBeanProperty)) {
                                                if (!prop.field.isStatic() && !it.name.equalsIgnoreCase('pkColumn')) {
                                                        columns[it.name.toUpperCase()] = it.name
                                                }
                                        }
                                }

                                //easyom.db.eachRow(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?'))) {rs ->
                                db.eachRow(sql) { rs ->
                                        def row = clazz.newInstance()
                                        for (i in 1..rs.getMetaData().getColumnCount()) {

                                                String colName = columns[rs.getMetaData().getColumnLabel(i).toUpperCase()]
                                                if (colName != null)
                                                        row."$colName" = getValue(getType(clazz, "$colName"), rs."$colName")
                                        }
                                        row.clearDynamicProperties()
                                        results << row
                                }
                        }

                        if (page != null && pageSize != null) {
                                pagedResults.results = results
                                return pagedResults
                        } else {
                                return results
                        }
                }

                String.metaClass.executeQuery = { Object[] args ->
                        String[] values = new String[0]
                        String[] sql = new String[1]
                        sql[0] = delegate.toString()

                        GString gs = new GStringImpl(values, sql)
                        gs.executeQuery(args)

                }
        }

        public void addStringExecuteUpdate() {
                String.metaClass.executeUpdate = { Object[] values ->
                        String sql = delegate

                        def db = getSqlInstance(null)
                        db.executeUpdate(new GStringImpl(values, sql.toString().trim().split('\\?')))
                }
        }

        public void addGStringExecuteUpdate() {
                GString.metaClass.executeUpdate = { Object[] values ->
                        GString sql = delegate

                        def db = getSqlInstance(null)
                        db.executeUpdate(sql)
                }
        }

        public void addDAOProperties(clazz) {
                clazz.metaClass.dataSource = ''
                clazz.metaClass.tableName = getTableName(clazz)
        }

        public void overrideSetProperty(clazz) {
                clazz.metaClass.setProperty = { String name, value ->
                        def metaProperty = clazz.metaClass.getMetaProperty(name)
                        if (metaProperty instanceof ThreadManagedMetaBeanProperty) {
                                metaProperty.setThreadBoundPropertyValue(delegate, name, value)
                        } else {
                                metaProperty.setProperty(delegate, value)
                                if (!delegate.dynamicProperties.contains(name))
                                        delegate.dynamicProperties << name
                                logger.finest("setProperty $name")
                        }
                }
        }

        public void addInsertMethod(clazz) {
                clazz.metaClass.insert = { Boolean insertUpdatedColumnsOnly ->
                        def columnName
                        def values = []
                        StringBuilder s = new StringBuilder()

                        s.append "INSERT INTO $tableName ("
                        boolean hasColumnsProperty = isProperty(clazz, 'columns')
                        def allColumns = getAllColumns(clazz, delegate)

                        def properties = allColumns
                        if (insertUpdatedColumnsOnly)
                                properties = delegate.dynamicProperties

                        boolean manualPrimaryKeys
                        delegate.primaryKeys.each {
                                if (delegate.dynamicProperties.contains(it)) {
                                        manualPrimaryKeys = true
                                } else {
                                        manualPrimaryKeys = false
                                }
                        }

                        if (!manualPrimaryKeys) {
                                properties.removeAll(delegate.primaryKeys)
                        }

                        properties.each {
                                if (hasColumnsProperty && delegate.columns?.containsKey(it)) {
                                        s << delegate.columns[it]
                                } else {
                                        s << unCamelCase(it)
                                }

                                s << ', '
                        }
                        s.replace(s.size() - 2, s.size(), '')

                        s << ') VALUES ('

                        properties.each {
                                s << '?,'
                                def val = delegate."$it"
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

                        logger.finer s.toString()

                        def db = getSqlInstance(delegate.dataSource)
                        db.executeUpdate(new GStringImpl(values.toArray(), s.toString().trim().split('\\?')))
                }
        }

        private def getSelectValue(obj, val) {
                if (val == null)
                        return

                if (obj == java.util.Date.class && val.class == java.sql.Timestamp) {
                        return new java.util.Date(val.time)
                } else if (obj == java.sql.Date.class) {
                        return new java.util.Date(val.time)
                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
                        //console  "EasyOM.getValue() " + val + " " + obj
                        return (val == '1' || val == 1 || val == 'true' || val == 't' || val == 'Y' || val == true) ? true : false
                } else {
                        return val
                }
        }

        private def getValue(obj, val) {
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

        public void addSaveMethod(clazz) {
                clazz.metaClass.save = { ->
                        save(false)
                }

                clazz.metaClass.save = { Boolean insertUpdatedColumns ->

                        boolean hasPrimaryKeyValues = false
                        clazz.primaryKeys.each {
                                def val = delegate?."$it"
                                if (val != null)
                                        hasPrimaryKeyValues = true
                        }

                        if (hasPrimaryKeyValues)
                                update()
                        else
                                insert(insertUpdatedColumns)
                }
        }

        private void addUpdateMethod(clazz) {
                clazz.metaClass.update = { ->
                        def values = []
                        def sql = new StringBuilder()
                        def columnName

                        boolean hasColumnsProperty = isProperty(clazz, 'columns')

                        sql << "UPDATE $tableName SET "

                        delegate.dynamicProperties.each {
                                if (hasColumnsProperty && delegate?.columns.containsKey(it))
                                        columnName = unCamelCaseColumn(delegate?.columns[it])
                                else
                                        columnName = unCamelCaseColumn(it)

                                sql << "$columnName = ?, "

                                def _type = getType(clazz, it)
                                values << getValue(_type, delegate?."$it")
                        }

                        sql.replace(sql.size() - 2, sql.size(), '')

                        sql << " WHERE "

                        clazz.primaryKeys.each {
                                columnName = unCamelCaseColumn(it)
                                sql << "$columnName = ? and "
                                def val = delegate?."$it"
                                values << getValue(val?.class, val)
                        }

                        sql.replace(sql.size() - 6, sql.size(), '')

                        logger.finer sql.toString()

                        def db = getSqlInstance(null)
                        db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))
                }
        }

        private void addStaticFindMethod(clazz) {

                clazz.metaClass.static.find = { whereMap ->
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

                        createSelectList(clazz, hasColumnsProperty, sql, columns, delegate)

                        sql.replace(sql.size() - 2, sql.size(), ' ')

                        sql << "FROM $tbl WHERE "

                        whereMap.each {
                                if (hasColumnsProperty && delegate.columns?.containsKey(it.key)) {
                                        columnName = delegate.columns[it.key]
                                } else {
                                        columnName = unCamelCaseColumn(it.key)
                                }

                                if (it.value == null) {
                                        sql << "$columnName IS NULL $operator "
                                } else {
                                        sql << "$columnName = ?  $operator "
                                        values << getValue(it?.value?.class, it.value)
                                }

//                                sql << "$columnName = ?  $operator "
//                                values << getValue(it?.value?.class,it?.value)
                        }

                        sql.replace(sql.size() - 4, sql.size(), '')

                        logger.finer sql.toString()

                        def db = getSqlInstance(null)
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
        }

        private void addStaticFindAllMethod(clazz) {
                clazz.metaClass.static.findAll = { whereMap ->
                        def values = []
                        def sql = new StringBuilder()
                        String columnName
                        def tbl = getTableName(clazz)
                        def orderBy
                        def page
                        def pageSize
                        def operator = "AND"
                        def countQuery

                        if (whereMap.containsKey('operator'))
                                if (whereMap.operator.toUpperCase() == 'OR' || whereMap.operator.toUpperCase() == 'AND')
                                        operator = whereMap.remove('operator').toUpperCase()

                        if (whereMap instanceof Map) {
                                orderBy = whereMap.remove("orderBy")
                                page = whereMap.remove("page")
                                pageSize = whereMap.remove("pageSize")
                        }

                        sql << "SELECT "
                        Map columns = [:]
                        boolean hasColumnsProperty = isProperty(clazz, 'columns')

                        createSelectList(clazz, hasColumnsProperty, sql, columns, delegate)

                        sql.replace(sql.size() - 2, sql.size(), ' ')

                        def start = sql.length()

                        sql << "FROM $tbl WHERE "
                        whereMap.each {
                                if (hasColumnsProperty && delegate.columns?.containsKey(it.key)) {
                                        columnName = delegate.columns[it.key]
                                } else {
                                        columnName = unCamelCaseColumn(it.key)
                                }

                                //console  'columnName:'  + columnName + ' value=' + it.value

                                if (it.value == null) {
                                        sql << "$columnName IS NULL $operator "
                                } else {
                                        sql << "$columnName = ?  $operator "

                                        values << getValue(it?.value?.class, it.value)
                                }
                        }

                        sql.replace(sql.size() - 4, sql.size(), '')
                        countQuery = "SELECT count(*) " + sql.toString().substring(start)
                        logger.fine("countQuery: ${countQuery}, values: ${values}")
                        parseOrderBy(sql, orderBy, clazz)

                        logger.finer sql.toString()

                        return doPageSelect(pageSize, page, sql, values, orderBy, tbl, clazz, columns, countQuery)
                }
        }

        private void addStaticListMethod(clazz) {
                clazz.metaClass.static.list = { optionsMap ->
                        def values = []
                        def sql = new StringBuilder()
                        String columnName
                        def tbl = getTableName(clazz)
                        def orderBy
                        def page
                        def pageSize
                        def limit

                        if (optionsMap instanceof Map) {
                                orderBy = optionsMap.remove("orderBy")
                                page = optionsMap.remove("page")
                                pageSize = optionsMap.remove("pageSize")
                                limit = optionsMap.remove("limit")
                        }

                        sql << "SELECT "
                        Map columns = [:]
                        boolean hasColumnsProperty = isProperty(clazz, 'columns')

                        createSelectList(clazz, hasColumnsProperty, sql, columns, delegate)

                        sql.replace(sql.size() - 2, sql.size(), ' ')

                        sql << "FROM $tbl "

                        parseOrderBy(sql, orderBy, clazz)

//                                           if (isNumeric(limit))
//                                                sql << " LIMIT $limit"
                        logger.finer sql.toString()

                        return doPageSelect(pageSize, page, sql, values, orderBy, tbl, clazz, columns, null)
                }
        }

        private void addClearMethod(clazz) {
                clazz.metaClass.clearDynamicProperties = { ->
                        delegate?.dynamicProperties.clear()
                }
        }

        private void addDeleteMethod(clazz) {
                clazz.metaClass.delete = { ->
                        def values = []
                        def sql = new StringBuilder()
                        def columnName

                        sql << "DELETE FROM $tableName WHERE "

                        clazz.primaryKeys.each {
                                columnName = unCamelCaseColumn(it)
                                sql << "$columnName = ?, and "
                                def val = delegate?."$it"
                                values << getValue(val?.class, val)
                        }

                        sql.replace(sql.size() - 6, sql.size(), '')

                        logger.finer sql.toString()

                        def db = getSqlInstance(null)
                        db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))
                }
        }

        private void addStaticDeleteMethod(clazz) {
                clazz.metaClass.static.delete = { Map whereMap ->
                        def values = []
                        def sql = new StringBuilder()
                        def columnName
                        def tbl = getTableName(clazz)
                        def operator = "AND"

                        if (whereMap.containsKey('operator'))
                                if (whereMap.operator.toUpperCase() == 'OR' || whereMap.operator.toUpperCase() == 'AND')
                                        operator = whereMap.remove('operator').toUpperCase()

                        sql << "DELETE FROM $tbl WHERE "
                        boolean hasColumnsProperty = isProperty(clazz, 'columns')
                        whereMap.each {
                                if (hasColumnsProperty && delegate.columns?.containsKey(it.key)) {
                                        columnName = delegate.columns[it.key]
                                } else {
                                        columnName = unCamelCaseColumn(it.key)
                                }

                                sql << "$columnName = ?  $operator "
                                values << getValue(it?.value?.class, it.value)
                        }

                        sql.replace(sql.size() - 6, sql.size(), '')

                        logger.finer sql.toString()

                        def db = getSqlInstance(null)
                        db.executeUpdate(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?')))
                }
        }

        private def getTableName(Class clazz) {
                String tableName

                if (propertyExist(clazz, 'tableName')) {
                        def metaProperty = clazz.metaClass.getMetaProperty('tableName')
                        if (metaProperty instanceof ThreadManagedMetaBeanProperty)
                                return metaProperty.initialValue else
                                return metaProperty.getProperty('tableName')
                } else {
                        if (clazz.name.lastIndexOf('.') > -1) {
                                tableName = clazz.name.substring(clazz.name.lastIndexOf('.') + 1)
                        } else {
                                tableName = clazz.name
                        }
                }

                if (prop.getProperty("camel.case.table.name") == 'true') {
                        tableName = unCamelCase(tableName)
                }

                if (prop.getProperty("use.table.prefix") == 'true') {
                        return prop.getProperty("table.prefix") + tableName
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

                                        if (clazz.metaClass.getMetaProperty(property) && !property.equals('metaClass') && (prop instanceof MetaBeanProperty)) {

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


        private def createSelectList(clazz, boolean hasColumnsProperty, sql, Map columns, Object thisObject) {
                clazz.declaredFields.each {
                        def prop = clazz.metaClass.getMetaProperty(it.name)
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

        def String camelCase(String column) {
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

        def String unCamelCase(String column) {
                StringBuffer newColumn = new StringBuffer()
                for (int i = 0; i < column.length(); i++) {
                        if (Character.isLetter(column.charAt(i)) && Character.isUpperCase(column.charAt(i))) {
                                if (i > 0)
                                        newColumn.append("_")

                                newColumn.append(Character.toLowerCase(column.charAt(i)))
                        } else {
                                newColumn.append(column.charAt(i))
                        }
                }

                return newColumn.toString()
        }

        def javax.sql.DataSource getDataSource(String dataSourceName) {
                // Look up our data source
                DataSource ds = (DataSource) envCtx.lookup(dataSourceName)
                // Allocate and use a connection from the pool
                return ds
        }

        def Sql getSqlInstance(String dataSourceName) {
                logger.fine 'Obtaining SQL Instance from  threadlocal'
                def db = sqlThreadLocal.get()
                if (db != null)
                        return db

                return newSqlInstance(dataSourceName)

        }

        def camelCaseColumn(String column) {
                if (prop.getProperty('camel.case.column.name') == 'true') {
                        return camelCase(column)
                } else {
                        return column
                }
        }

        def unCamelCaseColumn(String column) {
                if (prop.getProperty('camel.case.column.name') == 'true') {
                        return unCamelCase(column)
                } else {
                        return column
                }
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

                return unCamelCaseColumn(propertyName)
        }

        private def parseOrderBy(sql, orderBy, clazz) {
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

        private def doPageSelect(pageSize, page,
                                        def sql, List values, orderBy, tbl, clazz, Map columns, countQuery) {
                def db = getSqlInstance(null)
                List results = new ArrayList()

                def totalCountQuery = countQuery
                if (countQuery == null)
                        totalCountQuery = "SELECT count(*) FROM $tbl".toString()

                def pagedResults

                if (page != null && pageSize != null) {
                        if (orderBy == null)
                                throw new RuntimeException('orderBy: [columnName] required')
                        logger.fine("doPageSelect countQuery: ${countQuery}, values: ${values}")
                        pagedResults = doPagedResults(sql.toString(), totalCountQuery, page, pageSize, (countQuery == null ? null : values))

                        sql = pagedResults.sql
                }

                db.eachRow(new GStringImpl(values.toArray(), sql.toString().trim().split('\\?'))) { rs ->
                        def row = clazz.newInstance()
                        columns.each { col ->
                                row."$col.key" = getSelectValue(getType(clazz, "$col.key"), rs."$col.key")
                        }
                        row.clearDynamicProperties()
                        results << row
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

                if (databaseDialect.equals("firebird")) {
                        Object[] gs = new Object[0];
                        return new GStringImpl(gs, sql.replaceFirst("SELECT", "SELECT FIRST " + pageSize + " SKIP " + getSkip(pageSize, page)).split("/?"))
                } else if (prop.getProperty('database.driver', '').indexOf('mysql') > -1) {
                        def skip = getSkip(pageSize, page)
                        return sql.plus(" LIMIT $pageSize OFFSET ${skip}")
                }

        }

        def getSkip(int pageSize, def page) {
                if (page >= 1) {
                        return (pageSize * (page - 1))
                } else {
                        page = 1
                        return 0
                }
        }

        String createRecordCountQuery(def sql) {
                def newSQL = sql.toString().replaceAll("\n", " ").replaceAll("\t", " ")

                int index = newSQL.indexOf(" FROM ")
                int orderByIndex = newSQL.toLowerCase().indexOf(" order by ")

                if (index == -1) {
                        throw new RuntimeException("What?, a paging query must have a \"FROM\" section. The \"FROM\" in the main FROM section must be capitalized.  Don't use all capitals with in other from clauses.")
                }

                if (orderByIndex == -1) {
                        throw new RuntimeException("What?, a paging query must have a \"FROM\" section.")
                        orderByIndex = sql.length()
                }


                return "SELECT COUNT(*) " + newSQL.substring(index, orderByIndex)

        }

//        def loadModelClasses(path, root, app) {
//                new File(path.toString()).eachFile {
//                        if (!it.isDirectory()) {
//                                def cls = it.absolutePath.substring(root.size()) //.replaceAll("\\\\", ".").replaceAll("/",".")
//                                injectMethods(app.classForName(cls))
//                        } else {
//                                loadModelClasses(it.absolutePath, root, app)
//                        }
//                }
//        }

        def withTransaction(Closure closure) {
                Sql db = getSqlInstance(null)

                db.cacheConnection { java.sql.Connection connection ->
                        try {
                                connection.setAutoCommit(false)

                                sqlThreadLocal.set(db)

                                closure.call()

                                connection.commit()
                        } catch (Throwable e) {
                                connection.rollback()
                                throw e
                        } finally {

                                if (db != null)
                                        try {
                                                db.close()
                                        }catch(Exception e){

                                        }

                                sqlThreadLocal.set(null)
                        }
                }
        }

        public Sql newSqlInstance(String dataSourceName) {
                try {
                        if (dataSourceName == null) {
                                Sql db = sqlThreadLocal.get()
                                if (db != null && !db.getConnection().isClosed()) {
                                        return db;
                                }
                        }

                        if (dataSourceName == null) {
                                dataSourceName = "";
                        } else {
                                dataSourceName += ".";
                        }

                        String dataSourceClass = (String) prop.getString(dataSourceName + "datasource.class");

                        if (dataSource != null) {
                                Sql db = new Sql(dataSource)
                                return db;
                        }

                        if (dataSource == null && dataSourceClass != null) {

                                Class<?> dsClass = Class.forName(dataSourceClass);

                                dataSource = dsClass.newInstance();
                                Map<String, Object> dataSourceProperties = getDataSourceProperties(dataSourceName)
                                for (String property : dataSourceProperties.keySet()) {
                                        callMethod(dataSource, "set" + capFirstLetter(property), prop.getString(dataSourceProperties.get(property)))
                                }

                                Connection connection =  dataSource.connection
                                databaseDialect = getDatabaseDialect(connection.metaData.databaseProductName)
                                connection.close()

                                return new Sql(dataSource)

                        } else if (dataSource == null && dataSourceClass == null) {

                                String driver = prop.getProperty(dataSourceName + "database.driver")
                                String url = prop.getProperty(dataSourceName + "database.url")
                                String pwd = prop.getProperty(dataSourceName + "database.password");
                                String username = prop.getProperty(dataSourceName + "database.username")
                                if (databaseDialect == null){
                                        Connection connection = Sql.newInstance(url, username, pwd, driver)
                                        databaseDialect = getDatabaseDialect(connection.metaData.databaseProductName)
                                        connection.close()
                                }
                                return Sql.newInstance(url, username, pwd, driver);
                        }

                } catch (Exception e) {
                        throw new RuntimeException("newSqlInstance() failed. Make sure app['database.*]' properties are set and correct." + e.getMessage(), e);
                }

                return null;
        }

        String getDatabaseDialect(String s) {
                if (s.toLowerCase().contains("firebird")){
                        return "firebird"
                }
        }

        public Sql newSqlInstance() {
                newSqlInstance(null)
        }


        private Map<String, Object> getDataSourceProperties(String datasourceName) {
                Map<String, Object> dataSourceProperties = new HashMap<String, Object>();
                Enumeration<String> keyset = prop.keys()

                keyset.each { key ->
                        if (key.startsWith(datasourceName + "datasource.")) {
                                if (!key.equals(datasourceName + "datasource.class")) {
                                        dataSourceProperties.put(key.substring(key.lastIndexOf(".") + 1), key);
                                }
                        }
                }

                return dataSourceProperties;
        }

        public String capFirstLetter(String s) {
                return s.substring(0, 1).toUpperCase() + s.substring(1);
        }

        private void callMethod(Object ds, String methodName, Object parameterValue) {

                try {
                        Method method = null;

                        Method[] methods = ds.getClass().getMethods();
                        for (Method m : methods) {
                                if (m.getName().equals(methodName) && m.getParameterTypes().length == 1) {
                                        method = m;
                                }
                        }

                        Class<?> cls = method.getParameterTypes()[0];
                        if (cls.getName().contains("boolean")) {
                                cls = Boolean.class;
                        } else if (cls.getName().contains("int")) {
                                cls = Integer.class;
                        } else if (cls.getName().contains("long")) {
                                cls = Long.class;
                        } else if (cls.getName().contains("double")) {
                                cls = Double.class;
                        }

                        Constructor<?> constructor = cls.getConstructor(String.class);
                        Object val = constructor.newInstance(parameterValue.toString());

                        Method m = ds.getClass().getMethod(methodName, method.getParameterTypes()[0]);
                        m.invoke(ds, val);
                } catch (Throwable e) {
                        throw new RuntimeException("Error setting DataSource property. datasource=" + ds.toString() + ", methodName=" + methodName + ", " +
                                "url=" + parameterValue, e);
                }
        }
}





