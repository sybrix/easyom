package sybrix.easyom

import com.mysql.cj.jdbc.MysqlDataSource
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import sybrix.easyom.dialects.Dialect

import java.beans.BeanInfo
import java.beans.Introspector
import java.beans.PropertyDescriptor
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import java.sql.Blob
import java.sql.Connection
import java.sql.ResultSetMetaData
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

@Slf4j
class EasyOM {


        private Dialect dbDialect


        private Context envCtx
        private Properties prop
        private DataSource dataSource
        private Boolean dbConnectionTested = false

        private final ThreadLocal<Sql> sqlThreadLocal = new ThreadLocal<Sql>() {
                @Override
                protected Sql initialValue() {
                        return null
                }
        }

        EasyOM(Properties properties) {
                initializeDBProperties(properties)
                Class cls = Class.forName(properties.get("db.dialect"))
                dbDialect = cls.newInstance()
                dbDialect.init(properties)
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

                log.info('EasyOM loaded class:' + clazz.name)
        }

        private def initializeDBProperties(Properties propertiesFile) {
                def db
                prop = propertiesFile

                if (!dbConnectionTested) {
                        try {
                                db = newSqlInstance(null)
                                dbConnectionTested = true
                        } catch (Throwable e) {
                                //e.printStackTrace()
                                log.error("newSqlInstance() failed", e)
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
                        PagedResults pagedResults

                        if (page != null && pageSize != null) {
                                totalCountQuery = dbDialect.createRecordCountStringQuery(sql)
                                pagedResults = doPagedResults(sql, totalCountQuery, page, pageSize, null)
                                sql =  pagedResults.sql
                        }

                        log.debug delegate.toString()

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

                                                String colName = rs.getMetaData().getColumnLabel(i)
                                                String propertyName = columns[rs.getMetaData().getColumnLabel(i).toUpperCase()]

                                                if (propertyName == null) {
                                                        propertyName = columns[camelCase(rs.getMetaData().getColumnLabel(i).toLowerCase()).toUpperCase()]
                                                }
                                                if (propertyName == null) {
                                                        log.debug("propertyName not found for column ${colName} for class ${clazz.name}")
                                                }


                                                if (colName != null)
                                                        row."$propertyName" = dbDialect.getValue(getPropertyClassType(clazz, "$propertyName"), rs."$colName")
                                        }
                                        //row.clearDynamicProperties()
                                        results << row
                                }
                        }

                        if (page != null && pageSize != null) {
                                pagedResults.data = results
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

        public void addStringExecuteQuery1() {

                GString.metaClass.executeQuery = { Map args ->
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

                        SelectSqlStatement sqlStatement = new SelectSqlStatement(sqlGString: sql, values: values)
                        sqlStatement.selectColumnsAndAliasMap = dbDialect.createSelectColumnsAndAliasMap(clazz)

                        if (page != null && pageSize != null) {
                                def totalCountQuery = createRecordCountQuery(sql)
                                sqlStatement.countStatement = new SqlStatement(sqlGString: new GStringImpl(values.toArray(), totalCountQuery.trim().split('\\?')))
                        }

                        if (clazz == null) {
                                def db = getSqlInstance(null)
                                return db.rows(sqlStatement.sqlGString)
                        }

                        if (sqlStatement.countStatement)
                                doPageSelect(pageSize, page, sqlStatement, clazz)
                        else
                                doSelect(sqlStatement, clazz)
                }

                String.metaClass.executeQuery = { Map args ->
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
                                metaProperty.setThreadBoundPropertyValue(delegate, name, convert(value))
                        } else {

                                metaProperty.setProperty(delegate, convert(value))

                                if (!delegate.dynamicProperties.contains(name))
                                        delegate.dynamicProperties << name
                                log.debug("setProperty $name")
                        }
                }
        }

        def convert(def p) {
                if (p instanceof java.sql.Blob) {
                        new sybrix.easyom.Blob(p)
                } else {
                                p
                }
        }

        public void addInsertMethod(clazz) {
                clazz.metaClass.insert = { Boolean insertUpdatedColumnsOnly ->

                        SqlStatement sqlStatement = dbDialect.createInsertStatement(delegate, insertUpdatedColumnsOnly)

                        def db = getSqlInstance(delegate.dataSource)

                        for (int i = 0; i < sqlStatement.sqlGString.values.length; i++) {
                                if (sqlStatement.sqlGString.values[i] instanceof sybrix.easyom.Blob) {
                                        sqlStatement.sqlGString.values[i] = createBlob(db.dataSource.getConnection(), ((sybrix.easyom.Blob) sqlStatement.sqlGString.values[i]).toInputStream())
                                }
                        }
                        def l = db.executeInsert(sqlStatement.sqlGString)

                        if (l.size()>0) {
                                def id = l[0][0]

                                return id
                        }
                }
        }

//        private def getSelectValue(Class obj, val) {
//                if (val == null)
//                        return
//
//                if (obj == java.util.Date.class && val.class == java.sql.Timestamp) {
//                        return new java.util.Date(val.time)
//                } else if (obj == java.sql.Date.class) {
//                        return new java.util.Date(val.time)
//
//                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
//                        //console  "EasyOM.getValue() " + val + " " + obj
//                        return (val == '1' || val == 1 || val == 'true' || val == 't' || val == 'Y' || val == true) ? true : false
//                } else if (obj == java.lang.String.class && val instanceof java.sql.Blob) {
//                        java.sql.Blob blob = (java.sql.Blob) val
//                        byte[] data = new byte[1024]
//                        ByteArrayOutputStream out = new ByteArrayOutputStream()
//
//                        int c = 0;
//                        while (true) {
//                                c = blob.binaryStream.read(data)
//                                out.write(data, 0, c)
//                                if (out.size() >= blob.length()) {
//                                        break
//                                }
//                        }
//
//                        return out.toString()
//                } else if (obj == java.lang.String.class && val instanceof java.sql.Clob) {
//                        java.sql.Clob clob = (java.sql.Clob) val
//                        char[] data = new char[1024]
//                        StringBuffer sb = new StringBuffer()
//
//                        int c = 0;
//                        while (true) {
//                                c = clob.getCharacterStream().read(data)
//                                sb.append(data, 0, c)
//
//                                if (sb.length() >= sb.length()) {
//                                        break
//                                }
//                        }
//
//
//                        return sb.toString()
//
//                } else {
//                        return val
//                }
//        }

//        private def getValue(obj, val) {
//                if (val == null)
//                        return null
//
//                if (obj == java.sql.Date.class || obj == java.util.Date.class) {
//                        return new java.sql.Timestamp(val.time)
//                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
//                        return (val == true ? '1' : '0')
//                } else {
//                        return val
//                }
//        }

        public void addSaveMethod(clazz) {
                clazz.metaClass.save = { ->
                        save(false)
                }

                clazz.metaClass.save = { Boolean insertUpdatedColumns ->

                        boolean hasPrimaryKeyValues = false
                        clazz.primaryKey.each {
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

                        SqlStatement sqlStatement = dbDialect.createUpdateStatement(delegate)

                        def db = getSqlInstance(delegate.dataSource)
                        db.executeUpdate(sqlStatement.sqlGString)
                }
        }

        private void addStaticFindMethod(Class clazz) {

                clazz.metaClass.static.find = { parameters ->

                        WhereClauseParameters whereClauseParameters = createParameters(parameters)

                        SelectSqlStatement sqlStatement = dbDialect.createSelectStatement(clazz, whereClauseParameters)

                        def results = doSelect(sqlStatement, clazz)

                        if (results?.size() == 0)
                                return null

                        results.get(0)
                }
        }


        private void addStaticFindAllMethod(clazz) {
                clazz.metaClass.static.findAll = { parameters ->

                        WhereClauseParameters whereClauseParameters = createParameters(parameters)

                        SelectSqlStatement sqlStatement = dbDialect.createSelectStatement(clazz, whereClauseParameters)

                        if (sqlStatement.countStatement)
                                doPageSelect(whereClauseParameters.getPageSize(), whereClauseParameters.getPage(), sqlStatement, clazz)
                        else
                                doSelect(sqlStatement, clazz)
                }
        }

        private WhereClauseParameters createParameters(Map parameters) {
                WhereClauseParameters whereClauseParameters = new WhereClauseParameters()

                if (parameters instanceof Map) {
                        if (parameters.containsKey('operator'))
                                if (parameters.operator.toUpperCase() == 'OR' || parameters.operator.toUpperCase() == 'AND')
                                        whereClauseParameters.setOperator(parameters.remove('operator').toUpperCase())

                        whereClauseParameters.setOrderBy(parameters.get("orderBy"))
                        whereClauseParameters.setPage(parameters.get("page"))
                        whereClauseParameters.setPageSize(parameters.get("pageSize"))

                        if (parameters.get("countColumn")) {
                                whereClauseParameters.setCountColumn(parameters.get("countColumn"))
                        }

                        parameters.each { k, v ->
                                whereClauseParameters[k] = v
                        }
                }

                whereClauseParameters
        }


        def addStaticListMethod(clazz) {

                clazz.metaClass.static.list = { parameters ->
                        WhereClauseParameters whereClauseParameters = createParameters(parameters)

                        SelectSqlStatement sqlStatement = dbDialect.createSelectStatement(clazz, whereClauseParameters)

                        if (sqlStatement.countStatement)
                                doPageSelect(whereClauseParameters.getPageSize(), whereClauseParameters.getPage(), sqlStatement, clazz)
                        else
                                doSelect(sqlStatement, clazz)

                }
        }


        private void addClearMethod(clazz) {
                clazz.metaClass.clearDynamicProperties = { ->
                        delegate?.dynamicProperties.clear()
                }
        }

        private void addDeleteMethod(clazz) {
                clazz.metaClass.delete = {
                        WhereClauseParameters whereClauseParameters = new WhereClauseParameters()

                        clazz.primaryKey.each {
                                String columnName = unCamelCaseColumn(it)
                                def val = delegate?."$it"
                                whereClauseParameters[columnName] = dbDialect.getValue(val?.class, val)
                        }

                        SqlStatement sqlStatement = dbDialect.createDeleteStatement(clazz, whereClauseParameters)

                        def db = getSqlInstance(null)
                        db.executeUpdate(sqlStatement.sqlGString)

                }
        }

        private void addStaticDeleteMethod(clazz) {
                clazz.metaClass.static.delete = { Map parameters ->

                        WhereClauseParameters whereClauseParameters = createParameters(parameters)

                        SqlStatement sqlStatement = dbDialect.createDeleteStatement(clazz, whereClauseParameters)


                        def db = getSqlInstance(null)
                        db.executeUpdate(sqlStatement.sqlGString)
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


        def String camelCase(String column) {
                StringBuffer newColumn = new StringBuffer()
                Boolean underScoreFound = false
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
                log.debug('Obtaining SQL Instance from  threadlocal')
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

        Class getPropertyClassType(Class clazz, String propertyName) {
                try {
                        MetaBeanProperty metaProperty = clazz.metaClass.getMetaProperty(propertyName)
                        return metaProperty.getSetter().getNativeParameterTypes()[0]
                } catch (Exception e) {
                        log.error "class ${clazz.name}, propertyName: ${propertyName}"
                        throw e;
                }
        }


        PagedResults doPagedResults(def sql, String totalCountQuery, Integer page, Integer pageSize, def parameterValues) {

//                WhereClauseParameters whereClauseParameters = createParameters(parameters)
//
//                SelectSqlStatement sqlStatement = dbDialect.createSelectStatement(clazz, whereClauseParameters)
//
//                if (sqlStatement.countStatement)
//                        doPageSelect(whereClauseParameters.getPageSize(), whereClauseParameters.getPage(), sqlStatement, clazz)
//                else
//                        doSelect(sqlStatement, clazz)
//
                def newSQL = dbDialect.createPagingStringQuery(sql, page, pageSize)
                log.debug("countQuery parameters: ${parameterValues}")
                PagedResults pagedResults = new PagedResults()

                pagedResults.recordCount = totalCountQuery.executeScalar(parameterValues)
                pagedResults.pageCount = (int) Math.ceil(pagedResults.recordCount/pageSize)
                pagedResults.page = page

                log.debug """doPagingQuery: $newSQL
                                recordCount:  $totalCount
                                totalNumberOfPages: $pageCount
                                page: $page"""

                //[recordCount: totalCount, 'sql': newSQL, pageCount: pageCount, page: page]

                pagedResults.sql = newSQL
                pagedResults
        }

//        def loadModelClasses(path, root, app) {
//                new File(path.toString()).eachFile {
//                        if (!it.isDirectory()) {query
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
                                        } catch (Exception e) {

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

                        if (isEmpty(dataSourceName)) {
                                dataSourceName = "";
                        } else {
                                dataSourceName += ".";
                        }

                        String dataSourceClass = (String) prop.getProperty(dataSourceName + "datasource.class");

                        if (dataSource != null) {
                                Sql db = new Sql(dataSource)
                                return db;
                        }

                        if (dataSource == null && dataSourceClass != null) {

                                Class<?> dsClass = Class.forName(dataSourceClass);

                                dataSource = dsClass.newInstance();
                                Map<String, Object> dataSourceProperties = getDataSourceProperties(dataSourceName)
                                for (String property : dataSourceProperties.keySet()) {
                                        callMethod(dataSource, "set" + capFirstLetter(property), prop.getProperty(dataSourceProperties.get(property)))
                                }

                                Connection connection = dataSource.connection
                                connection.close()

                                return new Sql(dataSource)

                        } else if (dataSource == null && dataSourceClass == null) {

                                String driver = prop.getProperty(dataSourceName + "database.driver")
                                String url = prop.getProperty(dataSourceName + "database.url")
                                String pwd = prop.getProperty(dataSourceName + "database.password");
                                String username = prop.getProperty(dataSourceName + "database.username")

                                return Sql.newInstance(url, username, pwd, driver);
                        }
new MysqlDataSource().setU
                } catch (Exception e) {
                        throw new RuntimeException("newSqlInstance() failed. Make sure app['database.*]' properties are set and correct." + e.getMessage(), e);
                }

                return null;
        }

        boolean isEmpty(String s) {
                if (s)
                        return s.isEmpty()
                else
                        return true
        }

        String getDatabaseDialect(String s) {
                if (s.toLowerCase().contains("firebird")) {
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


        def doPageSelect(Integer pageSize, Integer page, sybrix.easyom.SelectSqlStatement selectSqlStatement, Class modelClass) {
                def db = getSqlInstance(null)
                List results = new ArrayList()

                def pagedResults


                def ct = db.firstRow(selectSqlStatement.countStatement.sqlGString)

                def totalCount = ct.ct
                def pageCount = (int) Math.ceil(totalCount / pageSize)


                pagedResults = [recordCount: totalCount, pageCount: pageCount, page: page]

                try {
                        db.eachRow(selectSqlStatement.sqlGString) { rs ->
                                def row = modelClass.newInstance()
                                selectSqlStatement.selectColumnsAndAliasMap.columnAliasMap.each { col ->
                                        row."$col.key" = dbDialect.getValue(getPropertyClassType(modelClass, "$col.key"), rs."$col.key")
                                }
                                row.clearDynamicProperties()
                                results << row
                        }
                } catch (Exception e) {
                        e.printStackTrace()
                }

                pagedResults.results = results

                pagedResults
        }

        private def doSelect(sybrix.easyom.SelectSqlStatement selectSqlStatement, Class modelClass) {
                def db = getSqlInstance(null)
                List results = new ArrayList()

                db.eachRow(selectSqlStatement.sqlGString) { rs ->
                        def row = modelClass.newInstance()
                        selectSqlStatement.selectColumnsAndAliasMap.columnAliasMap.each { col ->
                                row."$col.key" = dbDialect.getValue(getPropertyClassType(modelClass, "$col.key"), rs."$col.key")
                        }
                        row.clearDynamicProperties()
                        results << row
                }

                return results
        }

//        private def doSelect(sybrix.easyom.SelectSqlStatement selectSqlStatement, Class modelClass) {
//                def db = getSqlInstance(null)
//                List results = new ArrayList()
//                Map columnsInResultSet
//
//                db.eachRow(selectSqlStatement.sqlGString) { rs ->
//                        def row = modelClass.newInstance()
//                        if (columnsInResultSet == null) {
//                                columnsInResultSet = getColumnsFromResultSet(rs.getMetaData())
//                        }
//
//                        columnsInResultSet.each { columnLabel,propertyName ->
//                                row."$propertyName" = getSelectValue(getPropertyClassType(modelClass, propertyName), rs."$columnLabel")
//                        }
//                        row.clearDynamicProperties()
//                        results << row
//                }
//
//                return results
//        }

        Map getColumnsFromResultSet(ResultSetMetaData resultSetMetaData) {
                Map columns = [:]
                for (Integer i = 1; i <= resultSetMetaData.columnCount; i++) {
                        columns[resultSetMetaData.getColumnLabel(i)] = camelCase(resultSetMetaData.getColumnLabel(i).toLowerCase())
                }

                columns
        }

        Blob createBlob(Connection connection, InputStream inputStream) {
                Blob blob = connection.createBlob();
                OutputStream out = blob.setBinaryStream(1)

                byte[] data = new byte[1024]
                int c = 0

                while ((c = inputStream.read(data)) > -1) {
                        out.write(data, 0, c)
                }

                blob
        }
}





