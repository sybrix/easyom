package sybrix.easyom.dialects

import groovy.transform.CompileStatic
import sybrix.easyom.SelectColumnsAndAliasMap
import sybrix.easyom.SelectSqlStatement
import sybrix.easyom.SqlStatement
import sybrix.easyom.WhereClauseParameters

import java.sql.Blob
import java.sql.Connection

@CompileStatic
interface Dialect {
        void init(Properties properties)
        SelectSqlStatement createSelectStatement(Class modelClass, WhereClauseParameters whereClauseParameters);
        SqlStatement createUpdateStatement(Object modelInstance)
        SqlStatement createInsertStatement(Object instance, Boolean insertUpdatedColumnsOnly)
        SqlStatement createDeleteStatement(Class clazz, WhereClauseParameters whereMap)
        SelectColumnsAndAliasMap createSelectColumnsAndAliasMap(Class clazz)
        GString createPagingStringQuery(GString sql, Integer page, Integer pageSize)
        String createRecordCountStringQuery(String sql)
        String createPagingAfterSelect(Integer page, Integer pageSize)
        String createPagingAfterOrderBy(Integer page, Integer pageSize, String orderBy)
        Object getValue(Object obj, Object val)
        Blob createBlob(Connection connection, InputStream inputStream)
}