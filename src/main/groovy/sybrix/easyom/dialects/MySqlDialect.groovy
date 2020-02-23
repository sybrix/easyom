package sybrix.easyom.dialects

import org.codehaus.groovy.runtime.GStringImpl
import sybrix.easyom.SqlStatement

import java.sql.Blob
import java.sql.Connection

class MySqlDialect extends AbstractDialect {
        @Override
        String createPagingAfterSelect(Integer page, Integer pageSize) {
                return ""
        }

        @Override
        public String createPagingAfterOrderBy(Integer page, Integer pageSize, String orderBy) {
                StringBuilder sql = new StringBuilder()

                if (page != null && pageSize != null) {
                        if (orderBy == null)
                                throw new RuntimeException('orderBy: [columnName] required')

                        def skip = getSkip(pageSize, page)
                        sql.append(" OFFSET ${skip} rows fetch first $pageSize rows only")
                }

                sql.toString()
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


                boolean manualPrimaryKeys = false
                Map sequence = [:]
                if (isProperty(instance, "sequence")) {
                        sequence = instance.sequence
                }

                instance.primaryKey.each {
                        if (instance.dynamicProperties.contains(it)) {
                                manualPrimaryKeys = true
                        } else {
                                manualPrimaryKeys = false
                        }
                }

                if (insertUpdatedColumnsOnly == true || (insertUpdatedColumnsOnly == null && instance?.dynamicProperties && manualPrimaryKeys)) {
                        properties = instance.dynamicProperties
                        instance?.primaryKey?.each {
                                if (!properties.contains(it)) {
                                        properties.add(it)
                                }
                        }
                }

                if (manualPrimaryKeys == false) {
                        List l = instance.primaryKey.findAll {
                                sequence.containsKey(it) == false
                        }
                        properties.removeAll(l)
                }

                if (isProperty(clazz, 'exclude')) {
                        properties.removeAll(instance.exclude)
                }

                properties.each {
                        if (instance.dynamicProperties.contains(it) || (manualPrimaryKeys == false && instance.primaryKey.contains(it))) {
                                if (hasColumnsProperty && instance.columns?.containsKey(it)) {
                                        s << instance.columns[it]
                                } else {
                                        s << unCamelCase(it)
                                }

                                s << ', '
                        }
                }
                s.replace(s.size() - 2, s.size(), '')

                s << ') VALUES ('

                properties.each {
                        if (instance.dynamicProperties.contains(it) || (manualPrimaryKeys == false && instance.primaryKey.contains(it))) {
                                if (instance.primaryKey.contains(it) && sequence.containsKey(it)) {
                                        s << "NEXT VALUE FOR ${sequence[it]},"
                                } else {
                                        s << '?,'
                                        def val = instance."$it"
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
                }
                s.replace(s.size() - 1, s.size(), '')
                s << ')'

                GString gString = new GStringImpl(values?.toArray() ?: [].toArray(), s.toString().trim().split('\\?'))
                new SqlStatement(sqlGString: gString)
        }


        public Object getValue1(Class clazz, Object val) {
                if (val == null)
                        return null

                if (clazz == java.sql.Date.class || clazz == java.util.Date.class) {
                        return new java.sql.Timestamp(val.time)
                } else if (clazz.name == '[B') {
                        return null

                } else {
                        return val
                }
        }

        public Object getValue(Class obj, val) {
                if (val == null)
                        return

                if (obj == java.util.Date.class && val.class == java.sql.Timestamp) {
                        return new java.util.Date(val.time)
                } else if (obj == java.sql.Date.class) {
                        return new java.util.Date(val.time)

                } else if (obj == java.lang.Boolean.class || obj == boolean.class) {
                        //console  "EasyOM.getValue() " + val + " " + obj
                        return (val == '1' || val == 1 || val == 'true' || val == 't' || val == 'Y' || val == true) ? true : false
                } else if (obj == java.lang.String.class && val instanceof java.sql.Blob) {
                        java.sql.Blob blob = (java.sql.Blob) val
                        byte[] data = new byte[1024]
                        ByteArrayOutputStream out = new ByteArrayOutputStream()

                        int c = 0;
                        while (true) {
                                c = blob.binaryStream.read(data)
                                out.write(data, 0, c)
                                if (out.size() >= blob.length()) {
                                        break
                                }
                        }

                        return out.toString()
                } else if (obj == java.lang.String.class && val instanceof java.sql.Clob) {
                        java.sql.Clob clob = (java.sql.Clob) val
                        char[] data = new char[1024]
                        StringBuffer sb = new StringBuffer()

                        int c = 0;
                        int count = 0;

                        while (true) {
                                count = clob.getCharacterStream().read(data, 0, data.length)
                                if (count == -1)
                                        break;

                                sb.append(data, 0, count)

                                if (sb.length() >= sb.length()) {
                                        break
                                }
                        }

                        return sb.toString()

                } else {
                        return val
                }
        }

        GString createPagingStringQuery(GString sql, Integer page, Integer pageSize) {

                //offset ? rows fetch first ? rows only

                def skip = getSkip(pageSize, page)
                return sql.plus(" LIMIT $pageSize OFFSET ${skip} ")

        }

        String createRecordCountStringQuery(String sql) {
                def newSQL = sql.toString().replaceAll("\n", " ").replaceAll("\t", " ")

                int index = newSQL.indexOf(" FROM ")
                int orderByIndex = newSQL.toLowerCase().indexOf(" order by ")

                if (index == -1) {
                        throw new RuntimeException("What?, a paging query must have a \"FROM\" section. The \"FROM\" in the main FROM section must be capitalized.  Don't use all capitals with in other from clauses.")
                }

                return "SELECT COUNT(*) " + newSQL.substring(index, orderByIndex)
        }

        @Override
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
