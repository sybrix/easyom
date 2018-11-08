package sybrix.easyom.dialects

import sybrix.easyom.dialects.AbstractDialect

import java.sql.Blob
import java.sql.Connection

class FirebirdDialect extends AbstractDialect{


        GString createPagingStringQuery(GString sql, Integer page, Integer pageSize) {
                return null
        }

        @Override
        public String createPagingAfterSelect(Integer page, Integer pageSize) {
                StringBuilder sql = new StringBuilder()

                if (page != null && pageSize != null) {
                        sql.append("FIRST ").append(pageSize).append(" SKIP ").append(getSkip(pageSize, page))
                }

                sql.toString()
        }


        public String createPagingAfterOrderBy(Integer page, Integer pageSize, String orderBy) {
                StringBuilder sql = new StringBuilder()

                if (page != null && pageSize != null) {
                        if (orderBy == null)
                                throw new RuntimeException('orderBy: [columnName] required')

                        def skip = getSkip(pageSize, page)
                        sql.append(" LIMIT $pageSize OFFSET ${skip}")

                }

                sql.toString()
        }

        @Override
        String createRecordCountStringQuery(String sql) {
                return null
        }

        @Override
        Blob createBlob(Connection connection, InputStream inputStream) {
                return null
        }
}
