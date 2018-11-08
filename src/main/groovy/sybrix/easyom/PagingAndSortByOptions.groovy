package sybrix.easyom

import groovy.transform.CompileStatic

@CompileStatic
class PagingAndSortByOptions extends HashMap<String, Object> {
        String orderBy
        Integer page
        Integer pageSize
        String countColumn = "*"

        PagingAndSortByOptions() {

        }

        PagingAndSortByOptions(String orderBy) {
                this.orderBy = orderBy
        }

        PagingAndSortByOptions(String orderBy, Integer page, Integer pageSize) {
                this.orderBy = orderBy
                this.page = page
                this.pageSize = pageSize
        }

        PagingAndSortByOptions(String orderBy, int page, int pageSize, String countColumn) {
                this(orderBy, page, pageSize)
                this.countColumn = countColumn
        }

        def addParameter(String parameterName, Object value) {
                this.put(parameterName, value)
        }

        void addPage(int page) {
                this.put("page", page)
        }

        void addPaging(String orderBy, int page, int pageSize, String countColumn) {
                this.orderBy = orderBy
                this.page = page
                this.pageSize = pageSize
                this.countColumn = countColumn
        }

        String getOrderBy() {
                return orderBy
        }

        void setOrderBy(String orderBy) {
                this.orderBy = orderBy
        }
}