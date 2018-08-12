package sybrix.easyom


class WhereClauseParameters extends PagingAndSortByOptions {
    String operator = "AND"

    WhereClauseParameters() {
        this.put("operator", operator)
    }
}
