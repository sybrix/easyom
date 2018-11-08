package sybrix.easyom

import groovy.transform.CompileStatic


@CompileStatic
class WhereClauseParameters extends PagingAndSortByOptions {
    String operator = "AND"

    WhereClauseParameters() {

    }

}
