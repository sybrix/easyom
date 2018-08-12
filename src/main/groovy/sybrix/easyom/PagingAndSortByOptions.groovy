package sybrix.easyom

class PagingAndSortByOptions extends HashMap<String, Object> {
    PagingAndSortByOptions() {

    }
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