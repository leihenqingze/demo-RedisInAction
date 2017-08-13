package org.demo.commons;

import lombok.Data;

@Data
public class Page {

    private long page;
    private long limit;

    public Page(long page, long limit){
        setPage(page);
        setLimit(limit);
    }

    public long getOffset(){
        return (this.page - 1) * this.limit;
    }

    public void setPage(long page){
        if (page < 1){
            this.page = 1;
        } else {
            this.page = page;
        }
    }

    public void setLimit(long limit){
        if (limit < 5 || limit > 30){
            this.limit = 10;
        } else {
            this.limit = limit;
        }
    }

}