package com.gugu.mr.page.topn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author gugu
 * @Classname PageCount
 * @Description TODO
 * @Date 2019/11/27 16:06
 */
public class PageCount implements Comparable<PageCount> {

    private String page;
    private int count;

    public void set(String page, int count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        if(this.count - o.count == 0){
            return this.page.compareTo(o.getPage());
        }
        return o.count - this.count ;
    }
}
