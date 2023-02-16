package suishen.libs.data;

import suishen.libs.meta.SuishenConstant;
import org.apache.commons.lang3.math.NumberUtils;


/**
 * Author: Bryant Hang
 * Date: 16/6/15
 * Time: 17:13
 */
public class PageRequest implements SuishenConstant {
    /**
     * 页码
     */
    private int pageNo = 1;

    /**
     * 一页大小
     */
    private int pageSize = NumberUtils.toInt(DEFAULT_PAGE_SIZE);

    private String orderBy = null;

    private String order = PageOrder.DESC.val();

    public PageRequest() {
    }

    public PageRequest(int pageNumber, int pageSize) {
        this.pageNo = pageNumber;
        this.pageSize = pageSize;
    }

    public PageRequest next() {
        return new PageRequest(pageNo++, pageSize);
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getPageNumber() {
        return pageNo;
    }

    public void setPageNumber(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getOffset() {
        return (this.pageNo - 1) * this.pageSize;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }
}
