package simpledb.storage;

import simpledb.common.DbException;

/**
 * @className: Evictstrategy
 * @author: 朱江
 * @description: 页面置换策略接口
 * @date: 2023/7/19
 **/
public interface EvictStrategy {

    // 返回要删除的页面的ID
    PageId getEvictPageId() throws DbException;

    void addPageId(PageId pageId,Page page);

    void updateByPageId(PageId pageId,Page page);
}
