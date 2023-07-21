package simpledb.storage;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @className: FIFOStracy
 * @author: 朱江
 * @description:
 * @date: 2023/7/20
 **/
public class FIFOEvict implements EvictStrategy {
    /**
     * 存储数据的队列
     */
    private final Queue<PageId> queue;

    public FIFOEvict(int numPages) {
        this.queue = new ArrayDeque<>(numPages);
    }

    @Override
    public void addPageId(PageId pageId, Page page) {
        // 向尾部插入元素
        boolean offer = queue.offer(pageId);
        if (offer) {
            System.out.println("PageId: " + pageId + " 插入队列成功");
            System.out.println(queue.size());
        } else {
            System.out.println("PageId: " + pageId + " 插入队列失败");
        }
    }

    @Override
    public void updateByPageId(PageId pageId, Page page) {

    }

    @Override
    public PageId getEvictPageId() {
        // 从队列头部获取元素
        return queue.poll();
    }

}