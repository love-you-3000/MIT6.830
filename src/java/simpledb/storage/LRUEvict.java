package simpledb.storage;

import simpledb.common.DbException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @className: LRUCache
 * @author: 朱江
 * @description: LRU置换页面
 * @date: 2023/7/19
 **/

public class LRUEvict implements EvictStrategy {
    private final DLinkedNode head;
    private final DLinkedNode tail;
    private final Map<PageId, DLinkedNode> map;

    // LRU缓存容量

    public LRUEvict(int numPages) {
        map = new ConcurrentHashMap<>(numPages);
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
    }

    @Override
    public void addPageId(PageId pageId, Page page) {
        if (map.containsKey(pageId)) {
            DLinkedNode node = map.get(pageId);
            moveToHead(node);
        } else {
            DLinkedNode node = new DLinkedNode(pageId, page);
            map.put(pageId, node);
            addToHead(node);
        }
        System.out.println("LRU SIZE = " + map.size());
    }

    @Override
    public PageId getEvictPageId() throws DbException {
        DLinkedNode node = removeTail();
        map.remove(node.getKey());
        return node.getKey();
    }

    public void updateByPageId(PageId pageId, Page page){
        DLinkedNode node = new DLinkedNode(pageId, page);
        map.put(pageId, node);
    }
    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() throws DbException {
        DLinkedNode res = tail.prev;
        while (res.getValue().isDirty() != null && res != head)
            res = res.prev;
        if (res == head || res == tail)
            throw new DbException("没有合适的页存储空间或者所有页都为脏页！！");
        removeNode(res);
        return res;
    }

    private static class DLinkedNode {
        PageId key;

        Page value;
        DLinkedNode prev;
        DLinkedNode next;

        public DLinkedNode() {
        }

        public DLinkedNode(PageId key, Page value) {
            this.key = key;
            this.value = value;
        }

        public PageId getKey() {
            return key;
        }

        public Page getValue() {
            return value;
        }
    }
}