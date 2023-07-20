package simpledb.storage;

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
    public void addPageId(PageId pageId) {
        if (map.containsKey(pageId)) {
            DLinkedNode node = map.get(pageId);
            moveToHead(node);
        } else {
            DLinkedNode node = new DLinkedNode(pageId);
            map.put(pageId, node);
            addToHead(node);
        }
        System.out.println("LRU SIZE = " + map.size());
    }

    @Override
    public PageId getEvictPageId() {
        DLinkedNode node = removeTail();
        map.remove(node.getValue());
        return node.getValue();
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

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }

    private static class DLinkedNode {
        PageId value;
        DLinkedNode prev;
        DLinkedNode next;

        public DLinkedNode() {
        }

        public DLinkedNode(PageId value) {
            this.value = value;
        }

        public PageId getValue() {
            return value;
        }
    }
}