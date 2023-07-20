# MIT6.830

## 踩坑日记

LRU的忘记remove Hashmap的值
修改后

```java
    @Override
public PageId getEvictPageId(){
        DLinkedNode node=removeTail();
        map.remove(node.getValue());
        return node.getValue();
        }
```