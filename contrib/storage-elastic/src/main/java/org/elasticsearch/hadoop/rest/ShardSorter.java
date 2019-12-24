/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.elasticsearch.hadoop.util.Assert;

// Utility introduced for sorting shard overlaps across multiple nodes. Occurs when dealing with aliases that involve searching multiple indices whom shards (primary or replicas)
// might sit on the same node. As the preference API does not allow a shard for a given index to be selected, the shard with the given ID for the entire alias is used instead which
// results in duplicates.
// As a workaround for this, in case of aliases, the search_shard information is retrieved and the possible combinations of nodes are searched for duplicates for all shards. The
// combination with the most nodes is selected.
//
// If no combination is possible, the preferred node option is used instead.

public abstract class ShardSorter {
    private static Log log = LogFactory.getLog(ShardSorter.class);
    public static Map<Shard, Node> find(List<List<Map<String, Object>>> targetShards, Map<String, Node> httpNodes, Log log) {
        // group the shards per node
        Map<Node, Set<Shard>> shardsPerNode = new LinkedHashMap<Node, Set<Shard>>();
        // nodes for each shard
        Map<SimpleShard, Set<Node>> nodesForShard = new LinkedHashMap<SimpleShard, Set<Node>>();

        // for each shard group
        for (List<Map<String, Object>> shardGroup : targetShards) {
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                Node node = httpNodes.get(shard.getNode());
                if (node == null) {
                    log.warn(String.format("Cannot find node with id [%s] (is HTTP enabled?) from shard [%s] in nodes [%s]; layout [%s]", shard.getNode(), shard, httpNodes, targetShards));
                    return Collections.emptyMap();
                }

                // node -> shards
                Set<Shard> shardSet = shardsPerNode.get(node);
                if (shardSet == null) {
                    shardSet = new LinkedHashSet<Shard>();
                    shardsPerNode.put(node, shardSet);
                }
                shardSet.add(shard);

                // shard -> nodes
                SimpleShard ss = SimpleShard.from(shard);
                Set<Node> nodeSet = nodesForShard.get(ss);
                if (nodeSet == null) {
                    nodeSet = new LinkedHashSet<Node>();
                    nodesForShard.put(ss, nodeSet);
                }
                nodeSet.add(node);
            }
        }
        //实时当节点数过多时，进行拆分成多个powerlist对象进行算数组合
        if(httpNodes.values().size() < 21 ){
            return checkCombo(httpNodes.values(), shardsPerNode, targetShards.size() , -1 ,0);
        } else {
            int splitShard = 100;
            for(int i = 0 ; i < splitShard ; i ++ ){
                Map<Shard, Node> finalShards =  checkCombo(httpNodes.values(), shardsPerNode, targetShards.size() ,splitShard , i );
                if(finalShards.size() > 0 ){
                    return finalShards;
                }
            }
            return Collections.emptyMap();
        }

    }

    private static Map<Shard, Node> checkCombo(Collection<Node> nodes, Map<Node, Set<Shard>> shardsPerNode, int numberOfShards ,int split ,int atMode) {

        //拿个nodes所有的两两组合，由于可能同一个shard 具有主备 因子的，所以要排除相同的shard,所以就要把各种组合、
        //拿出来，然后判断拿只不同的shard
        List<Set<Node>> nodesCombinations = powerList(new LinkedHashSet<Node>(nodes), split ,  atMode);

        Set<SimpleShard> shards = new LinkedHashSet<SimpleShard>();
        boolean overlappingShards = false;
        // try each combination and check if there are duplicates
        for (Set<Node> set : nodesCombinations) {
            shards.clear();
            overlappingShards = false;

            //判断各个node是否包含相同的shard
            for (Node node : set) {
                //把这个节点下面的shard都拿出来
                Set<Shard> associatedShards = shardsPerNode.get(node);
                if (associatedShards != null) {
                    for (Shard shard : associatedShards) {
                        if (!shards.add(SimpleShard.from(shard))) {
                            //表示已经包含
                            overlappingShards = true;
                            break;
                        }
                    }
                    if (overlappingShards) {
                        break;
                    }
                }
            }
            // bingo! 如果各个shard是不同的，并且和原来的总数一样
            if (!overlappingShards && shards.size() == numberOfShards) {
                Map<Shard, Node> finalShards = new LinkedHashMap<Shard, Node>();
                for (Node node : set) {
                    Set<Shard> associatedShards = shardsPerNode.get(node);
                    if (associatedShards != null) {
                        // to avoid shard overlapping, only add one request for each shard # (regardless of its index) per node
                        Set<Integer> shardIds = new HashSet<Integer>();
                        for (Shard potentialShard : associatedShards) {
                            if (shardIds.add(potentialShard.getName())) {
                                //存放各个shard对应的节点
                                finalShards.put(potentialShard, node);
                            }
                        }
                    }
                }

                return finalShards;
            }
        }
        return Collections.emptyMap();
    }

    static class SimpleShard {
        private final String index;
        private final Integer id;

        private SimpleShard(String index, Integer id) {
            this.index = index;
            this.id = id;
        }

        static SimpleShard from(Shard shard) {
            return new SimpleShard(shard.getIndex(), shard.getName());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((index == null) ? 0 : index.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SimpleShard other = (SimpleShard) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            }
            else if (!id.equals(other.id))
                return false;
            if (index == null) {
                if (other.index != null)
                    return false;
            }
            else if (!index.equals(other.index))
                return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("SimpleShard [index=").append(index).append(", id=").append(id).append("]");
            return builder.toString();
        }
    }

    static <E> List<Set<E>> nodesCombinations(Set<E> set) {
        // remove empty or 1 element set
        List<Set<E>> list = powerList(set ,-1 ,0);
        for (Iterator<Set<E>> iterator = list.iterator(); iterator.hasNext();) {
            Set<E> s = iterator.next();
            if (s.size() < 2) {
                iterator.remove();
            }
        }
        return list;
    }

    // create the possible combinations using a power set. The results are afterwards sorted based on their set size.
    //一个含有无重复元素的集合，找出它所有的子集。例如{1,2}的所有集合是{}, {1}, {2}, {1, 2}. 这是一个专门的算法问题。学名为powerset
   public static <E> List<Set<E>> powerList(Set<E> set  ,int split ,int atMode) {
        log.info("powerList set.size= " + set.size());
        //在这里put进去就会达到 2 的 set.size 次方个 如  2 的27次方达到 134217728
        List<Set<E>> list = new ArrayList<Set<E>>(new PowerSet<E>(set  ,  split ,  atMode));
       for(int i = list.size() -1  ; i >= 0 ;i --){
           if(list.get(i) == null ){
               list.remove(i);
           }
       }
        //在这进行了排序,对set size进行了排序
        Collections.sort(list, new SetLengthComparator<E>());
        return list;
    }

    private static class SetLengthComparator<T> implements Comparator<Set<T>> {
        @Override
        public int compare(Set<T> o1, Set<T> o2) {
            if(o1 == null ||o2 == null  ){
                System.out.println("11");
            }
            return -Integer.compare(o1.size(), o2.size());
        }
    }

    /**
     * 当把set 放到list里面时，会调用toArray，会调用size，然后一个一个进行对象的复制，这
     * 里的size 是  2 的set.size 次方，对象会很大
     * 如果set.size = 27 ,刚element有 134217728
     *
     *
     * slpit atMode 实时toArray的数据分片
     * @param <E>
     */
    private static class PowerSet<E> extends AbstractSet<Set<E>> {
        private final Map<E, Integer> input;
        private final List<E> elements;
        private final int split;//取这么多mode
        private final int atMode;// 余数
        PowerSet(Set<E> set) {
            this(set ,-1 ,0 );
        }

        PowerSet(Set<E> set ,int split ,int atMode) {
            Assert.isTrue(set.size() <= 30, "Too many elements to create a power set " + set.size());

            input = new LinkedHashMap<E, Integer>(set.size());
            this.split = split;
            this.atMode = atMode;
            int i = set.size();
            for (E e : set) {
                input.put(e, Integer.valueOf(i--));
            }
            elements =  new ArrayList<E>(input.keySet());
        }

        @Override
        public Iterator<Set<E>> iterator() {
            return new ReverseIndexedListIterator<Set<E>>(size()) {
                /**
                 * 当前的size的对应位置的index
                 * @param setBits
                 * @return
                 */
                @Override
                protected Set<E> get(final int setBits) {
                    return new SubSet<E>(input, setBits ,elements);
                }
            };
        }

        //这里往左移动 相当于1 往左移动了 input.size()  相当于  2 的 (input.size() -1) 次方
        @Override
        public int size() {
            return 1 << input.size();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            if (o instanceof Set) {
                Set<?> set = (Set<?>) o;
                return input.keySet().containsAll(set);
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof PowerSet) {
                PowerSet<?> that = (PowerSet<?>) o;
                return input.equals(that.input);
            }
            return super.equals(o);
        }
        @Override
        public int hashCode() {
            return input.keySet().hashCode() << (input.size() - 1);
        }

        /**
         * 下面进行复制对象的优化，进行数据的分片复制
         * @return
         */
        @Override
        public Object[] toArray() {
            // Estimate size of array; be prepared to see more or fewer elements
            int startEle = 0 ;
            int endEle  = 0;
            int eleLeng = 0;

            Object[] r ;
            if(split <= 0 ) {
                eleLeng = size();
                r = new Object[eleLeng];
                //拿所有的数据
                Iterator it = iterator();
                int eleIndex = 0;
                for (int i = 0; i <  eleLeng; i++) {
                    if (!it.hasNext()) // fewer elements than expected
                        return Arrays.copyOf(r, eleIndex);

                    r[eleIndex] = it.next();
                    eleIndex++;
                }
            } else {
                int perSplitEle = size() / split;
                startEle = perSplitEle * atMode;
                endEle =  perSplitEle * (atMode + 1)  ;
                if(atMode == split - 1 ){
                    endEle = size();
                }
                eleLeng = endEle - startEle;
                r = new Object[eleLeng];

                //进行截取区间数据
                ReverseIndexedListIterator it = (ReverseIndexedListIterator)iterator();
                int eleIndex = 0;
                for (int i = startEle; i <  endEle; i++) {
                    r[eleIndex] = it.get(i);
                    eleIndex++;
                }
            }

            return  r ;
        }


//        private  <T> T[] finishToArray(T[] r, Iterator  it) {
//            if(true){
//                throw new RuntimeException("the ShardSorter toArray calculate is error");
//            }
//            int eleIndex = r.length;
//            int k = 0;
//            while (it.hasNext()) {
//                k ++;
//                int cap = r.length;
//                if (eleIndex == cap) {
//                    int newCap = cap + (cap >> 1) + 1;
//                    // overflow-conscious code
//                    if (newCap - MAX_ARRAY_SIZE > 0)
//                        newCap = hugeCapacity(cap + 1);
//                    r = Arrays.copyOf(r, newCap);
//                }
//
//                if(split > 0 ){
//                    if(k % split == atMode ){
//                        //刚好取的到那一个余
//                        r[eleIndex] =  (T)it.next();
//                        eleIndex ++;
//                    } else {
//                        //放弃
//                        it.next();
//                    }
//                } else {
//                    r[eleIndex] = (T)it.next();
//                    eleIndex ++;
//                }
//            }
//            // trim if overallocated
//            return (eleIndex == r.length) ? r : Arrays.copyOf(r, eleIndex);
//        }
//        private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
//        private static int hugeCapacity(int minCapacity) {
//            if (minCapacity < 0) // overflow
//                throw new OutOfMemoryError
//                        ("Required array size too large");
//            return (minCapacity > MAX_ARRAY_SIZE) ?
//                    Integer.MAX_VALUE :
//                    MAX_ARRAY_SIZE;
//        }


    }

    private static final class SubSet<E> extends AbstractSet<E> {
        private final Map<E, Integer> inputSet;
        private final int mask;
        private final List<E> elements;
        /**
         *
         * @param inputSet 当前size 个数据
         * @param mask 当前的 index  2 的size 次方 .如果转成二进制，就是 Size位的二进制，有0 有1 位,下面拿数据用到
         */
        SubSet(Map<E, Integer> inputSet, int mask ,final List<E> elements) {
            this.inputSet = inputSet;
            this.mask = mask;
            this.elements = elements;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {
                //为了减少内存的消耗，要把这个每个subset去掉
               // final List<E> elements = new ArrayList<E>(inputSet.keySet());
                int remainingSetBits = mask;

                @Override
                public boolean hasNext() {
                    return remainingSetBits != 0;
                }

                @Override
                public E next() {
                    int index = Integer.numberOfTrailingZeros(remainingSetBits);
                    if (index == 32) {
                        throw new NoSuchElementException();
                    }
                    remainingSetBits &= ~(1 << index);
                    return elements.get(index);
                }

                @Override
                public final void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return Integer.bitCount(mask);
        }

        @Override
        public boolean contains(Object o) {
            Integer index = inputSet.get(o);
            return index != null && (mask & (1 << index)) != 0;
        }
    }

    /**
     * 相当于读取一个固定大小的数组
     * @param <E>
     */
    private static abstract class ReverseIndexedListIterator<E> implements Iterator<E> {
        private final int size;
        private int position;

        protected ReverseIndexedListIterator(int size) {
            this.size = size;
            this.position = size - 1;
        }

        @Override
        public final boolean hasNext() {
            return position > 0;
        }

        @Override
        public final E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return get(position--);
        }

        public final int nextIndex() {
            return position;
        }

        public final boolean hasPrevious() {
            return position < size;
        }

        public final E previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }
            return get(++position);
        }

        public final int previousIndex() {
            return position + 1;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

        protected abstract E get(int index);
    }
}