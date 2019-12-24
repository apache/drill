package org.elasticsearch.hadoop.rest;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.FieldPresenceValidation;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.Version;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class RestService implements Serializable {

    public static class PartitionDefinition implements Serializable {
        public final String serializedSettings, serializedMapping;
        public final String nodeIp, nodeId, nodeName, shardId;
        public final int nodePort;
        public final boolean onlyNode;

        PartitionDefinition(Shard shard, Node node, String settings, String mapping, boolean onlyNode) {
            this(node.getIpAddress(), node.getHttpPort(), node.getName(), node.getId(), shard.getName().toString(),
                    onlyNode, settings, mapping);
        }

        public PartitionDefinition(String nodeIp, int nodePort, String nodeName, String nodeId, String shardId,
                boolean onlyNode, String settings, String mapping) {
            this.nodeIp = nodeIp;
            this.nodePort = nodePort;
            this.nodeName = nodeName;
            this.nodeId = nodeId;
            this.shardId = shardId;

            this.serializedSettings = settings;
            this.serializedMapping = mapping;

            this.onlyNode = onlyNode;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("EsPartition [node=[").append(nodeId).append("/").append(nodeName)
            .append("|").append(nodeIp).append(":").append(nodePort)
            .append("],shard=").append(shardId).append("]");
            return builder.toString();
        }

        public Settings settings() {
            return new PropertiesSettings(new Properties()).load(serializedSettings);
        }
    }

    public static class PartitionReader implements Closeable {
        public final ScrollReader scrollReader;
        public final RestRepository client;
        public final QueryBuilder queryBuilder;

        private ScrollQuery scrollQuery;

        private boolean closed = false;

        PartitionReader(ScrollReader scrollReader, RestRepository client, QueryBuilder queryBuilder) {
            this.scrollReader = scrollReader;
            this.client = client;
            this.queryBuilder = queryBuilder;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                if (scrollQuery != null) {
                    scrollQuery.close();
                }
                client.close();
            }
        }

        public ScrollQuery scrollQuery() {
            if (scrollQuery == null) {
                //在这里生成query了
                scrollQuery = queryBuilder.build(client, scrollReader);
            }

            return scrollQuery;
        }
    }

    public static class PartitionWriter implements Closeable {
        public final RestRepository repository;
        public final int number;
        public final int total;
        public final Settings settings;

        private boolean closed = false;

        PartitionWriter(Settings settings, int splitIndex, int splitsSize, RestRepository repository) {
            this.settings = settings;
            this.repository = repository;
            this.number = splitIndex;
            this.total = splitsSize;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                repository.close();
            }
        }
    }

    public static class MultiReaderIterator implements Closeable, Iterator {
        private final List<PartitionDefinition> definitions;
        private final Iterator<PartitionDefinition> definitionIterator;
        private PartitionReader currentReader;
        private ScrollQuery currentScroll;
        private boolean finished = false;

        private final Settings settings;
        private final Log log;

        MultiReaderIterator(List<PartitionDefinition> defs, Settings settings, Log log) {
            this.definitions = defs;
            definitionIterator = defs.iterator();

            this.settings = settings;
            this.log = log;
        }

        @Override
        public void close() {
            if (finished) {
                return;
            }

            ScrollQuery sq = getCurrent();
            if (sq != null) {
                sq.close();
            }
            if (currentReader != null) {
                currentReader.close();
            }

            finished = true;
        }

        @Override
        public boolean hasNext() {
            ScrollQuery sq = getCurrent();
            return (sq != null ? sq.hasNext() : false);
        }

        private ScrollQuery getCurrent() {
            if (finished) {
                return null;
            }


            for (boolean hasValue = false; !hasValue;) {
                if (currentReader == null) {
                    if (definitionIterator.hasNext()) {
                        currentReader = RestService.createReader(settings, definitionIterator.next(), log);
                    }
                    else {
                        finished = true;
                        return null;
                    }
                }

                if (currentScroll == null) {
                    currentScroll = currentReader.scrollQuery();
                }

                hasValue = currentScroll.hasNext();

                if (!hasValue) {
                    currentScroll.close();
                    currentScroll = null;

                    currentReader.close();
                    currentReader = null;
                }
            }

            return currentScroll;
        }

        @Override
        public JsonNode next() {
            ScrollQuery sq = getCurrent();
            return sq.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("unchecked")
    public static List<PartitionDefinition> findPartitions(Settings settings, Log log) {
        Version.logVersion();

        boolean overlappingShards = false;
        Map<Shard, Node> targetShards = null;

        InitializationUtils.validateSettings(settings);
        InitializationUtils.discoverEsVersion(settings, log);
        InitializationUtils.discoverNodesIfNeeded(settings, log);
        InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
        InitializationUtils.filterNonDataNodesIfNeeded(settings, log);

        String savedSettings = settings.save();

        RestRepository client = new RestRepository(settings);
        boolean indexExists = client.indexExists(true);

        if (!indexExists) {
            if (settings.getIndexReadMissingAsEmpty()) {
                log.info(String.format("Index [%s] missing - treating it as empty", settings.getResourceRead()));
                targetShards = Collections.emptyMap();
            }
            else {
                client.close();
                throw new EsHadoopIllegalArgumentException(
                        String.format("Index [%s] missing and settings [%s] is set to false", settings.getResourceRead(), ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY));
            }
        }
        else {
            Object[] result = client.getReadTargetShards(settings.getNodesClientOnly());
            overlappingShards = (Boolean) result[0];
            targetShards = (Map<Shard, Node>) result[1];

            if (log.isTraceEnabled()) {
                log.trace("Creating splits for shards " + targetShards);
            }
        }

        log.info(String.format("Reading from [%s]", settings.getResourceRead()));

        String savedMapping = null;
        if (!targetShards.isEmpty()) {
            Field mapping = client.getMapping();
            log.info(String.format("Discovered mapping {%s} for [%s]", mapping, settings.getResourceRead()));
            // validate if possible
            FieldPresenceValidation validation = settings.getReadFieldExistanceValidation();
            if (validation.isRequired()) {
                MappingUtils.validateMapping(settings.getScrollFields(), mapping, validation, log);
            }

            //TODO: implement this more efficiently
            savedMapping = IOUtils.serializeToBase64(mapping);
        }

        client.close();

        List<PartitionDefinition> partitions = new ArrayList<PartitionDefinition>(targetShards.size());

        for (Entry<Shard, Node> entry : targetShards.entrySet()) {
            partitions.add(new PartitionDefinition(entry.getKey(), entry.getValue(), savedSettings, savedMapping, !overlappingShards));
        }

        return partitions;
    }

    public static PartitionReader createReader(Settings settings, PartitionDefinition partition, Log log) {

        if (!SettingsUtils.hasPinnedNode(settings)) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Partition reader instance [%s] assigned to [%s]:[%s]", partition,
                        partition.nodeId, partition.nodePort));
            }

            SettingsUtils.pinNode(settings, partition.nodeIp, partition.nodePort);
        }

        ValueReader reader = ObjectUtils.instantiate(settings.getSerializerValueReaderClassName(), settings);

        Field fieldMapping = null;

        if (StringUtils.hasText(partition.serializedMapping)) {
            fieldMapping = IOUtils.deserializeFromBase64(partition.serializedMapping);
        }
        else {
            log.warn(String.format("No mapping found for [%s] - either no index exists or the partition configuration has been corrupted", partition));
        }

        ScrollReader scrollReader = new ScrollReader(new ScrollReaderConfig(reader, fieldMapping, settings));

        // initialize REST client
        RestRepository client = new RestRepository(settings);

        if (settings.getNodesClientOnly()) {
            String clientNode = client.getRestClient().getCurrentNode();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Client-node routing detected; partition reader instance [%s] assigned to [%s]",
                        partition, clientNode));
            }
            SettingsUtils.pinNode(settings, clientNode);
        }

        // take into account client node routing
        QueryBuilder queryBuilder = QueryBuilder.query(settings).shard(partition.shardId)
                .node(partition.nodeId).restrictToNode(partition.onlyNode && (!settings.getNodesClientOnly() && !settings.getNodesWANOnly()));
        queryBuilder.fields(settings.getScrollFields());
        queryBuilder.filter(SettingsUtils.getFilters(settings));

        return new PartitionReader(scrollReader, client, queryBuilder);
    }


    // expects currentTask to start from 0
    public static List<PartitionDefinition> assignPartitions(List<PartitionDefinition> partitions, int currentTask, int totalTasks) {
        int esPartitions = partitions.size();
        if (totalTasks >= esPartitions) {
            return (currentTask >= esPartitions ? Collections.<PartitionDefinition> emptyList() : Collections.singletonList(partitions.get(currentTask)));
        }
        else {
            int partitionsPerTask = esPartitions / totalTasks;
            int remainder = esPartitions % totalTasks;

            int partitionsPerCurrentTask = partitionsPerTask;

            // spread the reminder against the tasks
            if (currentTask < remainder) {
                partitionsPerCurrentTask++;
            }

            // find the offset inside the collection
            int offset = partitionsPerTask * currentTask;
            if (currentTask != 0) {
                offset += (remainder > currentTask ? 1 : remainder);
            }

            // common case
            if (partitionsPerCurrentTask == 1) {
                return Collections.singletonList(partitions.get(offset));
            }

            List<PartitionDefinition> pa = new ArrayList<PartitionDefinition>(partitionsPerCurrentTask);
            for (int index = offset; index < offset + partitionsPerCurrentTask; index++) {
                pa.add(partitions.get(index));
            }
            return pa;
        }
    }

    public static MultiReaderIterator multiReader(Settings settings, List<PartitionDefinition> definitions, Log log) {
        return new MultiReaderIterator(definitions, settings, log);
    }

    public static PartitionWriter createWriter(Settings settings, int currentSplit, int totalSplits, Log log) {
        Version.logVersion();

        InitializationUtils.validateSettings(settings);
        InitializationUtils.discoverEsVersion(settings, log);
        InitializationUtils.discoverNodesIfNeeded(settings, log);
        InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
        InitializationUtils.filterNonDataNodesIfNeeded(settings, log);

        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);

        // check invalid splits (applicable when running in non-MR environments) - in this case fall back to Random..
        int selectedNode = (currentSplit < 0) ? new Random().nextInt(nodes.size()) : currentSplit % nodes.size();

        // select the appropriate nodes first, to spread the load before-hand
        SettingsUtils.pinNode(settings, nodes.get(selectedNode));

        Resource resource = new Resource(settings, false);

        log.info(String.format("Writing to [%s]", resource));

        // single index vs multi indices
        IndexExtractor iformat = ObjectUtils.instantiate(settings.getMappingIndexExtractorClassName(), settings);
        iformat.compile(resource.toString());
        //当包含正则时
        RestRepository repository = (iformat.hasPattern() ? initMultiIndices(settings, currentSplit, resource, log) : initSingleIndex(settings, currentSplit, resource, log));

        return new PartitionWriter(settings, currentSplit, totalSplits, repository);
    }

    private static RestRepository initSingleIndex(Settings settings, int currentInstance, Resource resource, Log log) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Resource [%s] resolves as a single index", resource));
        }

        RestRepository repository = new RestRepository(settings);
        // create the index if needed
        if (repository.touch()) {
            if (repository.waitForYellow()) {
                log.warn(String.format("Timed out waiting for index [%s] to reach yellow health", resource));
            }
        }

        if (settings.getNodesWANOnly()) {
            //如果是全部节点
            return randomNodeWrite(settings, currentInstance, resource, log);
        }

        // if client-nodes are used, simply use the underlying nodes
        if (settings.getNodesClientOnly()) {
            String clientNode = repository.getRestClient().getCurrentNode();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Client-node routing detected; partition writer instance [%s] assigned to [%s]",
                        currentInstance, clientNode));
            }

            return repository;
        }

        // no routing necessary; select the relevant target shard/node
        Map<Shard, Node> targetShards = Collections.emptyMap();

        targetShards = repository.getWriteTargetPrimaryShards(settings.getNodesClientOnly());
        for (int retries = 0; retries < 5 && targetShards.isEmpty(); retries++) {
            //可能是系统比较忙，所以请求shard总是请求不到，这里进行等待一下
            try {
                Thread.currentThread().sleep(20 * 1000);
            } catch (InterruptedException e) {
            }
            targetShards = repository.getWriteTargetPrimaryShards(settings.getNodesClientOnly());
        }
        repository.close();
        Assert.isTrue(!targetShards.isEmpty(),
                String.format("Cannot determine write shards for [%s]; likely its format is incorrect (maybe it contains illegal characters?)", resource));


        List<Shard> orderedShards = new ArrayList<Shard>(targetShards.keySet());
        // make sure the order is strict
        Collections.sort(orderedShards);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Partition writer instance [%s] discovered [%s] primary shards %s", currentInstance, orderedShards.size(), orderedShards));
        }

        // if there's no task info, just pick a random bucket
        if (currentInstance <= 0) {
            currentInstance = new Random().nextInt(targetShards.size()) + 1;
        }
        int bucket = currentInstance % targetShards.size();
        Shard chosenShard = orderedShards.get(bucket);
        Node targetNode = targetShards.get(chosenShard);

        // pin settings
        SettingsUtils.pinNode(settings, targetNode.getIpAddress(), targetNode.getHttpPort());
        String node = SettingsUtils.getPinnedNode(settings);
        repository = new RestRepository(settings);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Partition writer instance [%s] assigned to primary shard [%s] at address [%s]",
                    currentInstance, chosenShard.getName(), node));
        }

        return repository;
    }

    private static RestRepository initMultiIndices(Settings settings, int currentInstance, Resource resource, Log log) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Resource [%s] resolves as an index pattern", resource));
        }

        return randomNodeWrite(settings, currentInstance, resource, log);
    }

    /**
     * 随机找一个节点进行写入
     * @param settings
     * @param currentInstance
     * @param resource
     * @param log
     * @return
     */
    private static RestRepository randomNodeWrite(Settings settings, int currentInstance, Resource resource, Log log) {
        // multi-index write - since we don't know before hand what index will be used, pick a random node from the given list
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        String node = nodes.get(new Random().nextInt(nodes.size()));
        // override the global settings to communicate directly with the target node
        SettingsUtils.pinNode(settings, node);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Partition writer instance [%s] assigned to [%s]", currentInstance, node));
        }

        return new RestRepository(settings);
    }
}