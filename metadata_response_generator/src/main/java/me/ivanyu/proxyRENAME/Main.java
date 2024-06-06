package me.ivanyu.proxyRENAME;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final String BROKER_0_ORIGINAL_HOST = "host0.remote.com";
    private static final int BROKER_0_ORIGINAL_PORT = 9092;
    private static final String BROKER_1_ORIGINAL_HOST = "host1.remote.com";
    private static final int BROKER_1_ORIGINAL_PORT = 9092;
    private static final String BROKER_2_ORIGINAL_HOST = "host2.remote.com";
    private static final int BROKER_2_ORIGINAL_PORT = 9092;

    private static final String LOCALHOST = "localhost";
    private static final int BROKER_0_MODIFIED_PORT = 10000;
    private static final int BROKER_1_MODIFIED_PORT = 10001;
    private static final int BROKER_2_MODIFIED_PORT = 10002;

    public static void main(final String[] args) throws IOException {
        for (short v = 0; v <= ApiKeys.METADATA.latestVersion(); v++) {
            final var context = createRequestContext(ApiKeys.METADATA, v);
            final var metadataResponseOriginal = generateMetadataResponse(v, true);
            writeToFile(context, metadataResponseOriginal, String.format("output/MetadataResponse.V%d.original", v));
            final var metadataResponseModified = generateMetadataResponse(v, false);
            writeToFile(context, metadataResponseModified, String.format("output/MetadataResponse.V%d.modified", v));
        }
    }

    private static RequestContext createRequestContext(
        final ApiKeys apiKey, final short version
    ) throws UnknownHostException {
        final var header = new RequestHeader(apiKey, version, "client-id", 999444);
        return new RequestContext(header, "connection-id",
            InetAddress.getLocalHost(), KafkaPrincipal.ANONYMOUS,
            new ListenerName("ssl"), SecurityProtocol.SASL_SSL, ClientInformation.EMPTY, false);
    }

    private static MetadataResponse generateMetadataResponse(final short version, final boolean original) {
        final var brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
        brokers.add(
            new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(0)
                .setHost(BROKER_0_ORIGINAL_HOST)
                .setPort(BROKER_0_ORIGINAL_PORT)
                .setRack("rack0")
        );
        brokers.add(
            new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1)
                .setHost(BROKER_1_ORIGINAL_HOST)
                .setPort(BROKER_1_ORIGINAL_PORT)
                .setRack(null)
        );
        brokers.add(
            new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(2)
                .setHost(BROKER_2_ORIGINAL_HOST)
                .setPort(BROKER_2_ORIGINAL_PORT)
                .setRack("rack2")
        );

        if (!original) {
            brokers.find(0)
                .setHost(LOCALHOST)
                .setPort(BROKER_0_MODIFIED_PORT);
            brokers.find(1)
                .setHost(LOCALHOST)
                .setPort(BROKER_1_MODIFIED_PORT);
            brokers.find(2)
                .setHost(LOCALHOST)
                .setPort(BROKER_2_MODIFIED_PORT);
        }

        final var partitions = new ArrayList<MetadataResponseData.MetadataResponsePartition>();
        partitions.add(new MetadataResponseData.MetadataResponsePartition()
            .setErrorCode((short) 0)
            .setPartitionIndex(0)
            .setLeaderId(0)
            .setReplicaNodes(List.of(0, 1, 2))
            .setIsrNodes(List.of(0, 1, 2))
            .setOfflineReplicas(List.of(2))
            .setLeaderEpoch(10)
        );

        final var topics = new MetadataResponseData.MetadataResponseTopicCollection();
        var topic = new MetadataResponseData.MetadataResponseTopic()
            .setErrorCode((short) 0)
            .setName("topic-0")
            .setIsInternal(false)
            .setPartitions(partitions)
            .setTopicId(Uuid.METADATA_TOPIC_ID);

        if (version >= 8) {
            topic.setTopicAuthorizedOperations(2);
        }
        topics.add(topic);

        var metadataResponseData = new MetadataResponseData()
            .setThrottleTimeMs(12)
            .setBrokers(brokers)
            .setClusterId("cluster-id")
            .setControllerId(1)
            .setTopics(topics);
        if (version >= 8 && version < 11) {
            metadataResponseData.setClusterAuthorizedOperations(5);
        }

        return new MetadataResponse(metadataResponseData, (short) 0);
    }

    private static void writeToFile(
        final RequestContext context,
        final AbstractResponse response,
        final String fileName
    ) throws IOException {
        final Send send = context.buildResponseSend(response);
        final ByteBufferChannel channel = new ByteBufferChannel(1024);
        send.writeTo(channel);
        try (final var fileOutputStream = new FileOutputStream(fileName)) {
            fileOutputStream.write(channel.buffer().array(), 0, channel.buffer().position());
        }
    }
}
