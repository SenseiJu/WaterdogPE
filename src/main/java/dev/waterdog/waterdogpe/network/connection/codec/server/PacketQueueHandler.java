package dev.waterdog.waterdogpe.network.connection.codec.server;

import dev.waterdog.waterdogpe.network.NetworkMetrics;
import dev.waterdog.waterdogpe.network.connection.codec.batch.BatchFlags;
import dev.waterdog.waterdogpe.network.connection.codec.packet.BedrockPacketCodec;
import dev.waterdog.waterdogpe.network.connection.peer.BedrockServerSession;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.PlatformDependent;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import lombok.extern.log4j.Log4j2;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.codec.BedrockPacketDefinition;
import org.cloudburstmc.protocol.bedrock.netty.BedrockBatchWrapper;
import org.cloudburstmc.protocol.bedrock.netty.BedrockPacketWrapper;

import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
public class PacketQueueHandler extends ChannelDuplexHandler {
    public static final String NAME = "packet-queue-handler";
    private static final int MAX_BATCHES = 256;
    private static final int MAX_PACKETS = 8000;
    // Emit a growth checkpoint every time the queued packet count crosses a multiple of this, so the
    // fill rate is visible in debug logs before an eventual overflow (only logged when debug is enabled).
    private static final int QUEUE_LOG_INTERVAL = 2000;

    private final BedrockServerSession session;

    private int packetCounter = 0;
    private long queueStartedAt = 0;
    private final Queue<BedrockBatchWrapper> queue = PlatformDependent.newMpscQueue(MAX_BATCHES);

    private volatile boolean finished;
    private volatile boolean dropQueued;

    public PacketQueueHandler(BedrockServerSession session) {
        this.session = session;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.queueStartedAt = System.nanoTime();
        log.debug("[{}] transfer queue started", this.session.getSocketAddress());
    }

    /**
     * Drop queued batches instead of flushing them on removal.
     * Used when the transfer fails and the queue holds packets from the abandoned target server.
     */
    public void dropQueued() {
        this.dropQueued = true;
    }

    private void finish(ChannelHandlerContext ctx, boolean send) {
        if (this.finished) {
            return;
        }
        this.finished = true;

        if (ctx.pipeline().get(NAME) == this) {
            ctx.pipeline().remove(this);
        }

        BedrockBatchWrapper batch;
        while ((batch = this.queue.poll()) != null) {
            if (send) {
                ctx.write(batch);
            } else {
                batch.release();
            }
        }

        if (send) {
            ctx.flush();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        this.finish(ctx, false);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.finish(ctx, !this.dropQueued && ctx.channel().isActive());
    }

    /**
     * Build a human-readable breakdown of the packet ids currently sitting in the transfer queue
     * (plus the batch that pushed it over the limit), sorted by frequency. Used purely for diagnostics
     * when the queue overflows so we can tell which packet type is flooding the transfer.
     */
    private String dumpPacketIds(ChannelHandlerContext ctx, BedrockBatchWrapper overflow) {
        Int2IntOpenHashMap counts = new Int2IntOpenHashMap();
        int totalPackets = 0;

        for (BedrockBatchWrapper batch : this.queue) {
            for (BedrockPacketWrapper packet : batch.getPackets()) {
                counts.addTo(packet.getPacketId(), 1);
                totalPackets++;
            }
        }
        for (BedrockPacketWrapper packet : overflow.getPackets()) {
            counts.addTo(packet.getPacketId(), 1);
            totalPackets++;
        }

        if (counts.isEmpty()) {
            return "<empty>";
        }

        BedrockPacketCodec packetCodec = ctx.pipeline().get(BedrockPacketCodec.class);
        BedrockCodec codec = packetCodec == null ? null : packetCodec.getCodec();

        String breakdown = counts.int2IntEntrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getIntValue(), a.getIntValue()))
                .map(entry -> packetName(codec, entry.getIntKey()) + "(id=" + entry.getIntKey() + ") x" + entry.getIntValue())
                .collect(Collectors.joining(", "));
        return "total=" + totalPackets + " uniqueIds=" + counts.size() + " [" + breakdown + "]";
    }

    /**
     * Resolve the packet class name for the given id using the session codec, falling back to "Unknown"
     * if the codec is unavailable or the id is not registered. Packet ids are protocol-version specific,
     * so the name can only be resolved via the codec attached to this connection.
     */
    private static String packetName(BedrockCodec codec, int id) {
        if (codec != null) {
            try {
                BedrockPacketDefinition<?> definition = codec.getPacketDefinition(id);
                if (definition != null && definition.getFactory() != null) {
                    return definition.getFactory().get().getClass().getSimpleName();
                }
            } catch (Throwable t) {
                // Diagnostics must never throw - fall through to the unknown label.
            }
        }
        return "Unknown";
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (this.finished || !(msg instanceof BedrockBatchWrapper batch) || batch.hasFlag(BatchFlags.SKIP_QUEUE)) {
            ctx.write(msg, promise);
            return;
        }

        if (this.queue.offer(batch) && this.packetCounter < MAX_PACKETS) {
            int previousCounter = this.packetCounter;
            this.packetCounter += batch.getPackets().size();
            if (log.isDebugEnabled() && previousCounter / QUEUE_LOG_INTERVAL != this.packetCounter / QUEUE_LOG_INTERVAL) {
                long elapsedMs = this.queueStartedAt == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.queueStartedAt);
                log.debug("[{}] transfer queue growing: batches={} packets={} elapsed={}ms",
                        this.session.getSocketAddress(), this.queue.size(), this.packetCounter, elapsedMs);
            }
        } else {
            long buildTimeMs = this.queueStartedAt == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.queueStartedAt);
            log.warn("[{}] has reached maximum transfer queue capacity: batches={} packets={} buildTime={}ms limits=[batches={} packets={}]",
                    this.session.getSocketAddress(), this.queue.size(), this.packetCounter, buildTimeMs, MAX_BATCHES, MAX_PACKETS);
            log.warn("[{}] transfer queue packet id breakdown: {}", this.session.getSocketAddress(), this.dumpPacketIds(ctx, batch));
            this.finish(ctx, false);
            this.session.disconnect("Transfer queue got too large");

            NetworkMetrics metrics = ctx.channel().attr(NetworkMetrics.ATTRIBUTE).get();
            if (metrics != null) {
                metrics.packetQueueTooLarge();
            }
        }
    }
}
