/*
 * Copyright 2022 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.network.protocol.handler;

import dev.waterdog.waterdogpe.event.defaults.PostTransferCompleteEvent;
import dev.waterdog.waterdogpe.event.defaults.ServerTransferFailedEvent;
import dev.waterdog.waterdogpe.event.defaults.TransferCompleteEvent;
import dev.waterdog.waterdogpe.network.connection.client.ClientConnection;
import dev.waterdog.waterdogpe.network.connection.handler.ReconnectReason;
import dev.waterdog.waterdogpe.network.protocol.ProtocolVersion;
import dev.waterdog.waterdogpe.network.protocol.handler.downstream.ConnectedDownstreamHandler;
import dev.waterdog.waterdogpe.network.protocol.handler.upstream.ConnectedUpstreamHandler;
import dev.waterdog.waterdogpe.network.serverinfo.ServerInfo;
import dev.waterdog.waterdogpe.network.protocol.rewrite.types.RewriteData;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import dev.waterdog.waterdogpe.scheduler.TaskHandler;
import dev.waterdog.waterdogpe.utils.types.TranslationContainer;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.cloudburstmc.math.vector.Vector3f;
import org.cloudburstmc.math.vector.Vector3i;
import org.cloudburstmc.protocol.bedrock.packet.SetLocalPlayerAsInitializedPacket;
import org.cloudburstmc.protocol.bedrock.packet.StopSoundPacket;

import java.util.List;

import static dev.waterdog.waterdogpe.network.protocol.user.PlayerRewriteUtils.*;
import static dev.waterdog.waterdogpe.network.protocol.handler.TransferCallback.TransferPhase.*;

public class TransferCallback {

    /**
     * Hard cap on how long the dimension change sequence may take after StartGame claimed the
     * transfer. Past that point the old server is gone, so a stuck client or server can only
     * be resolved by kicking the player.
     */
    private static final int TRANSFER_TIMEOUT_SECONDS = 60;

    public enum TransferPhase {
        RESET,
        PHASE_1,
        PHASE_2
    }

    private final ProxiedPlayer player;
    private final ClientConnection connection;
    private final ServerInfo targetServer;
    private final ServerInfo sourceServer;
    private final int targetDimension;

    private volatile TransferPhase transferPhase = PHASE_1;
    private volatile boolean finalized = false;
    private volatile boolean hasPlayStatus = false;
    private volatile TaskHandler<?> timeoutTask;

    private LongSet pendingAckColumns;
    private int pendingAckDimension;

    public TransferCallback(ProxiedPlayer player, ClientConnection connection, ServerInfo sourceServer, int targetDimension) {
        this.player = player;
        this.connection = connection;
        this.targetServer = connection.getServerInfo();
        this.sourceServer = sourceServer;
        this.targetDimension = targetDimension;
    }

    /**
     * Correlation id for logs: the upstream socket address plus the player name. The address matches the
     * one PacketQueueHandler logs, so transfer-flow lines and transfer-queue lines can be traced together.
     */
    private String logId() {
        return this.player.getAddress() + "|" + this.player.getName();
    }

    /**
     * Started by the winner of the transfer claim: kicks the player if the transfer never
     * finishes nor fails on its own.
     */
    public void startTimeout() {
        this.timeoutTask = this.player.getProxy().getScheduler().scheduleDelayed(this::onTimeout, TRANSFER_TIMEOUT_SECONDS * 20);
    }

    private void cancelTimeout() {
        TaskHandler<?> task = this.timeoutTask;
        if (task != null) {
            task.cancel();
        }
    }

    private synchronized void onTimeout() {
        if (this.finalized || !this.player.isConnected()) {
            return;
        }
        // Resolve the transfer state first so the disconnect cascade does not report this
        // as another failure.
        TransferPhase phase = this.transferPhase;
        this.transferPhase = RESET;
        this.finalized = true;
        this.pendingAckColumns = null;
        this.player.getRewriteData().clearTransferCallback(this);

        this.player.getLogger().warning("[" + this.logId() + "] Transfer to " + this.targetServer.getServerName()
                + " timed out in phase " + phase + " (spawned=" + this.hasPlayStatus + ")");
        this.player.getProxy().getEventManager().callEvent(new ServerTransferFailedEvent(
                this.player, this.targetServer, ReconnectReason.TIMEOUT, "Transfer timed out", false));
        this.player.disconnect(new TranslationContainer("waterdog.downstream.transfer.failed", this.targetServer.getServerName(), "Transfer timed out"));
    }

    /**
     * In sub-chunk request mode the fake chunks are empty shells the client still has to request.
     * Sending the server-side dim change ACK before those requests can race the client's chunk
     * loading, so it is held back until every column was requested. An empty column list means
     * nothing to wait for and the ACK is sent right away.
     */
    public synchronized void armDimChangeAck(LongList requestModeColumns, int dimension) {
        if (requestModeColumns.isEmpty() || this.player.getProtocol().isBefore(ProtocolVersion.MINECRAFT_PE_1_19_50)) {
            injectDimensionChangeAck(this.player.getConnection(), this.player.getRewriteData().getEntityId(), this.player.getProtocol());
            return;
        }
        this.pendingAckColumns = new LongOpenHashSet(requestModeColumns);
        this.pendingAckDimension = dimension;
        this.player.getLogger().debug("[{}] Deferred dim change ACK armed: columns={} dim={}",
                this.logId(), requestModeColumns.size(), dimension);
    }

    public synchronized void onSubChunkRequest(int dimension, Vector3i center, List<Vector3i> offsets) {
        if (this.pendingAckColumns == null) {
            this.player.getLogger().debug("[{}] sub-chunk request (no deferred ACK pending): dim={} center=({},{}) offsets={}",
                    this.logId(), dimension, center.getX(), center.getZ(), offsets.size());
            return;
        }
        if (dimension != this.pendingAckDimension) {
            this.player.getLogger().debug("[{}] sub-chunk request for dim={} but deferred ACK armed for dim={} - not counted (center=({},{}) offsets={})",
                    this.logId(), dimension, this.pendingAckDimension, center.getX(), center.getZ(), offsets.size());
            return;
        }
        int before = this.pendingAckColumns.size();
        if (offsets.isEmpty()) {
            this.pendingAckColumns.remove(chunkKey(center.getX(), center.getZ()));
        } else {
            for (Vector3i offset : offsets) {
                this.pendingAckColumns.remove(chunkKey(center.getX() + offset.getX(), center.getZ() + offset.getZ()));
            }
        }
        this.player.getLogger().debug("[{}] sub-chunk request: center=({},{}) offsets={} pending columns {} -> {} | left: {}",
                this.logId(), center.getX(), center.getZ(), offsets.size(), before, this.pendingAckColumns.size(),
                remainingColumnsPreview());
        if (this.pendingAckColumns.isEmpty()) {
            this.player.getLogger().debug("[{}] All fake columns requested, sending deferred dim change ACK (dim={})",
                    this.logId(), dimension);
            this.sendDeferredAck();
        }
    }

    /**
     * Formats the still-unrequested fake columns (up to 24) as (chunkX,chunkZ) pairs so a stuck transfer
     * shows exactly which columns the client never asked for.
     */
    private String remainingColumnsPreview() {
        if (this.pendingAckColumns.isEmpty()) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        LongIterator it = this.pendingAckColumns.iterator();
        while (it.hasNext()) {
            long key = it.nextLong();
            if (shown++ >= 24) {
                sb.append("...");
                break;
            }
            sb.append('(').append((int) (key >> 32)).append(',').append((int) key).append(')');
        }
        return sb.toString();
    }

    private void sendDeferredAck() {
        this.pendingAckColumns = null;
        injectDimensionChangeAck(this.player.getConnection(), this.player.getRewriteData().getEntityId(), this.player.getProtocol());
    }

    public synchronized boolean onDimChangeSuccess() {
        this.player.getLogger().debug("[{}] onDimChangeSuccess in phase {} (target={})",
                this.logId(), this.transferPhase, this.targetServer.getServerName());
        if (this.pendingAckColumns != null) {
            // Client finished the dim change without requesting every column, the ACK must not be withheld now.
            this.player.getLogger().debug("[{}] Client dim change done with {} columns never requested, sending deferred ACK",
                    this.logId(), this.pendingAckColumns.size());
            this.sendDeferredAck();
        }
        switch (this.transferPhase) {
            case PHASE_1:
                // First dimension change was completed successfully.
                this.onTransferPhase1Completed();
                break;
            case PHASE_2:
                // At this point dimension change sequence was completed.
                // We can finally fully initialize connection.
                this.onTransferPhase2Completed();
                break;
            default:
                this.player.getLogger().debug("[{}] onDimChangeSuccess ignored: transfer not in a dimension-change phase (phase={})",
                        this.logId(), this.transferPhase);
                return false;
        }
        return true;
    }

    private void onTransferPhase1Completed() {
        RewriteData rewriteData = this.player.getRewriteData();
        injectEntityImmobile(this.player.getConnection(), rewriteData.getEntityId(), true);

        Vector3f fakePosition = rewriteData.getSpawnPosition().add(-2000, 0, -2000);
        if (this.player.getProtocol().isAfterOrEqual(ProtocolVersion.MINECRAFT_PE_1_19_50)) {
            injectInputLocks(this.player.getConnection(), INPUT_LOCK_FREEZE, fakePosition);
        }

        if (rewriteData.getDimension() != this.targetDimension) {
            injectPosition(this.player.getConnection(), fakePosition, rewriteData.getRotation(), rewriteData.getEntityId());
            rewriteData.setDimension(determineDimensionId(rewriteData.getDimension(), this.targetDimension));
            LongList requestModeColumns = injectDimensionChange(this.player.getConnection(), rewriteData.getDimension(),
                    rewriteData.getSpawnPosition(), this.player.getProtocol(), true, this.player.isSubChunkRequestMode());
            this.armDimChangeAck(requestModeColumns, rewriteData.getDimension());
        }

        // Hand the client over to the new server and flush the queue so its real chunks reach the client. This
        // is the single wiring point for every transfer path; phase 2 only finalizes.
        if (this.player.getConnection().getPacketHandler() instanceof ConnectedUpstreamHandler handler) {
            handler.setTargetConnection(this.connection);
        }
        this.player.getConnection().setTransferQueueActive(false, this.player.getName());
        this.transferPhase = PHASE_2;
        this.player.getLogger().debug("[{}] Transfer phase 1 complete, transfer queue flushed, awaiting phase 2 dimension change",
                this.logId());
    }

    private void onTransferPhase2Completed() {
        if (!this.connection.isConnected()) {
            this.onTransferFailed("Downstream Disconnected");
            return;
        }

        RewriteData rewriteData = this.player.getRewriteData();

        StopSoundPacket soundPacket = new StopSoundPacket();
        soundPacket.setSoundName("portal.travel");
        soundPacket.setStoppingAllSound(true);
        this.player.sendPacketImmediately(soundPacket);

        injectPosition(this.player.getConnection(), rewriteData.getSpawnPosition(), rewriteData.getRotation(), rewriteData.getEntityId());

        if (!rewriteData.hasImmobileFlag()) {
            injectEntityImmobile(this.player.getConnection(), rewriteData.getEntityId(), false);
        }
        if (this.player.getProtocol().isAfterOrEqual(ProtocolVersion.MINECRAFT_PE_1_19_50)) {
            injectInputLocks(this.player.getConnection(), this.player.getInputLockData(), rewriteData.getSpawnPosition());
        }
        this.connection.setPacketHandler(new ConnectedDownstreamHandler(player, this.connection));

        // RESET before the event so handlers may start a new transfer right away.
        this.transferPhase = RESET;
        this.player.getLogger().debug("[{}] Transfer phase 2 complete (spawned={})", this.logId(), this.hasPlayStatus);

        TransferCompleteEvent event = new TransferCompleteEvent(this.sourceServer, this.connection, this.player);
        this.player.getProxy().getEventManager().callEvent(event);

        tryTransferFinalize();
    }

    /**
     * This function will check if we have finished the dimension exchange and got PlayStatus.PLAYER_SPAWN
     * Only then it will finalize the transfer.
     */
    public synchronized void tryTransferFinalize() {
        if (this.finalized || !this.hasPlayStatus || this.transferPhase != RESET) {
            this.player.getLogger().debug("[{}] Transfer not finalized yet: finalized={} spawned={} phase={}",
                    this.logId(), this.finalized, this.hasPlayStatus, this.transferPhase);
            return;
        }
        this.finalized = true;
        this.cancelTimeout();
        this.player.getLogger().debug("[{}] Transfer to {} finalized", this.logId(), this.targetServer.getServerName());
        // The callback stays registered until now so a PLAYER_SPAWN arriving after phase 2 can still
        // finalize the transfer through AbstractDownstreamHandler.
        this.player.getRewriteData().clearTransferCallback(this);

        SetLocalPlayerAsInitializedPacket initializedPacket = new SetLocalPlayerAsInitializedPacket();
        initializedPacket.setRuntimeEntityId(this.player.getRewriteData().getOriginalEntityId());
        this.connection.sendPacket(initializedPacket);

        PostTransferCompleteEvent event = new PostTransferCompleteEvent(this.connection, this.player);
        this.player.getProxy().getEventManager().callEvent(event);
    }

    public void onPlayStatus() {
        this.hasPlayStatus = true;
        this.player.getLogger().debug("[{}] Received PLAYER_SPAWN play status (phase={})", this.logId(), this.transferPhase);
        tryTransferFinalize();
    }

    public synchronized void onTransferFailed(String reason) {
        if (this.transferPhase == RESET) {
            return; // already completed or failed
        }
        // Release the transfer state first: the fallback connect() below is blocked while this
        // callback is active, and queued packets from the abandoned target must never reach the client.
        this.transferPhase = RESET;
        this.finalized = true; // a late PLAYER_SPAWN must not finalize a failed transfer
        this.pendingAckColumns = null;
        this.cancelTimeout();
        this.player.getRewriteData().clearTransferCallback(this);
        this.player.getConnection().discardTransferQueue();

        this.player.getProxy().getEventManager().callEvent(new ServerTransferFailedEvent(
                this.player, this.targetServer, ReconnectReason.TRANSFER_FAILED, reason, false));

        if (this.player.sendToFallback(this.targetServer, ReconnectReason.TRANSFER_FAILED, "Disconnected")) {
            this.player.sendMessage(new TranslationContainer("waterdog.connected.fallback", this.targetServer.getServerName()));
        } else {
            this.player.disconnect(new TranslationContainer("waterdog.downstream.transfer.failed", targetServer.getServerName(), reason));
        }

        this.connection.disconnect();
        this.player.getLogger().warning("[" + this.logId() + "] Failed to transfer to " + this.targetServer.getServerName() + ": " + reason);
    }

    public TransferPhase getPhase() {
        return this.transferPhase;
    }

    public ServerInfo getTargetServer() {
        return this.targetServer;
    }

    public ClientConnection getConnection() {
        return this.connection;
    }
}
