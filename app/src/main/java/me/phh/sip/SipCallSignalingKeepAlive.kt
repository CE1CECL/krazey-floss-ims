// SPDX-License-Identifier: GPL-2.0
package me.phh.sip

import android.content.Context
import android.net.Network
import android.os.PowerManager
import android.telephony.Rlog
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import kotlin.concurrent.thread

/** Sends Samsung-compatible CRLF call-signaling keepalives on a separate socket. */
internal class SipCallSignalingKeepAlive(
    private val tag: String,
    context: Context,
    private val policy: SipCallSignalingKeepAlivePolicy,
    private val network: () -> Network?,
    private val remoteAddress: () -> InetAddress?,
    private val remotePort: () -> Int,
) {
    companion object {
        private const val LOCAL_PORT = 45_016
        private val PAYLOAD = byteArrayOf(13, 10, 13, 10)
    }

    private val powerManager = context.getSystemService(PowerManager::class.java)

    private val lock = Any()
    private var generation = 0L
    private var worker: Thread? = null
    private var socket: DatagramSocket? = null

    fun start(reason: String) {
        synchronized(lock) {
            if (worker?.isAlive == true) return
            val currentGeneration = ++generation
            worker =
                thread(
                    start = true,
                    isDaemon = true,
                    name = "PhhSipCallKeepAlive",
                ) {
                    runSender(currentGeneration, reason)
                }
        }
    }

    fun stop(reason: String) {
        val oldWorker: Thread?
        synchronized(lock) {
            if (worker == null && socket == null) return
            generation++
            oldWorker = worker
            worker = null
            socket?.close()
            socket = null
        }
        oldWorker?.interrupt()
        if (oldWorker != null && oldWorker !== Thread.currentThread()) {
            try {
                oldWorker.join(1_000L)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        Rlog.d(tag, "Stopped SIP call-signaling keepalive: $reason")
    }

    private fun runSender(
        currentGeneration: Long,
        reason: String,
    ) {
        val selectedNetwork = network()
        val selectedAddress = remoteAddress()
        val selectedPort = remotePort()
        if (selectedNetwork == null || selectedAddress == null || selectedPort <= 0) {
            Rlog.w(tag, "Cannot start SIP call-signaling keepalive: missing endpoint")
            clearWorker(currentGeneration)
            return
        }

        var openedSocket: DatagramSocket? = null
        val runWakeLock =
            powerManager
                .newWakeLock(
                    PowerManager.PARTIAL_WAKE_LOCK,
                    "$tag:call-keepalive",
                ).also { it.setReferenceCounted(false) }
        try {
            runWakeLock.acquire()
            val datagramSocket = openSocket(selectedNetwork)
            openedSocket = datagramSocket
            synchronized(lock) {
                if (generation != currentGeneration) {
                    datagramSocket.close()
                    return
                }
                socket = datagramSocket
            }
            Rlog.d(
                tag,
                "Started SIP call-signaling keepalive: reason=$reason " +
                    "target=${selectedAddress.hostAddress}:$selectedPort " +
                    "intervalMs=${policy.intervalMs}",
            )

            if (policy.delayFirstPacket) sleepInterval()
            while (isCurrent(currentGeneration)) {
                val packet =
                    DatagramPacket(
                        PAYLOAD,
                        PAYLOAD.size,
                        selectedAddress,
                        selectedPort,
                    )
                datagramSocket.send(packet)
                sleepInterval()
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (t: Throwable) {
            if (isCurrent(currentGeneration)) {
                Rlog.w(tag, "SIP call-signaling keepalive failed", t)
            }
        } finally {
            openedSocket?.close()
            if (runWakeLock.isHeld) runWakeLock.release()
            clearWorker(currentGeneration)
        }
    }

    private fun openSocket(selectedNetwork: Network): DatagramSocket {
        val selectedSocket = DatagramSocket(null)
        try {
            selectedSocket.reuseAddress = true
            selectedSocket.bind(InetSocketAddress(LOCAL_PORT))
        } catch (t: Throwable) {
            selectedSocket.close()
            Rlog.w(
                tag,
                "Could not bind Samsung keepalive port $LOCAL_PORT; using an ephemeral port",
                t,
            )
            return DatagramSocket(null).also {
                it.bind(InetSocketAddress(0))
                selectedNetwork.bindSocket(it)
            }
        }
        selectedNetwork.bindSocket(selectedSocket)
        return selectedSocket
    }

    private fun sleepInterval() {
        Thread.sleep(policy.intervalMs.coerceAtLeast(1_000L))
    }

    private fun isCurrent(currentGeneration: Long): Boolean = synchronized(lock) { generation == currentGeneration }

    private fun clearWorker(currentGeneration: Long) {
        synchronized(lock) {
            if (generation == currentGeneration) {
                worker = null
                socket = null
            }
        }
    }
}
