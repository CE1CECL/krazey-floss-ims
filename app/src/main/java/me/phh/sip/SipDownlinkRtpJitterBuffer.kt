//SPDX-License-Identifier: GPL-2.0
package me.phh.sip

import android.media.MediaCodec
import android.os.Process
import android.os.SystemClock
import android.telephony.Rlog
import java.util.PriorityQueue
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

internal data class SipDownlinkRtpJitterFrame(
    val extSeq: Long,
    val seq: Int,
    val rtpTimestamp: Long,
    val payloadType: Int,
    val frameType: Int,
    val codecFrame: ByteArray,
    val arrivalMs: Long,
)

internal class SipDownlinkRtpJitterBuffer(
    private val logTag: String,
    private val audioCodec: NegotiatedAudioCodec,
    private val targetDelayMs: Long = 80L,
    private val minStartFrames: Int = 4,
    private val maxFrames: Int = 16,
) {
    private val lock = Object()
    private val queue = PriorityQueue<SipDownlinkRtpJitterFrame>(compareBy { it.extSeq })

    private var stopped = false
    private var started = false
    private var startAtMs = 0L
    private var nextPlayoutSeq = -1L
    private var highestExtSeq = -1L

    private var lastLoggedPushSeq = -1L
    private var lastPushExtSeq = -1L
    private var lastPushArrivalMs = -1L
    private var lastPushRtpTimestamp = -1L

    private var pushed = 0
    private var decoded = 0
    private var duplicate = 0
    private var late = 0
    private var overflow = 0
    private var missing = 0
    private var maxQueued = 0

    fun push(
        packet: ByteArray,
        packetLength: Int,
        payloadType: Int,
        frameType: Int,
        codecFrame: ByteArray,
        packetCount: Int,
        nowMs: Long = SystemClock.elapsedRealtime(),
    ) {
        val seq = rtpSequenceNumber(packet, packetLength) ?: return
        val rtpTimestamp = rtpTimestamp(packet, packetLength) ?: 0L

        synchronized(lock) {
            if (stopped) return

            val extSeq = extendSeqLocked(seq)
            val arrivalDeltaMs = if (lastPushArrivalMs >= 0L) nowMs - lastPushArrivalMs else -1L
            val seqDelta = if (lastPushExtSeq >= 0L) extSeq - lastPushExtSeq else 0L
            val rtpDelta = if (lastPushRtpTimestamp >= 0L) rtpTimestamp - lastPushRtpTimestamp else 0L

            if (queue.any { it.extSeq == extSeq }) {
                duplicate++
                return
            }

            if (started && nextPlayoutSeq >= 0L && extSeq < nextPlayoutSeq) {
                late++
                if (late == 1 || late % 20 == 0) {
                    Rlog.w(logTag, "Downlink RTP jitter late drop: seq=$seq ext=$extSeq next=$nextPlayoutSeq stats=${statsLocked()}")
                }
                return
            }

            if (!started) {
                started = true
                nextPlayoutSeq = extSeq
                startAtMs = nowMs + targetDelayMs.coerceAtLeast(0L)
                Rlog.d(
                    logTag,
                    "Downlink RTP jitter started: codec=${audioCodec.name}/${audioCodec.sampleRate} " +
                        "seq=$seq ext=$extSeq targetDelay=${targetDelayMs}ms " +
                        "minStartFrames=$minStartFrames maxFrames=$maxFrames",
                )
            }

            queue.add(
                SipDownlinkRtpJitterFrame(
                    extSeq = extSeq,
                    seq = seq,
                    rtpTimestamp = rtpTimestamp,
                    payloadType = payloadType,
                    frameType = frameType,
                    codecFrame = codecFrame.copyOf(),
                    arrivalMs = nowMs,
                )
            )
            pushed++
            if (queue.size > maxQueued) maxQueued = queue.size

            while (queue.size > maxFrames) {
                val dropped = queue.poll() ?: break
                overflow++
                if (dropped.extSeq >= nextPlayoutSeq) {
                    nextPlayoutSeq = dropped.extSeq + 1L
                }
            }

            val shouldLogNormal = packetCount <= 10 || packetCount % 50 == 0
            val shouldLogGap = seqDelta > 1L || seqDelta < 0L || arrivalDeltaMs > 80L
            if ((shouldLogNormal || shouldLogGap) && lastLoggedPushSeq != extSeq) {
                val levelGap = if (shouldLogGap) "gap" else "push"
                val msg = "Downlink RTP jitter $levelGap: packet=#$packetCount seq=$seq ext=$extSeq " +
                    "dSeq=$seqDelta arrivalDelta=${arrivalDeltaMs}ms rtpDelta=$rtpDelta " +
                    "pt=$payloadType ft=$frameType codecBytes=${codecFrame.size} queued=${queue.size} stats=${statsLocked()}"
                if (shouldLogGap) Rlog.w(logTag, msg) else Rlog.d(logTag, msg)
                lastLoggedPushSeq = extSeq
            }

            lastPushExtSeq = extSeq
            lastPushArrivalMs = nowMs
            lastPushRtpTimestamp = rtpTimestamp
            lock.notifyAll()
        }
    }

    fun pollReady(nowMs: Long = SystemClock.elapsedRealtime()): SipDownlinkRtpJitterFrame? {
        synchronized(lock) {
            if (stopped || !started) return null

            if (nowMs < startAtMs && queue.size < minStartFrames) {
                return null
            }

            while (true) {
                val head = queue.peek() ?: return null

                if (head.extSeq < nextPlayoutSeq) {
                    queue.poll()
                    late++
                    continue
                }

                if (head.extSeq == nextPlayoutSeq) {
                    queue.poll()
                    nextPlayoutSeq++
                    decoded++
                    return head
                }

                // A packet is missing before the oldest queued packet.  Give it
                // the target delay window, then skip forward.  Otherwise one lost
                // RTP packet can block all later audio and cause audible stalls.
                val waitedMs = nowMs - head.arrivalMs
                if (waitedMs >= targetDelayMs) {
                    val skipped = (head.extSeq - nextPlayoutSeq).coerceAtLeast(1L)
                    missing += skipped.toInt().coerceAtMost(999)
                    Rlog.w(
                        logTag,
                        "Downlink RTP jitter missing skip: next=$nextPlayoutSeq head=${head.extSeq} " +
                            "skipped=$skipped waited=${waitedMs}ms stats=${statsLocked()}",
                    )
                    nextPlayoutSeq = head.extSeq
                    continue
                }

                return null
            }
        }
    }

    fun waitForWork(timeoutMs: Long) {
        synchronized(lock) {
            if (!stopped) {
                try {
                    lock.wait(timeoutMs.coerceAtLeast(1L))
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        }
    }

    fun stop() {
        synchronized(lock) {
            stopped = true
            queue.clear()
            lock.notifyAll()
        }
    }

    fun isStopped(): Boolean = synchronized(lock) { stopped }

    fun stats(): String = synchronized(lock) { statsLocked() }

    private fun statsLocked(): String =
        "queued=${queue.size} pushed=$pushed decoded=$decoded next=$nextPlayoutSeq " +
            "late=$late dup=$duplicate overflow=$overflow missing=$missing maxQueued=$maxQueued"

    private fun extendSeqLocked(seq: Int): Long {
        val s = seq and 0xffff

        if (highestExtSeq < 0L) {
            highestExtSeq = s.toLong()
            return highestExtSeq
        }

        val base = highestExtSeq and 0xffff0000L
        var candidate = base + s.toLong()

        if (candidate - highestExtSeq > 32768L) {
            candidate -= 65536L
        } else if (highestExtSeq - candidate > 32768L) {
            candidate += 65536L
        }

        if (candidate > highestExtSeq) highestExtSeq = candidate
        return candidate
    }

    private fun rtpSequenceNumber(packet: ByteArray, packetLength: Int): Int? {
        if (packetLength < 4) return null
        return ((packet[2].toInt() and 0xff) shl 8) or
            (packet[3].toInt() and 0xff)
    }

    private fun rtpTimestamp(packet: ByteArray, packetLength: Int): Long? {
        if (packetLength < 8) return null
        return ((packet[4].toLong() and 0xffL) shl 24) or
            ((packet[5].toLong() and 0xffL) shl 16) or
            ((packet[6].toLong() and 0xffL) shl 8) or
            (packet[7].toLong() and 0xffL)
    }
}

internal object SipDownlinkRtpJitterDecoder {
    fun start(
        logTag: String,
        jitterBuffer: SipDownlinkRtpJitterBuffer,
        decoder: MediaCodec,
        pcmQueue: ArrayBlockingQueue<ByteArray>,
        callStopped: AtomicBoolean,
        callGeneration: AtomicInteger,
        generation: Int,
    ): Thread =
        thread(name = "PhhDownlinkRtpJitter") {
            try {
                try {
                    Process.setThreadPriority(Process.THREAD_PRIORITY_AUDIO)
                } catch (t: Throwable) {
                    Rlog.d(logTag, "Downlink RTP jitter decoder priority set failed", t)
                }

                var decodedFrames = 0
                var idleTicks = 0
                Rlog.d(logTag, "Downlink RTP jitter decoder started: gen=$generation")

                while (!jitterBuffer.isStopped() && !callStopped.get() && callGeneration.get() == generation) {
                    val frame = jitterBuffer.pollReady(SystemClock.elapsedRealtime())
                    if (frame == null) {
                        idleTicks++
                        if (idleTicks == 200 || idleTicks % 1000 == 0) {
                            Rlog.d(logTag, "Downlink RTP jitter decoder waiting: gen=$generation stats=${jitterBuffer.stats()}")
                        }
                        jitterBuffer.waitForWork(5L)
                        continue
                    }

                    idleTicks = 0
                    SipDownlinkAudioDecoder.queueCodecFrameAndDrainPcm(
                        logTag = logTag,
                        decoder = decoder,
                        codecFrame = frame.codecFrame,
                        pcmQueue = pcmQueue,
                    )
                    decodedFrames++

                    if (decodedFrames <= 10 || decodedFrames % 100 == 0) {
                        Rlog.d(
                            logTag,
                            "Downlink RTP jitter decoded: decoded=$decodedFrames seq=${frame.seq} " +
                                "ext=${frame.extSeq} pt=${frame.payloadType} ft=${frame.frameType} " +
                                "codecBytes=${frame.codecFrame.size} gen=$generation stats=${jitterBuffer.stats()}",
                        )
                    }
                }
            } catch (_: InterruptedException) {
                // Normal during call teardown.
            } catch (t: Throwable) {
                Rlog.w(logTag, "Downlink RTP jitter decoder failed", t)
            }

            Rlog.d(
                logTag,
                "Downlink RTP jitter decoder exiting: callStopped=${callStopped.get()} " +
                    "genMismatch=${callGeneration.get() != generation} stats=${jitterBuffer.stats()}",
            )
        }
}
