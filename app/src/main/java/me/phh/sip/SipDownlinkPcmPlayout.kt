//SPDX-License-Identifier: GPL-2.0
package me.phh.sip

import android.media.AudioTrack
import android.os.Process
import android.os.SystemClock
import android.telephony.Rlog
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

object SipDownlinkPcmPlayout {
    // PhhIms jitter test playout prebuffer.
    // Keep the existing PCM-side smoother, but avoid starting playback on an
    // almost-empty queue and tolerate tiny decoder/scheduler delays before
    // falling back to filler silence.
    private const val FRAME_MS = 20L
    private const val DEFAULT_TARGET_DELAY_MS = 80L
    private const val DEFAULT_MIN_START_FRAMES = 4
    private const val DEFAULT_LATE_POLL_MS = 12L
    private const val CLOCK_STALL_RESET_MS = 200L

    fun start(
        logTag: String,
        audioTrack: AudioTrack,
        audioCodec: NegotiatedAudioCodec,
        buffers: SipDownlinkPcmPlayoutBuffers,
        callStopped: AtomicBoolean,
        callGeneration: AtomicInteger,
        generation: Int,
        targetDelayMs: Long = DEFAULT_TARGET_DELAY_MS,
        minStartFrames: Int = DEFAULT_MIN_START_FRAMES,
        latePollMs: Long = DEFAULT_LATE_POLL_MS,
    ): Thread =
        thread(name = "PhhDownlinkPcmPlayout") {
            try {
                try {
                    Process.setThreadPriority(Process.THREAD_PRIORITY_AUDIO)
                } catch (t: Throwable) {
                    Rlog.d(logTag, "Downlink PCM playout priority set failed", t)
                }

                var fillerFrames = 0
                val prebufferUntilMs = SystemClock.elapsedRealtime() + targetDelayMs.coerceAtLeast(0L)
                var prebufferLogged = false

                while (buffers.running.get() && !callStopped.get() && callGeneration.get() == generation) {
                    val nowMs = SystemClock.elapsedRealtime()
                    val queued = buffers.pcmQueue.size
                    if (queued >= minStartFrames || nowMs >= prebufferUntilMs) break

                    if (!prebufferLogged) {
                        Rlog.d(
                            logTag,
                            "Downlink PCM playout prebuffering: queued=$queued " +
                                "targetDelay=${targetDelayMs}ms minStartFrames=$minStartFrames gen=$generation",
                        )
                        prebufferLogged = true
                    }
                    Thread.sleep(5L)
                }

                var nextWriteAtMs = SystemClock.elapsedRealtime()
                Rlog.d(
                    logTag,
                    "Downlink PCM playout started: frameBytes=${buffers.frameBytes} " +
                        "codec=${audioCodec.name}/${audioCodec.sampleRate} " +
                        "targetDelay=${targetDelayMs}ms minStartFrames=$minStartFrames " +
                        "latePoll=${latePollMs}ms queued=${buffers.pcmQueue.size} gen=$generation",
                )

                while (buffers.running.get() && !callStopped.get() && callGeneration.get() == generation) {
                    val nowMs = SystemClock.elapsedRealtime()
                    val sleepMs = nextWriteAtMs - nowMs
                    if (sleepMs > 0L) Thread.sleep(sleepMs.coerceAtMost(40L))

                    val pcm = buffers.pcmQueue.poll(latePollMs.coerceAtLeast(0L), TimeUnit.MILLISECONDS)
                        ?: buffers.silenceFrame

                    if (pcm === buffers.silenceFrame) {
                        fillerFrames++
                        if (fillerFrames == 1 || fillerFrames % 25 == 0) {
                            Rlog.d(
                                logTag,
                                "Downlink PCM playout filler frames=$fillerFrames " +
                                    "queued=${buffers.pcmQueue.size} gen=$generation",
                            )
                        }
                    } else if (fillerFrames > 0) {
                        Rlog.d(
                            logTag,
                            "Downlink PCM playout recovered after fillerFrames=$fillerFrames " +
                                "queued=${buffers.pcmQueue.size} gen=$generation",
                        )
                        fillerFrames = 0
                    }

                    val written = audioTrack.write(pcm, 0, pcm.size, AudioTrack.WRITE_BLOCKING)
                    if (written < 0) {
                        Rlog.w(logTag, "Downlink PCM AudioTrack write failed: ret=$written gen=$generation")
                    }

                    nextWriteAtMs += FRAME_MS
                    val afterWriteMs = SystemClock.elapsedRealtime()
                    val lagMs = afterWriteMs - nextWriteAtMs
                    if (lagMs > CLOCK_STALL_RESET_MS) {
                        Rlog.w(
                            logTag,
                            "Downlink PCM playout clock reset after stall: " +
                                "lag=${lagMs}ms queued=${buffers.pcmQueue.size} gen=$generation",
                        )
                        nextWriteAtMs = afterWriteMs + FRAME_MS
                    }
                }
            } catch (_: InterruptedException) {
                // Normal during call teardown.
            } catch (t: Throwable) {
                Rlog.w(logTag, "Downlink PCM playout failed", t)
            }
            Rlog.d(
                logTag,
                "Downlink PCM playout exiting: running=${buffers.running.get()} " +
                    "callStopped=${callStopped.get()} " +
                    "genMismatch=${callGeneration.get() != generation} " +
                    "queued=${buffers.pcmQueue.size}",
            )
        }
}
