//SPDX-License-Identifier: GPL-2.0
package me.phh.sip

import android.media.AudioTrack
import android.os.SystemClock
import android.telephony.Rlog
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

object SipDownlinkPcmPlayout {
    fun start(
        logTag: String,
        audioTrack: AudioTrack,
        audioCodec: NegotiatedAudioCodec,
        buffers: SipDownlinkPcmPlayoutBuffers,
        callStopped: AtomicBoolean,
        callGeneration: AtomicInteger,
        generation: Int,
    ): Thread =
        thread(name = "PhhDownlinkPcmPlayout") {
            try {
                android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_URGENT_AUDIO)
                Rlog.d(logTag, "Downlink PCM playout thread priority set to urgent audio")
            } catch (t: Throwable) {
                Rlog.w(logTag, "Failed to set downlink PCM playout thread priority", t)
            }

            var fillerFrames = 0
            var nextWriteAtMs = SystemClock.elapsedRealtime() + 60L
            Rlog.d(logTag, "Downlink PCM playout started: frameBytes=${buffers.frameBytes} codec=${audioCodec.name}/${audioCodec.sampleRate} gen=$generation")
            try {
                while (buffers.running.get() && !callStopped.get() && callGeneration.get() == generation) {
                    val now = SystemClock.elapsedRealtime()
                    val sleepMs = nextWriteAtMs - now
                    if (sleepMs > 0L) Thread.sleep(sleepMs.coerceAtMost(40L))

                    val pcm = buffers.pcmQueue.poll() ?: buffers.silenceFrame
                    if (pcm === buffers.silenceFrame) {
                        fillerFrames++
                        if (fillerFrames == 1 || fillerFrames % 50 == 0) {
                            Rlog.d(logTag, "Downlink PCM playout filler frames=$fillerFrames queued=${buffers.pcmQueue.size} gen=$generation")
                        }
                    } else if (fillerFrames > 0) {
                        Rlog.d(logTag, "Downlink PCM playout recovered after fillerFrames=$fillerFrames queued=${buffers.pcmQueue.size} gen=$generation")
                        fillerFrames = 0
                    }

                    audioTrack.write(pcm, 0, pcm.size, AudioTrack.WRITE_BLOCKING)
                    nextWriteAtMs += 20L
                    val afterWriteMs = SystemClock.elapsedRealtime()
                    if (afterWriteMs - nextWriteAtMs > 200L) {
                        nextWriteAtMs = afterWriteMs + 20L
                    }
                }
            } catch (_: InterruptedException) {
                // Normal during call teardown.
            } catch (t: Throwable) {
                Rlog.w(logTag, "Downlink PCM playout failed", t)
            }
            Rlog.d(logTag, "Downlink PCM playout exiting: running=${buffers.running.get()} callStopped=${callStopped.get()} genMismatch=${callGeneration.get() != generation} queued=${buffers.pcmQueue.size}")
        }
}
