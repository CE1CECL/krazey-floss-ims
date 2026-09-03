// SPDX-License-Identifier: GPL-2.0
package me.phh.sip

import android.telephony.Rlog
import java.io.OutputStream

internal object SipMessageWriter {
    private const val DEFAULT_TAG = "PHH SipWriter"

    fun write(
        writer: OutputStream,
        bytes: ByteArray,
        label: String,
    ): Boolean = write(DEFAULT_TAG, writer, bytes, label)

    fun write(
        tag: String,
        writer: OutputStream,
        bytes: ByteArray,
        label: String,
    ): Boolean {
        val firstLine =
            bytes
                .toString(Charsets.US_ASCII)
                .lineSequence()
                .firstOrNull()
                .orEmpty()
        return try {
            synchronized(writer) {
                writer.write(bytes)
                writer.flush()
            }
            true
        } catch (t: Throwable) {
            Rlog.w(
                tag,
                "Failed to write SIP bytes label=$label bytes=${bytes.size} " +
                    "firstLine=$firstLine",
                t,
            )
            false
        }
    }
}
