//SPDX-License-Identifier: GPL-2.0
package me.phh.sip

data class SipSecurityServerSelection(
    val type: String,
    val params: Map<String, String>,
)

object SipSecurityServerSelector {
    private val supportedAlgorithms = listOf("hmac-sha-1-96", "hmac-md5-96")
    private val supportedEncryptionAlgorithms = listOf("aes-cbc", "null")

    fun select(securityServerHeaders: List<String>): SipSecurityServerSelection {
        val (type, params) = securityServerHeaders
            .map { it.getParams() }
            .filter {
                val encryptionAlgorithm = it.component2()["ealg"] ?: "null"
                supportedEncryptionAlgorithms.contains(encryptionAlgorithm)
            }
            .filter { supportedAlgorithms.contains(it.component2()["alg"]) }
            .sortedByDescending { it.component2()["q"]?.toFloat() ?: 0.toFloat() }[0]

        require(type == "ipsec-3gpp")
        val nonNullParams = params.entries
            .mapNotNull { (key, value) -> value?.let { key to it } }
            .toMap()
        return SipSecurityServerSelection(type = type, params = nonNullParams)
    }
}
