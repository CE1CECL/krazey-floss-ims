# PhhIms — VoLTE/VoWiFi for LineageOS on Samsung devices

Open-source SIP/IMS stack for LineageOS, based on [phhusson/ims](https://github.com/phhusson/ims) and the Samsung-focused fork history from [amikhasenko/ims](https://github.com/amikhasenko/ims).

This fork is used as a userspace `ImsService`/MmTel provider for Samsung Exynos devices where the vendor IMS stack is missing, unusable, or not easily portable to current LineageOS releases.

The current work mainly targets Samsung Exynos LineageOS 23.x / Android 16 bring-up, with testing around:

- Samsung Galaxy A21s / Exynos850 (`a21s`, original bring-up target)
- Samsung Galaxy S9 / S9+ / Note9 / Exynos9810 (`starlte`, `star2lte`, `crownlte`)
- Samsung Galaxy S20 5G / Exynos9830 (`x1s`)
- O2 Germany as the main known-good carrier test environment

This is not a drop-in universal IMS replacement. It depends on carrier provisioning, Samsung RIL behavior, device overlays, sepolicy, audio HAL behavior, and correct ROM-side integration.

## What this app does

Android expects a privileged app implementing `android.telephony.ims.ImsService` and registering itself as the MmTel provider. This app provides that service with a pure-userspace SIP/IMS stack.

At a high level it:

- requests and tracks the IMS bearer network
- reads P-CSCF information from `LinkProperties` or falls back to 3GPP DNS discovery
- performs SIP AKA registration
- reports IMS registration state back to Android telephony
- handles VoLTE/VoWiFi voice calls with SIP and RTP
- handles basic SMS over IMS
- bridges incoming and outgoing SIP call state into Android `ImsCallSession` callbacks

## Current status

Status is based on the current Samsung LineageOS 23.x test branches. Expect carrier and device differences.

| Area | Status |
| --- | --- |
| IMS registration | Working in current tests, including retry/reconnect handling after IMS bearer loss or failed REGISTER attempts. |
| VoLTE outgoing calls | Working in tested configs, including Android call UI, SIP call setup, and two-way audio when ROM-side audio fixes are present. |
| VoLTE incoming calls | Recently fixed/tested for accept and reject paths, but still the most sensitive area. Re-test after every dialog/call-state change. |
| VoWiFi | Experimental. IWLAN/LTE transitions and stale IMS network handling need careful testing. |
| SMS over IMS | Implemented and basically tested, but not as broadly validated as voice. |
| Video calling / RCS / UT | Not a goal for now. Voice and basic SMS are the focus. |

A typical failure mode is: the phone shows LTE, but IMS is not registered, Android falls back to circuit-switched calling, and the modem drops to 2G/EDGE for the call. In that case the interesting logs are around IMS network acquisition, P-CSCF discovery, SIP REGISTER, 401 challenge handling, and reconnect retry behavior.

## Important Samsung-specific background

This fork exists because Samsung devices usually do not expose a clean, generic AOSP IMS stack. A working ROM needs cooperation between several layers:

1. Android telephony must bind this package as the MmTel IMS provider.
2. Carrier config and framework overlays must expose VoLTE/VoWiFi capability.
3. The RIL must expose a usable IMS APN/network and P-CSCF information, or DNS fallback must work.
4. Samsung audio HAL routing must allow userspace RTP audio instead of forcing modem/baseband call paths.
5. IMS registration must survive network loss, IWLAN/LTE transitions, and REGISTER failures.
6. Incoming SIP dialog state must be bridged correctly into Android call sessions, otherwise the UI may show an incoming call while the remote side keeps ringing.

The code has been iterated around these problem areas:

- delayed SIP handler startup until a valid service state/RPLMN exists
- correct AKA challenge realm handling for SIP registration
- reconnect/backoff after IMS bearer loss or failed REGISTER attempts
- avoiding IMS access switches while a call is active or pending
- outgoing provisional response handling for ringback/progress
- separate incoming/outgoing call session state
- incoming `INVITE` parsing robustness
- incoming accept path: build dialog state before notifying Android, then send `200 OK` and handle ACK correctly
- incoming reject path: signal busy/reject to the remote side instead of only closing Android UI state
- call cleanup after BYE/CANCEL/network failure
- SMS-over-IMS plumbing via the same SIP handler

## Repository integration

### Local manifest

```xml
<project path="packages/apps/PhhIms" remote="github" name="krazey/ims" revision="main" />
```

### `lineage.dependencies`

```json
{
  "repository": "krazey/ims",
  "target_path": "packages/apps/PhhIms",
  "branch": "main"
}
```

After syncing, initialize the `rnnoise` submodule. `repo sync` does not do this automatically:

```sh
cd packages/apps/PhhIms
git submodule update --init app/jni/rnnoise
```

## Building in-tree

Use the Soong/LineageOS build. This is the intended build path.

`Android.bp` builds `PhhIms` as a privileged platform-signed app using `platform_apis: true`, so it can access the internal telephony/IMS APIs required by `MmTelFeature`, `ImsConfigImplBase`, `Rlog`, and friends.

No Gradle build or public SDK modification is needed for production ROM builds.

Add the package from your device or common tree:

```makefile
PRODUCT_PACKAGES += \
    PhhIms
```

`PhhImsOverlay` is pulled in by the app module's `required` entry.

## Device tree integration

The exact paths differ per tree, but a Samsung Exynos device usually needs the following pieces.

### Packages

```makefile
# IMS over Wi-Fi data service and network qualification service.
# These are also useful for VoLTE-only bring-up because the telephony
# framework still expects the WLAN data/network service hooks to exist.
PRODUCT_PACKAGES += \
    Iwlan \
    QualifiedNetworksService \
    PhhIms
```

### Debug availability overrides

For bring-up, these properties are useful when carrier config defaults would otherwise hide IMS capability:

```makefile
PRODUCT_PROPERTY_OVERRIDES += \
    persist.dbg.volte_avail_ovr=1 \
    persist.dbg.wfc_avail_ovr=1 \
    persist.dbg.allow_ims_off=1
```

Do not treat these as a replacement for correct carrier config. They are bring-up helpers.

### Framework overlay

Example: `overlay/frameworks/base/core/res/res/values/config.xml`

```xml
<resources>
    <bool name="config_carrier_volte_available">true</bool>
    <bool name="config_device_volte_available">true</bool>
    <bool name="config_device_vt_available">true</bool>

    <string name="config_wlan_data_service_package">com.google.android.iwlan</string>
    <string name="config_wlan_network_service_package">com.google.android.iwlan</string>
    <string name="config_qualified_networks_service_package">com.android.telephony.qns</string>
</resources>
```

### Telephony overlay

Example: `overlay/packages/services/Telephony/res/values/config.xml`

```xml
<resources>
    <string name="config_ims_mmtel_package" translatable="false">me.phh.ims</string>
</resources>
```

### Privapp permissions

Example target path:

```makefile
PRODUCT_COPY_FILES += \
    $(COMMON_PATH)/privapp-permissions-me.phh.ims.xml:$(TARGET_COPY_OUT_SYSTEM)/etc/permissions/privapp-permissions-me.phh.ims.xml
```

Example file:

```xml
<?xml version="1.0" encoding="utf-8"?>
<permissions>
    <privapp-permissions package="me.phh.ims">
        <permission name="android.permission.READ_PRIVILEGED_PHONE_STATE"/>
        <permission name="android.permission.MODIFY_PHONE_STATE"/>
    </privapp-permissions>
</permissions>
```

### Carrier config overlay

A real device tree should also carry a carrier config overlay for the tested carrier/device combination. The debug properties above may make the UI expose toggles, but stable behavior should come from proper CarrierConfig values.

Useful areas to check:

- `carrier_volte_available_bool`
- `editable_enhanced_4g_lte_bool`
- `carrier_wfc_ims_available_bool`
- `editable_wfc_mode_bool`
- IMS SMS availability
- default WFC mode and roaming behavior

### Vendor IMS properties / sepolicy

Some Samsung RIL components set or read `vendor.ril.ims.*` properties. If your device tree needs this, add a vendor property type and allow the Samsung radio service to set it.

Example:

```te
# sepolicy/vendor/property.te
vendor_internal_prop(vendor_ims_prop)
```

```text
# sepolicy/vendor/property_contexts
vendor.ril.ims.                u:object_r:vendor_ims_prop:s0
```

```te
# sepolicy/vendor/sehradiomanager.te
allow sehradiomanager vendor_ims_prop:property_service set;
```

## Samsung audio notes

Audio is not solved only inside this app. Samsung HALs often special-case cellular calls and may route capture/playback through modem/baseband paths instead of normal userspace audio paths.

### Exynos850 / A21s

The original A21s bring-up required a binary patch for `libaudioproxy.so`. The HAL only armed the microphone mixer path when an internal Samsung `proxy_mode` was in a specific range. Software IMS calls missed that range, so `AudioRecord` opened but returned silence.

The documented fix is a 2-byte NOP patch in `proxy_open_capture_stream`, described in [`RE/README.md`](RE/README.md).

### Exynos9810 / S9 family

The Exynos9810 LineageOS 23.x bring-up uses a ROM-side audio HAL change guarded by an `EXYNOS9810_CALLVOL_FIX` Soong flag. That fix maps Android voice-call volume to the Samsung mixer control `Rcv Digital Gain`, so the in-call earpiece volume follows the Android call volume slider.

This is not part of this app directly, but without the matching ROM-side audio fixes, the SIP/IMS stack may register and place calls while audio behavior still looks broken.

### Telecom audio mode

Some Samsung HALs treat `MODE_IN_CALL` as a modem-call path. For a userspace IMS stack, `MODE_IN_COMMUNICATION` may be required so `AudioRecord` stays on the real microphone ADC path instead of a baseband uplink PCM.

If calls connect but the microphone is silent, check the HAL routing first before assuming SIP/RTP is broken.

## Useful debug commands

```sh
adb shell dumpsys ims
adb shell dumpsys telephony.registry
adb shell dumpsys carrier_config
adb shell dumpsys connectivity
adb shell dumpsys package me.phh.ims
```

For logs:

```sh
adb logcat -b all -v threadtime | grep -iE \
  'PHH|PhhIms|SipHandler|MmTel|Ims|Iwlan|Qns|P-CSCF|REGISTER|401|INVITE|PRACK|ACK|BYE|CANCEL|RTP|SMS'
```

Useful UI check:

```text
*#*#4636#*#*
```

Check whether Android says IMS is registered and whether voice/SMS over IMS are available.

## Debugging common failure modes

### LTE call drops to 2G / EDGE

Usually means Android did not have an active IMS registration when the call started, so it used circuit-switched fallback.

Check:

- did `getVolteNetwork()` receive a valid IMS network?
- did `LinkProperties` contain P-CSCF addresses?
- did DNS fallback produce a P-CSCF?
- did the initial SIP REGISTER receive the expected `401 Unauthorized` challenge?
- did the second REGISTER use the correct realm from `WWW-Authenticate`?
- did reconnect retry trigger after failures?

### IMS registration freezes after VoWiFi/VoLTE switching

Usually means the app or framework still believes an old IMS access/network is valid.

Check:

- IWLAN/LTE registration tech reported to `ImsRegistrationImplBase`
- whether a call is active or pending while access changes
- stale `NetworkCallback` state
- reconnect/re-request of the IMS bearer after access becomes unsuitable

### Incoming call UI appears, but remote keeps ringing

Usually means Android was notified before SIP dialog state was fully usable, or accept handling did not send/complete the expected SIP response path.

Check:

- `INVITE` parsing
- `Call-ID`, tags, CSeq, Contact, Record-Route/Route handling
- whether `currentCall` exists before `notifyIncomingCall()`
- whether accept sends `200 OK`
- whether ACK is received and matched
- PRACK/100rel handling; do not wait for a PRACK that was never negotiated

### Reject/decline does not reach the caller

Check that reject sends a SIP reject response such as busy/reject while the call is still an incoming dialog. Closing only the Android call session is not enough; the remote network must receive a SIP response.

### Caller shown as unknown in call history

The incoming call profile must carry usable caller identity extras from the SIP `From`/P-Asserted-Identity information:

- `ImsCallProfile.EXTRA_OI`
- `ImsCallProfile.EXTRA_CNA`
- `ImsCallProfile.EXTRA_DISPLAY_TEXT`
- presentation flags such as `EXTRA_OIR` / `EXTRA_CNAP`

### Outgoing call has no ringback/progress

Check provisional SIP responses:

- `180 Ringing`
- `183 Session Progress`

Android should be notified with call-session progress before final answer, otherwise the remote side may ring while the local UI/audio state feels wrong.

### Audio is one-way or silent

Separate SIP success from audio routing. If SIP says the call is established but audio is broken, check:

- Android audio mode used by Telecom
- Samsung HAL route/mixer state
- receiver/earpiece gain mixer controls
- RTP socket lifecycle
- AMR/AMR-WB codec negotiation
- whether cleanup from a previous call left stale media threads or sockets

## P-CSCF fallback

If the RIL does not report P-CSCF addresses via `LinkProperties`, the app attempts standard 3GPP DNS discovery:

```text
ims.mnc<MNC>.mcc<MCC>.3gppnetwork.org
```

A last-resort manual override is available:

```sh
adb shell setprop persist.ims.pcscf_fallback <ip-address>
```

## Enabling VoLTE toggle state

On some builds it helps to force the enhanced 4G setting once:

```sh
adb shell settings put global enhanced_4g_mode_enabled 1
```

On fresh installs this usually defaults to enabled when overlays/carrier config expose VoLTE correctly.

## Building with Gradle

The public SDK stubs do not expose all internal IMS APIs used here. For development-only Gradle builds, you need a full `android.jar` from an AOSP/LineageOS build in `app/libs/android.jar`, and you may need to remove duplicate public IMS stubs from the platform SDK jar.

For ROM integration, use the in-tree Soong build instead.

## Notes

- The app has no launcher icon and does not appear in the app drawer.
- The app must be privileged and platform-signed.
- Carrier provisioning still matters. A carrier that does not provision IMS for the SIM/device combination may never register.
- Keep VoLTE, VoWiFi, SMS, and audio changes in separate commits while rebasing; it makes regressions much easier to isolate.
- For Samsung bring-up, always test: registration, outgoing call, incoming accept, incoming reject, SMS, VoWiFi-only, VoLTE-only, and VoWiFi→VoLTE transitions.

## License

GPL-2.0, following the upstream project.
