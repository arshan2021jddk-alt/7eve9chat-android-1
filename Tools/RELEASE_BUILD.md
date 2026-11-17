# Android Release build preparation

Reviewed base: `841c085`. No application Java, permissions, resources, package IDs,
backend or Web code was changed by this readiness review.

## Current verification and blockers

- Seven saved-gift ID / partial appearance checks compile extracted production
  methods using Java 8 language/API compatibility and pass. This is **not** full
  Android compilation or device testing.
- The changed fields and constants were checked against `TL_stars`, `TLRPC` and
  `MessagesController`. Gift routes, payloads and Premium errors match deployed
  backend `3707d49`; authenticated device round trips remain unverified.
- GitHub Actions run `44148075733` did not start because account payments failed
  or the spending limit must be increased. Keep the repository private.
- Local Windows has Gradle 8.14 and JDK 21/22, but SDK Platform 36, NDK
  `27.2.12479018`, CMake `3.22.1`, and the SDK command-line executable are absent.
  The `cmdline-tools/20.0` directory contains only installer metadata. The official
  Windows tools archive download returned HTTP 404 from this machine. WSL has no
  Java, Gradle, Docker or SDK manager installed. Release compilation could not start.
- No APK was generated. No new full-compilation, R8, install or device-test success
  is claimed. The previous successful Actions build predates `841c085` and built
  `AfatciDebug`, not Release.

## Reproduce on an authorized build host

1. Clone this private repository with its pinned submodules:

   ```sh
   git clone --recurse-submodules https://github.com/arshan2021jddk-alt/7eve9chat-android-1.git
   cd 7eve9chat-android-1
   git submodule update --init --recursive
   git rev-parse HEAD
   ```

2. Use JDK 17 (the existing Dockerfile uses JDK 17) and Gradle 8.13, or the checked-in
   Gradle wrapper. Install [official Android SDK tools](https://developer.android.com/studio)
   and the exact packages required by the existing Gradle files:

   ```sh
   sdkmanager --licenses
   sdkmanager "platform-tools" "platforms;android-36" "build-tools;36.0.0" "ndk;27.2.12479018" "cmake;3.22.1"
   ```

   Set `ANDROID_HOME` to that SDK root. On Windows use an installed compatible
   `gradle.bat`; this repository has a Unix `gradlew`, not `gradlew.bat`.

3. Provision the **existing authorized production signing certificate** at
   `TMessagesProj/config/release.keystore` through the established private secret
   mechanism. Configure `RELEASE_KEY_ALIAS`, `RELEASE_STORE_PASSWORD`, and
   `RELEASE_KEY_PASSWORD` in protected user Gradle properties outside the repository.
   Do not print or commit those values or substitute the repository's default key.
   Signing secrets currently available only inside Actions must stay there.

4. Choose an approved `APP_VERSION_CODE` base greater than the base used by the
   last distributed APK. Existing CI derives its base from `7089 + GITHUB_RUN_NUMBER`;
   the final Afat version code is `base * 10 + 9`. Do not blindly use the older
   checked-in default for an update. Run focused checks and one clean Release build:

   ```sh
   python Tools/check_gift_appearance.py
   ./gradlew --no-daemon --max-workers=2 clean :TMessagesProj_App:assembleAfatRelease -PAPP_VERSION_CODE=APPROVED_BASE_CODE --console=plain
   ```

   Replace `APPROVED_BASE_CODE` with the approved integer before running.
   Use the existing Dockerfile if building in Docker; its default command builds
   additional variants, so the targeted Gradle command above avoids unnecessary builds.

5. Expected Gradle artifact, **only after a successful build**:

   ```text
   TMessagesProj_App/build/outputs/apk/afat/release/app.apk
   ```

   Verify its release certificate with the SDK's `apksigner verify --print-certs`,
   and inspect its package/version with `aapt dump badging` before distribution.
   AfatRelease uses `ir.sevennine.chat`; existing AfatciDebug uses
   `ir.sevennine.chat.beta`. Preserve those existing IDs: Release does not replace
   the beta package or migrate its private data. Do not uninstall an existing app
   as part of build verification. Device messaging/login/media checks are pending.
