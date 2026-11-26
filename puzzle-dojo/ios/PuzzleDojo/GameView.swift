import SwiftUI
import UIKit
import WebKit

/// Shows the bundled game page (`Web/index.html`, built by `build.js`).
struct GameView: UIViewRepresentable {
    func makeCoordinator() -> GameBridge { GameBridge() }
    func makeUIView(context: Context) -> WKWebView { context.coordinator.makeWebView() }
    func updateUIView(_ webView: WKWebView, context: Context) {}
}

/// Connects the page to the device. The page posts `{type: ...}` messages on
/// the "dojo" channel:
///   save    progress, kept in UserDefaults and handed back on the next launch
///   haptic  a tap on the Taptic Engine
///   theme   the in-app light/dark choice, so the status bar follows it
@MainActor
final class GameBridge: NSObject, WKScriptMessageHandler, WKNavigationDelegate {
    private static let saveKey = "puzzle-dojo.v1"
    private static let channel = "dojo"

    private weak var webView: WKWebView?
    private var theme = "system"

    func makeWebView() -> WKWebView {
        let content = WKUserContentController()
        content.add(self, name: Self.channel)
        content.addUserScript(bootScript())
        let configuration = WKWebViewConfiguration()
        configuration.userContentController = content

        let webView = WKWebView(frame: .zero, configuration: configuration)
        let background = UIColor(named: "LaunchBackground") ?? .systemBackground
        webView.navigationDelegate = self
        webView.isOpaque = false
        webView.backgroundColor = background
        webView.scrollView.backgroundColor = background
        webView.scrollView.contentInsetAdjustmentBehavior = .never
        webView.scrollView.alwaysBounceHorizontal = false
        webView.scrollView.showsHorizontalScrollIndicator = false
        webView.allowsLinkPreview = false
        webView.allowsBackForwardNavigationGestures = false
        #if DEBUG
        webView.isInspectable = true
        #endif
        self.webView = webView
        NotificationCenter.default.addObserver(self, selector: #selector(appDidEnterBackground),
                                               name: UIApplication.didEnterBackgroundNotification, object: nil)

        if let page = Bundle.main.url(forResource: "index", withExtension: "html") {
            webView.loadFileURL(page, allowingReadAccessTo: page.deletingLastPathComponent())
        }
        return webView
    }

    /// Runs before the page's own scripts: tells the page it is inside the app
    /// and gives it the progress saved on this device.
    private func bootScript() -> WKUserScript {
        let saved = UserDefaults.standard.string(forKey: Self.saveKey)
        let source = "window.__dojoNative = { saved: \(Self.javaScriptLiteral(saved)) };"
        return WKUserScript(source: source, injectionTime: .atDocumentStart, forMainFrameOnly: true)
    }

    private static func javaScriptLiteral(_ text: String?) -> String {
        guard let text,
              let data = try? JSONSerialization.data(withJSONObject: [text]),
              let array = String(data: data, encoding: .utf8) else { return "null" }
        // ["..."] without the brackets is a quoted, escaped string literal.
        return String(array.dropFirst().dropLast())
    }

    /// iOS may end the app while it is in the background, so ask the page to
    /// save right away (it saves on `pagehide`), timer included.
    @objc private func appDidEnterBackground() {
        webView?.evaluateJavaScript("window.dispatchEvent(new Event('pagehide'))", completionHandler: nil)
    }

    // MARK: Messages from the page

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard message.name == Self.channel,
              let body = message.body as? [String: Any],
              let type = body["type"] as? String else { return }
        switch type {
        case "save":
            if let data = body["data"] as? String {
                UserDefaults.standard.set(data, forKey: Self.saveKey)
            }
        case "haptic":
            playHaptic(body["style"] as? String ?? "light")
        case "theme":
            theme = body["value"] as? String ?? "system"
            applyTheme()
        default:
            break
        }
    }

    private func playHaptic(_ style: String) {
        switch style {
        case "success":
            UINotificationFeedbackGenerator().notificationOccurred(.success)
        case "warning":
            UINotificationFeedbackGenerator().notificationOccurred(.warning)
        default:
            UIImpactFeedbackGenerator(style: .light).impactOccurred()
        }
    }

    private func applyTheme() {
        let style: UIUserInterfaceStyle
        switch theme {
        case "light": style = .light
        case "dark": style = .dark
        default: style = .unspecified
        }
        webView?.window?.overrideUserInterfaceStyle = style
    }

    // MARK: Navigation

    func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction) async -> WKNavigationActionPolicy {
        // The game is one bundled file. Anything else opens outside the app.
        guard let url = navigationAction.request.url, !url.isFileURL else { return .allow }
        if url.scheme == "https" { _ = await UIApplication.shared.open(url) }
        return .cancel
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        applyTheme()
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("-selftest") { runSelfTest(webView) }
        #endif
    }

    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
        // iOS can drop the page's process under memory pressure. Reload it with
        // the latest save rather than the one from launch.
        let content = webView.configuration.userContentController
        content.removeAllUserScripts()
        content.addUserScript(bootScript())
        webView.reload()
    }

    // MARK: Self-test (debug builds, `-selftest` launch argument)

    #if DEBUG
    /// Starts a board through the page's own buttons and writes what happened
    /// to Documents/selftest.json, so a build script can check that the page
    /// loaded, the bridge works and progress survives a relaunch.
    /// `savedElapsed` is the timer of the game found at launch, in ms.
    private func runSelfTest(_ webView: WKWebView) {
        let probe = """
        (function () {
          var all = function (q) { return Array.prototype.slice.call(document.querySelectorAll(q)); };
          var play = all('button').filter(function (b) { return /Play a 6/.test(b.textContent); })[0];
          var resumed = all('.eyebrow').some(function (e) { return e.textContent === 'Continue'; });
          var heading = document.querySelector('h1');
          var savedElapsed = -1;
          try { savedElapsed = JSON.parse(window.__dojoNative.saved).game.elapsed; } catch (e) {}
          if (play) play.click();
          return JSON.stringify({
            bridge: !!window.__dojoNative,
            savedElapsed: savedElapsed,
            heading: heading ? heading.textContent : null,
            resumed: resumed,
            started: !!play
          });
        })()
        """
        Task {
            try? await Task.sleep(nanoseconds: 1_000_000_000)
            let first = try? await webView.evaluateJavaScript(probe)
            try? await Task.sleep(nanoseconds: 2_500_000_000)
            let cells = try? await webView.evaluateJavaScript("document.querySelectorAll('.board .cell').length")
            let saved = UserDefaults.standard.string(forKey: Self.saveKey) ?? ""
            let report: [String: Any] = [
                "page": (first as? String) ?? "no result",
                "cells": (cells as? Int) ?? -1,
                "savedBytes": saved.utf8.count,
                "savedHasGame": saved.contains("\"game\":{"),
            ]
            let folder = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            if let data = try? JSONSerialization.data(withJSONObject: report, options: [.sortedKeys]) {
                try? data.write(to: folder.appendingPathComponent("selftest.json"))
            }
        }
    }
    #endif
}
