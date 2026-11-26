import SwiftUI

@main
struct PuzzleDojoApp: App {
    var body: some Scene {
        WindowGroup {
            // The page lays itself out around the notch and home indicator
            // (CSS safe-area insets), so the web view runs edge to edge.
            GameView()
                .ignoresSafeArea()
                .background(Color("LaunchBackground").ignoresSafeArea())
        }
    }
}
