#!/usr/bin/env python3
"""Small deterministic Language Server Protocol fixture.

The server intentionally implements only the protocol surface used by the
deterministic terminal journeys.  It is an ordinary stdio JSON-RPC peer: the
editors receive no prerecorded responses through production code and the
fixture contains no editor-specific branches.
"""

from __future__ import annotations

import json
import sys
from typing import BinaryIO


class FakeLspServer:
    """Serve a deterministic subset of LSP over binary input/output streams."""

    def __init__(self, reader: BinaryIO, writer: BinaryIO) -> None:
        self.reader = reader
        self.writer = writer
        self.documents: dict[str, tuple[int, str]] = {}
        self.shutdown_requested = False

    def read_message(self) -> dict | None:
        """Read one Content-Length framed JSON-RPC message."""
        content_length = None
        while True:
            line = self.reader.readline()
            if not line:
                return None
            if line in (b"\r\n", b"\n"):
                break
            name, separator, value = line.partition(b":")
            if separator and name.lower() == b"content-length":
                content_length = int(value.strip())
        if content_length is None:
            raise ValueError("JSON-RPC message omitted Content-Length")
        payload = self.reader.read(content_length)
        if len(payload) != content_length:
            raise EOFError("truncated JSON-RPC payload")
        return json.loads(payload.decode("utf-8"))

    def send(self, message: dict) -> None:
        """Write one compact Content-Length framed JSON-RPC message."""
        payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
        self.writer.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii"))
        self.writer.write(payload)
        self.writer.flush()

    def respond(self, request: dict, result=None, error: dict | None = None) -> None:
        response = {"jsonrpc": "2.0", "id": request["id"]}
        if error is None:
            response["result"] = result
        else:
            response["error"] = error
        self.send(response)

    def notify(self, method: str, params: dict) -> None:
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    @staticmethod
    def word_range(text: str, word: str = "alpha") -> dict:
        """Return the first WORD range, with a stable fallback at buffer start."""
        offset = text.find(word)
        if offset < 0:
            return {
                "start": {"line": 0, "character": 0},
                "end": {"line": 0, "character": 1},
            }
        before = text[:offset]
        line = before.count("\n")
        character = len(before.rsplit("\n", 1)[-1])
        return {
            "start": {"line": line, "character": character},
            "end": {"line": line, "character": character + len(word)},
        }

    def publish_diagnostics(self, uri: str) -> None:
        version, text = self.documents[uri]
        diagnostics = []
        if "alpha" in text:
            diagnostics.append(
                {
                    "range": self.word_range(text),
                    "severity": 2,
                    "source": "fake-lsp",
                    "code": "fixture-warning",
                    "message": "alpha is the deterministic fixture warning",
                }
            )
        self.notify(
            "textDocument/publishDiagnostics",
            {"uri": uri, "version": version, "diagnostics": diagnostics},
        )

    def document(self, params: dict) -> tuple[str, int, str]:
        uri = params["textDocument"]["uri"]
        version, text = self.documents.get(uri, (0, ""))
        return uri, version, text

    def handle_request(self, request: dict) -> None:
        method = request["method"]
        params = request.get("params") or {}
        if method == "initialize":
            self.respond(
                request,
                {
                    "capabilities": {
                        "textDocumentSync": {"openClose": True, "change": 1},
                        "completionProvider": {"triggerCharacters": ["."]},
                        "hoverProvider": True,
                        "renameProvider": True,
                        "definitionProvider": True,
                    },
                    "serverInfo": {"name": "emaxx-fake-lsp", "version": "1.0"},
                },
            )
        elif method == "shutdown":
            self.shutdown_requested = True
            self.respond(request, None)
        elif method == "textDocument/completion":
            self.respond(
                request,
                {
                    "isIncomplete": False,
                    "items": [
                        {
                            "label": "alphaValue",
                            "kind": 6,
                            "detail": "deterministic integer completion",
                            "insertText": "alphaValue",
                        }
                    ],
                },
            )
        elif method == "textDocument/hover":
            self.respond(
                request,
                {
                    "contents": {
                        "kind": "plaintext",
                        "value": "Fake hover: alpha is an integer.",
                    }
                },
            )
        elif method == "textDocument/definition":
            uri, _version, text = self.document(params)
            self.respond(request, {"uri": uri, "range": self.word_range(text)})
        elif method == "textDocument/rename":
            uri, _version, text = self.document(params)
            new_name = params["newName"]
            edits = []
            cursor = 0
            while True:
                offset = text.find("alpha", cursor)
                if offset < 0:
                    break
                before = text[:offset]
                line = before.count("\n")
                character = len(before.rsplit("\n", 1)[-1])
                edits.append(
                    {
                        "range": {
                            "start": {"line": line, "character": character},
                            "end": {"line": line, "character": character + 5},
                        },
                        "newText": new_name,
                    }
                )
                cursor = offset + 5
            self.respond(request, {"changes": {uri: edits}})
        else:
            self.respond(
                request,
                error={"code": -32601, "message": f"Method not found: {method}"},
            )

    def handle_notification(self, notification: dict) -> bool:
        method = notification["method"]
        params = notification.get("params") or {}
        if method == "exit":
            return False
        if method == "textDocument/didOpen":
            document = params["textDocument"]
            uri = document["uri"]
            self.documents[uri] = (document.get("version", 0), document["text"])
            self.publish_diagnostics(uri)
        elif method == "textDocument/didChange":
            uri, previous_version, previous_text = self.document(params)
            changes = params["contentChanges"]
            text = changes[-1].get("text", previous_text) if changes else previous_text
            self.documents[uri] = (
                params["textDocument"].get("version", previous_version + 1),
                text,
            )
            self.publish_diagnostics(uri)
        elif method == "textDocument/didClose":
            uri = params["textDocument"]["uri"]
            self.documents.pop(uri, None)
            self.notify(
                "textDocument/publishDiagnostics",
                {"uri": uri, "diagnostics": []},
            )
        return True

    def run(self) -> int:
        while True:
            message = self.read_message()
            if message is None:
                return 0
            if "id" in message and "method" in message:
                self.handle_request(message)
            elif "method" in message and not self.handle_notification(message):
                return 0 if self.shutdown_requested else 1


def main() -> int:
    return FakeLspServer(sys.stdin.buffer, sys.stdout.buffer).run()


if __name__ == "__main__":
    raise SystemExit(main())
