#!/usr/bin/env python3
"""Protocol-level regression tests for the deterministic fake LSP server."""

import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))
from fake_lsp_server import FakeLspServer


def framed(message):
    payload = json.dumps(message).encode("utf-8")
    return f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii") + payload


def messages(payload):
    reader = io.BytesIO(payload)
    result = []
    while reader.tell() < len(payload):
        header = reader.readline()
        length = int(header.partition(b":")[2].strip())
        assert reader.readline() == b"\r\n"
        result.append(json.loads(reader.read(length)))
    return result


class FakeLspServerTests(unittest.TestCase):
    def run_exchange(self, *incoming):
        reader = io.BytesIO(b"".join(framed(item) for item in incoming))
        writer = io.BytesIO()
        server = FakeLspServer(reader, writer)
        self.assertEqual(server.run(), 0)
        return messages(writer.getvalue())

    def test_initialize_open_and_shutdown_use_real_json_rpc_framing(self):
        uri = "file:///tmp/project/main.c"
        output = self.run_exchange(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            {"jsonrpc": "2.0", "method": "initialized", "params": {}},
            {
                "jsonrpc": "2.0",
                "method": "textDocument/didOpen",
                "params": {
                    "textDocument": {
                        "uri": uri,
                        "version": 1,
                        "text": "int alpha = 1;\n",
                    }
                },
            },
            {"jsonrpc": "2.0", "id": 2, "method": "shutdown"},
            {"jsonrpc": "2.0", "method": "exit"},
        )
        self.assertEqual(output[0]["result"]["serverInfo"]["name"], "emaxx-fake-lsp")
        self.assertEqual(output[1]["method"], "textDocument/publishDiagnostics")
        self.assertEqual(output[1]["params"]["diagnostics"][0]["source"], "fake-lsp")
        self.assertIsNone(output[2]["result"])

    def test_language_features_follow_protocol_and_document_state(self):
        uri = "file:///tmp/project/main.c"
        open_message = {
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "version": 1,
                    "text": "int alpha = 1;\nreturn alpha;\n",
                }
            },
        }
        output = self.run_exchange(
            open_message,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "textDocument/completion",
                "params": {"textDocument": {"uri": uri}},
            },
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "textDocument/hover",
                "params": {"textDocument": {"uri": uri}},
            },
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "textDocument/definition",
                "params": {"textDocument": {"uri": uri}},
            },
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "textDocument/rename",
                "params": {"textDocument": {"uri": uri}, "newName": "renamed"},
            },
        )
        self.assertEqual(output[1]["result"]["items"][0]["label"], "alphaValue")
        self.assertIn("alpha", output[2]["result"]["contents"]["value"])
        self.assertEqual(output[3]["result"]["range"]["start"], {"line": 0, "character": 4})
        edits = output[4]["result"]["changes"][uri]
        self.assertEqual(len(edits), 2)
        self.assertTrue(all(edit["newText"] == "renamed" for edit in edits))


if __name__ == "__main__":
    unittest.main()
