#!/usr/bin/env python3
"""
Simple HTTP server to serve the schema documentation
"""

import http.server
import socketserver
import webbrowser
import os
from pathlib import Path

PORT = 56731
DIRECTORY = Path(__file__).parent

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

if __name__ == "__main__":
    os.chdir(DIRECTORY)
    
    with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
        print("🚀 Schema Documentation Server Started!")
        print(f"📁 Serving from: {DIRECTORY}")
        print(f"🌐 Access the documentation at:")
        print(f"   • Complete Documentation: http://localhost:{PORT}/complete_schema_documentation.html")
        print(f"   • Visual Mapping: http://localhost:{PORT}/docs/visual_schema_mapping.html")
        print(f"   • Design Rationale: http://localhost:{PORT}/docs/sql_to_nosql_schema_design_rationale.md")
        print(f"\n✨ Press Ctrl+C to stop the server")
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n🛑 Server stopped")
            httpd.shutdown()
