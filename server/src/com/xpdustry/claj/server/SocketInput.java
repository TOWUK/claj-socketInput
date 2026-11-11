/**
 * This file is part of CLaJ. The system that allows you to play with your friends,
 * just by creating a room, copying the link and sending it to your friends.
 * Copyright (c) 2025-2026  Xpdustry
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.xpdustry.claj.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import arc.ApplicationListener;
import arc.util.Log;
import arc.util.Threads;

/**
 * Mindustry-style socket console, hardcoded to {@code 127.0.0.1:6860}.
 * Connect with: {@code nc -N 127.0.0.1 6860}
 * Commands are executed exactly like in the local console, and every line
 * printed to the server console is mirrored to the connected client.
 */
public class SocketInput implements ApplicationListener {

  private static final String ADDRESS = "127.0.0.1"; // localhost only, never exposed to the network
  private static final int PORT = 6860;
  /** Max command length in bytes, longer lines are truncated to prevent memory exhaustion. */
  private static final int MAX_LINE = 512;

  private final PrintStream original = System.out;
  private volatile ServerSocket server;
  private volatile Socket client;
  private volatile OutputStream output;
  private volatile boolean disposed;

  @Override
  public void init() {
    // Mirror all console output (Log, stack traces...) to the connected client
    System.setOut(new PrintStream(new OutputStream() {
      @Override public void write(int b) throws IOException { write(new byte[] {(byte)b}, 0, 1); }

      @Override public void write(byte[] b, int off, int len) throws IOException {
        original.write(b, off, len);
        OutputStream out = output;
        if (out != null) {
          try { out.write(b, off, len); out.flush(); } // flush on every write: instant delivery
          catch (IOException ignored) { // client is gone, its reader thread will clean up
          }
        }
      }

      @Override public void flush() throws IOException {
        original.flush();
        OutputStream out = output;
        if (out != null) { try { out.flush(); } catch (IOException ignored) {} }
      }
    }, true, StandardCharsets.UTF_8));

    Log.info("Socket input listening on @. Connect with: nc -N @", ADDRESS + ":" + PORT, ADDRESS + " " + PORT);
    Threads.daemon("Socket Input", () -> {
      try (ServerSocket s = new ServerSocket()) {
        s.setReuseAddress(true);
        s.bind(new InetSocketAddress(ADDRESS, PORT));
        server = s;
        while (!disposed) handle(s.accept());
      } catch (Throwable e) {
        if (!disposed) Log.err("Socket input failed", e);
      }
    });
  }

  /** Handles a single client, one at a time (like Mindustry's socket input). */
  private void handle(Socket socket) {
    try (Socket s = socket) {
      if (disposed) return;
      s.setTcpNoDelay(true); // no Nagle's algorithm: instant I/O
      client = s;
      output = s.getOutputStream();
      String line;
      while ((line = readLine(s.getInputStream())) != null) {
        if (line.isEmpty()) continue;
        try {
          ClajVars.logToFile("> " + line); // keep commands in the log file, same as the local console
          ClajVars.control.handleCommand(line); // same behavior and suggestions as the local console
        } catch (Throwable e) { Log.err(e); }
      }
    } catch (IOException ignored) { // connection dropped
    } finally {
      client = null;
      output = null;
    }
  }

  /** Reads one UTF-8 line capped at MAX_LINE; on EOF returns the remaining buffer (nc -N half-close) or null. */
  private static String readLine(InputStream in) throws IOException {
    byte[] buf = new byte[MAX_LINE];
    int len = 0, b;
    while ((b = in.read()) != '\n') {
      if (b == -1) return len > 0 ? new String(buf, 0, len, StandardCharsets.UTF_8) : null;
      if (b != '\r' && len < MAX_LINE) buf[len++] = (byte)b;
    }
    return new String(buf, 0, len, StandardCharsets.UTF_8);
  }

  @Override
  public void dispose() {
    disposed = true;
    output = null;
    try { server.close(); } catch (Exception ignored) {}
    try { client.close(); } catch (Exception ignored) {}
    System.setOut(original);
  }
}
