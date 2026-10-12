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

import arc.ApplicationListener;
import arc.util.CommandHandler;
import arc.util.Log;
import arc.util.Threads;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class SocketInput implements ApplicationListener {
    private static final int MAX_LINE = 512;
    private static final int MAX_QUEUE = 50;
    private static final int READ_TIMEOUT_MS = 10_000;

    /** Guards against re-hooking {@link System#out} if {@link #init()} is called twice. */
    private static boolean hooked = false;

    private final CommandHandler handler;
    private ServerSocket server;
    private volatile OutputStream output;
    private final PrintStream original = System.out;

    public SocketInput(CommandHandler handler) {
        this.handler = handler;
    }

    @Override
    public void init() {
        if (!ClajConfig.socketInput.get()) {
            Log.info("SocketInput is disabled.");
            return;
        }

        String address = ClajConfig.socketInputAddress.get();
        int port = ClajConfig.socketInputPort.get();

        // Restart on config change
        ClajConfig.socketInputPort.changed(p -> restart(ClajConfig.socketInputAddress.get(), p));
        ClajConfig.socketInputAddress.changed(a -> restart(a, ClajConfig.socketInputPort.get()));

        // Duplicate stdout into the socket, preserving console logs.
        // Errors are swallowed: a dead client must never crash CLaJ.
        if (!hooked) {
            hooked = true;
            System.setOut(new PrintStream(new OutputStream() {
                @Override public void write(int b) throws IOException {
                    original.write(b);
                    OutputStream o = output;
                    if (o != null) try { o.write(b); } catch (IOException ignored) {}
                }
                @Override public void write(byte[] b, int off, int len) throws IOException {
                    original.write(b, off, len);
                    OutputStream o = output;
                    if (o != null) try { o.write(b, off, len); } catch (IOException ignored) {}
                }
                @Override public void flush() throws IOException {
                    original.flush();
                    OutputStream o = output;
                    if (o != null) try { o.flush(); } catch (IOException ignored) {}
                }
            }, true));
        }

        Threads.daemon("Socket Input", () -> listen(address, port));
    }

    /** Atomically close the old socket and start a new listener thread. */
    private synchronized void restart(String address, int port) {
        ServerSocket old = server;
        server = null;
        if (old != null) try { old.close(); } catch (Exception ignored) {}
        output = null;
        Log.info("SocketInput restarting on @:@", address, port);
        Threads.daemon("Socket Input", () -> listen(address, port));
    }

    private void listen(String address, int port) {
        ServerSocket local = null;
        try {
            local = new ServerSocket();
            local.setReuseAddress(true);
            local.bind(new InetSocketAddress(InetAddress.getByName(address), port), MAX_QUEUE);

            // Guard against a racing restart that already installed its own socket.
            synchronized (this) {
                if (server != null) {
                    try { local.close(); } catch (Exception ignored) {}
                    return;
                }
                server = local;
            }

            Log.info("SocketInput listening on @:@", address, port);
        } catch (Exception e) {
            Log.err("SocketInput failed to start", e);
            try { if (local != null) local.close(); } catch (Exception ignored) {}
            return;
        }

        while (!local.isClosed()) {
            try (Socket c = local.accept()) {
                handle(c);
            } catch (SocketException e) {
                // Server stopped - normal.
            } catch (Exception e) {
                if (!local.isClosed()) Log.err("SocketInput accept", e);
            }
        }
    }

    private void handle(Socket c) {
        try {
            c.setTcpNoDelay(true);
            c.setSoTimeout(READ_TIMEOUT_MS);
            output = new BufferedOutputStream(c.getOutputStream(), 8192);

            try (InputStream in = new BufferedInputStream(c.getInputStream(), 512)) {
                String s;
                while ((s = readLine(in)) != null)
                    if (!s.isEmpty()) synchronized (handler) { handler.handleMessage(s); }
            }
            output.flush();
        } catch (SocketTimeoutException ignored) {
            // Client idle too long - normal.
        } catch (SocketException ignored) {
            // Client disconnected - normal.
        } catch (Exception e) {
            Log.err("SocketInput client", e);
        } finally {
            output = null;
        }
    }

    /** Reads a single UTF-8 line, capped at MAX_LINE bytes to prevent OOM. */
    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') return new String(buf.toByteArray(), StandardCharsets.UTF_8);
            if (b != '\r') {
                if (buf.size() >= MAX_LINE) throw new IOException("Command too long");
                buf.write(b);
            }
        }
        return buf.size() > 0 ? new String(buf.toByteArray(), StandardCharsets.UTF_8) : null;
    }

    @Override
    public void dispose() {
        output = null;
        ServerSocket old = server;
        server = null;
        try { if (old != null) old.close(); } catch (Exception ignored) {}
        if (hooked) {
            hooked = false;
            System.setOut(original);
        }
    }
}
