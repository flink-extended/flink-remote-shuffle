/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.rest;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/** A simple rest server implementation based on jetty. */
public class RestService {

    private static final Logger LOG = LoggerFactory.getLogger(RestService.class);

    private final Server server;

    private final ConcurrentHashMap<String, RestHandler> handlers = new ConcurrentHashMap<>();

    public RestService(String host, int port) {
        this.server = new Server(new InetSocketAddress(host, port));
        server.setHandler(new RestRequestHandler());
    }

    public void registerHandler(RestHandler handler) {
        if (handler == null) {
            LOG.error("Failed to register rest handler, handler must be not null.");
            return;
        }
        String queryPath = handler.getQueryPath();

        if (!queryPath.startsWith("/")) {
            queryPath = "/" + queryPath;
        }

        RestHandler prevHandler = handlers.putIfAbsent(queryPath, handler);
        if (prevHandler != null) {
            LOG.error("Failed to register rest handler, already exists.");
        }
    }

    public void unregisterHandler(String query) {
        handlers.remove(query);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() {
        try {
            server.stop();
        } catch (Throwable throwable) {
            LOG.error("Failed to stop rest service.", throwable);
        }
    }

    private final class RestRequestHandler extends AbstractHandler {

        @Override
        public void handle(
                String requestPath,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response) {

            try {
                if (requestPath.length() > 1 && requestPath.endsWith("/")) {
                    requestPath = requestPath.substring(0, requestPath.length() - 1);
                }
                RestHandler handler = handlers.get(requestPath);

                response.setContentType("application/json");
                response.setCharacterEncoding("UTF-8");
                PrintWriter writer = response.getWriter();

                if (handler == null) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                } else {
                    writer.print(handler.handle(baseRequest.getParameterMap()).toJson());
                    response.setStatus(HttpServletResponse.SC_OK);
                }
            } catch (Throwable throwable) {
                LOG.error("Failed to handle rest request.", throwable);
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            } finally {
                baseRequest.setHandled(true);
            }
        }
    }
}
