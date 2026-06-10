/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { createReadStream } from 'fs';
import { resolve } from 'path';

export default defineConfig({
  plugins: [
    react(),
    {
      name: 'serve-geojson',
      configureServer(server) {
        return () => {
          server.middlewares.use('/geojson', (req, res, next) => {
            // Serve GeoJSON files from public/geojson/
            const filePath = resolve(__dirname, `public/geojson${req.url}`);
            try {
              const stream = createReadStream(filePath);
              res.setHeader('Content-Type', 'application/json');
              stream.pipe(res);
              stream.on('error', () => next());
            } catch {
              next();
            }
          });
        };
      },
    },
  ],
  base: '/',
  build: {
    outDir: 'dist',
    sourcemap: true,
    target: ['es2020', 'safari14'],
    rollupOptions: {
      output: {
        manualChunks: {
          monaco: ['@monaco-editor/react'],
          charts: ['echarts', 'echarts-for-react'],
          grid: ['ag-grid-react', 'ag-grid-community'],
          antd: ['antd', '@ant-design/icons'],
        },
      },
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/query.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/storage.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      // /storage/{name}.json, /storage/{name}/enable/...
      '/storage': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/profiles': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      // Cluster page endpoints (DrillRoot)
      '/cluster.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/state': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/queriesCount': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/gracePeriod': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/portNum': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/gracefulShutdown': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/quiescent': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/shutdown': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      // Status / Options / Credentials JSON
      '/status.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/options.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/internal_options.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/credentials.json': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/credentials': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
      '/geojson': {
        target: 'http://localhost:8047',
        changeOrigin: true,
      },
    },
  },
});
