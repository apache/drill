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
import { Component, ErrorInfo, ReactNode } from 'react';

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
  stack: string;
}

/**
 * Without a boundary, React 18 unmounts the whole tree on any uncaught render
 * error and the user gets a blank white page with no clue what happened.
 *
 * Deliberately plain markup: antd, the theme provider and the router are all
 * inside this boundary, so anything that renders here must not depend on them.
 */
export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, stack: '' };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Keep the console record — the stack shown below is the component tree,
    // which is the useful half when diagnosing a blank screen.
    console.error('Unhandled error in React tree:', error, info.componentStack);
    this.setState({ stack: info.componentStack ?? '' });
  }

  render() {
    const { error, stack } = this.state;
    if (!error) {
      return this.props.children;
    }

    return (
      <div style={{ padding: '2rem', fontFamily: 'system-ui, sans-serif', color: '#333' }}>
        <h1 style={{ fontSize: '1.25rem', margin: '0 0 0.5rem' }}>Something went wrong</h1>
        <p style={{ margin: '0 0 1rem' }}>
          The page failed to render. Reloading usually fixes it; if your session expired you
          will be sent to the login page.
        </p>
        <button
          type="button"
          onClick={() => window.location.reload()}
          style={{ padding: '0.4rem 1rem', marginBottom: '1.5rem', cursor: 'pointer' }}
        >
          Reload
        </button>
        <details>
          <summary style={{ cursor: 'pointer' }}>Error details</summary>
          <pre style={{ whiteSpace: 'pre-wrap', fontSize: '0.8rem', marginTop: '0.75rem' }}>
            {error.message}
            {stack}
          </pre>
        </details>
      </div>
    );
  }
}
