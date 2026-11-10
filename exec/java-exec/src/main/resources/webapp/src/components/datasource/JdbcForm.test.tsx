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
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import JdbcForm from './JdbcForm';
import { testConnection } from '../../api/storage';

vi.mock('../../api/storage', () => ({
  testConnection: vi.fn().mockResolvedValue({ success: true, message: 'ok' }),
}));

// A config as Drill returns it when the credentials were stored through a credentials
// provider, which is what the newer plugin forms write.
const withProvider = {
  type: 'jdbc',
  driver: 'com.mysql.cj.jdbc.Driver',
  url: 'jdbc:mysql://localhost:3306',
  credentialsProvider: {
    credentialsProviderType: 'PlainCredentialsProvider',
    credentials: { username: 'user1', password: 'pass1' },
    userCredentials: {},
  },
};

describe('JdbcForm credentials', () => {
  it('shows credentials that are stored in a credentials provider', () => {
    render(<JdbcForm config={withProvider} onChange={vi.fn()} />);
    expect(screen.getByPlaceholderText('username')).toHaveValue('user1');
    expect(screen.getByPlaceholderText('password')).toHaveValue('pass1');
  });

  it('writes a changed credential back into the provider, leaving no stale top-level copy', () => {
    const onChange = vi.fn();
    render(<JdbcForm config={withProvider} onChange={onChange} />);
    fireEvent.change(screen.getByPlaceholderText('username'), { target: { value: 'user2' } });

    const next = onChange.mock.calls[0][0];
    expect(next.credentialsProvider.credentials).toEqual({ username: 'user2', password: 'pass1' });
    expect(next.username).toBeUndefined();
  });

  it('still reads credentials written at the top level', () => {
    render(<JdbcForm config={{ type: 'jdbc', username: 'store', password: 'secret' }} onChange={vi.fn()} />);
    expect(screen.getByPlaceholderText('username')).toHaveValue('store');
  });
});

describe('JdbcForm test connection', () => {
  // The config the form sends to the server must be the plugin as configured. Rebuilding
  // it from the form's own fields dropped everything the form does not model, so a test
  // could pass or fail for reasons unrelated to the saved data source.
  it('tests with the source parameters that are stored on the plugin', async () => {
    const config = {
      type: 'jdbc',
      driver: 'com.mysql.cj.jdbc.Driver',
      url: 'jdbc:mysql://localhost:8889/',
      username: 'store',
      password: 'store',
      sourceParameters: { maximumPoolSize: '3' },
      authMode: 'SHARED_USER',
    };
    render(<JdbcForm config={config} onChange={vi.fn()} />);
    fireEvent.click(screen.getByText('Test Connection'));

    await waitFor(() => expect(testConnection).toHaveBeenCalled());
    const sent = vi.mocked(testConnection).mock.calls[0][1] as Record<string, unknown>;
    expect(sent.sourceParameters).toEqual({ maximumPoolSize: '3' });
    expect(sent.authMode).toBe('SHARED_USER');
    expect(sent.url).toBe('jdbc:mysql://localhost:8889/');
  });

  it('sends a provider credential inside the provider, with no stale top-level copy', async () => {
    render(<JdbcForm config={withProvider} onChange={vi.fn()} />);
    fireEvent.click(screen.getByText('Test Connection'));

    await waitFor(() => expect(testConnection).toHaveBeenCalled());
    const sent = vi.mocked(testConnection).mock.calls.at(-1)![1] as Record<string, unknown>;
    const provider = sent.credentialsProvider as { credentials: Record<string, string> };
    expect(provider.credentials).toEqual({ username: 'user1', password: 'pass1' });
    expect(sent.username).toBeUndefined();
    expect(sent.password).toBeUndefined();
  });
});
