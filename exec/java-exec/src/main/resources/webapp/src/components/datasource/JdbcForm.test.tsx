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
import { render, screen, fireEvent } from '@testing-library/react';
import JdbcForm from './JdbcForm';

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
