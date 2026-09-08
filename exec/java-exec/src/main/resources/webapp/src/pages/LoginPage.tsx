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
import { useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Alert, Button, Card, Form, Input, Typography } from 'antd';
import { LockOutlined, UserOutlined } from '@ant-design/icons';

const { Title, Text } = Typography;

/**
 * Form login page. Posts to /j_security_check (Jetty's hardcoded form action).
 * Jetty handles the redirect back to the original URL via the session
 * attribute populated by LogInLogOutResources.getLoginPage.
 *
 * On auth failure, Jetty internally dispatches POST /login to the backend
 * which returns a tiny HTML that bounces back here with ?error=1.
 */
function LoginPage() {
  const [params] = useSearchParams();
  const hasError = params.get('error') !== null;

  // Native form POST so the browser submits via the URL Jetty expects.
  // We don't use the React-managed apiClient here — Jetty's FormAuthenticator
  // intercepts the request before any application code runs.
  const errorMessage = useMemo(
    () => (hasError ? 'Invalid username or password. Please try again.' : null),
    [hasError],
  );

  return (
    <div className="login-shell">
      <Card className="login-card" bordered={false}>
        <div className="login-brand">
          <img src="/static/img/apache-drill-logo.png" alt="Apache Drill" className="login-logo" />
          <Title level={3} style={{ margin: 0 }}>Log in to Drill</Title>
          <Text type="secondary">Web Console</Text>
        </div>

        {errorMessage && (
          <Alert
            type="error"
            showIcon
            message={errorMessage}
            style={{ marginBottom: 16 }}
          />
        )}

        <Form
          name="login"
          method="POST"
          action="/j_security_check"
          autoComplete="on"
          layout="vertical"
        >
          <Form.Item label="Username" required>
            <Input
              name="j_username"
              prefix={<UserOutlined />}
              placeholder="Username"
              autoFocus
              autoComplete="username"
            />
          </Form.Item>
          <Form.Item label="Password" required>
            <Input.Password
              name="j_password"
              prefix={<LockOutlined />}
              placeholder="Password"
              autoComplete="current-password"
            />
          </Form.Item>
          <Form.Item style={{ marginBottom: 0 }}>
            <Button type="primary" htmlType="submit" block size="large">
              Log in
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
}

export default LoginPage;
