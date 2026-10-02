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
import { useSearchParams } from 'react-router-dom';
import { Alert, Button, Card, Space, Spin, Typography } from 'antd';
import { KeyOutlined, SafetyCertificateOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getAuthConfig } from '../api/auth';

const { Title, Text } = Typography;

/**
 * Auth-mechanism chooser shown when more than one of Form / SPNEGO is
 * enabled, or as a fallback landing for `/mainLogin`. The actual auth
 * negotiation happens on the destination URL.
 */
function MainLoginPage() {
  const [params] = useSearchParams();
  const errorKind = params.get('error');

  const { data: config, isLoading } = useQuery({
    queryKey: ['auth-config'],
    queryFn: getAuthConfig,
    // The config is essentially static for the life of the Drillbit.
    staleTime: Infinity,
    retry: false,
  });

  const errorMessage = (() => {
    if (errorKind === 'spnego') {
      return 'Invalid SPNEGO credentials, or SPNEGO is not configured on this Drillbit.';
    }
    if (errorKind) {
      return 'Authentication failed.';
    }
    return null;
  })();

  return (
    <div className="login-shell">
      <Card className="login-card" bordered={false}>
        <div className="login-brand">
          <img src="/static/img/apache-drill-logo.png" alt="Apache Drill" className="login-logo" />
          <Title level={3} style={{ margin: 0 }}>Apache Drill</Title>
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

        {isLoading ? (
          <div style={{ textAlign: 'center', padding: 24 }}>
            <Spin />
          </div>
        ) : !config?.authEnabled ? (
          <Alert
            type="info"
            showIcon
            message="Authentication is disabled"
            description="This Drillbit is running without user authentication. No login is required."
          />
        ) : (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            {config.formEnabled && (
              <Button
                type="primary"
                size="large"
                block
                icon={<KeyOutlined />}
                href="/login"
              >
                Sign in with username & password
              </Button>
            )}
            {config.spnegoEnabled && (
              <Button
                size="large"
                block
                icon={<SafetyCertificateOutlined />}
                href="/spnegoLogin"
              >
                Sign in with SPNEGO
              </Button>
            )}
            {!config.formEnabled && !config.spnegoEnabled && (
              <Alert
                type="warning"
                showIcon
                message="No HTTP auth mechanism is configured."
              />
            )}
          </Space>
        )}
      </Card>
    </div>
  );
}

export default MainLoginPage;
