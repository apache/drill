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
import { useState, useEffect, useCallback } from 'react';
import {
  Form,
  Input,
  Switch,
  Select,
  Steps,
  Button,
  Alert,
  Space,
  Tooltip,
  Typography,
} from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';

const { Text, Link } = Typography;

const helpIcon = { color: '#999', cursor: 'help' as const };

function label(text: string, tip: string) {
  return (
    <span>
      {text}{' '}
      <Tooltip title={tip}>
        <QuestionCircleOutlined style={helpIcon} />
      </Tooltip>
    </span>
  );
}

// Google OAuth constants (matching GoogleSheetsStoragePluginConfig.java)
const AUTH_URI = 'https://accounts.google.com/o/oauth2/auth';
const TOKEN_URI = 'https://oauth2.googleapis.com/token';
const GOOGLE_SHEET_SCOPE = 'https://www.googleapis.com/auth/spreadsheets';
const GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive.readonly';
const DEFAULT_SCOPE = `${GOOGLE_SHEET_SCOPE} ${GOOGLE_DRIVE_SCOPE}`;

interface GoogleSheetsFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function GoogleSheetsForm({ config, onChange }: GoogleSheetsFormProps) {
  // Detect if we already have a configured plugin (non-wizard mode)
  const credsProv = config.credentialsProvider as Record<string, unknown> | undefined;
  const creds = credsProv?.credentials as Record<string, string> | undefined;
  const existingClientID = creds?.clientID || '';
  const existingClientSecret = creds?.clientSecret || '';

  const [useWizard, setUseWizard] = useState(!existingClientID);
  const [step, setStep] = useState(0);

  // Fields
  const [clientID, setClientID] = useState(existingClientID);
  const [clientSecret, setClientSecret] = useState(existingClientSecret);
  const [allTextMode, setAllTextMode] = useState<boolean>(
    config.allTextMode !== false
  );
  const [extractHeaders, setExtractHeaders] = useState<boolean>(
    config.extractHeaders !== false
  );
  const [authMode, setAuthMode] = useState<string>(
    (config.authMode as string) || 'SHARED_USER'
  );
  const [callbackURL, setCallbackURL] = useState<string>(() => {
    const oauth = config.oAuthConfig as Record<string, unknown> | undefined;
    return (oauth?.callbackURL as string) || '';
  });

  useEffect(() => {
    const cp = config.credentialsProvider as Record<string, unknown> | undefined;
    const cr = cp?.credentials as Record<string, string> | undefined;
    setClientID(cr?.clientID || '');
    setClientSecret(cr?.clientSecret || '');
    setAllTextMode(config.allTextMode !== false);
    setExtractHeaders(config.extractHeaders !== false);
    setAuthMode((config.authMode as string) || 'SHARED_USER');
    const oauth = config.oAuthConfig as Record<string, unknown> | undefined;
    setCallbackURL((oauth?.callbackURL as string) || '');
  }, [config]);

  const buildConfig = useCallback(
    (overrides: Record<string, unknown> = {}): Record<string, unknown> => {
      const cid = overrides.clientID ?? clientID;
      const csec = overrides.clientSecret ?? clientSecret;
      const atm = overrides.allTextMode ?? allTextMode;
      const eh = overrides.extractHeaders ?? extractHeaders;
      const am = overrides.authMode ?? authMode;
      const cb = overrides.callbackURL ?? callbackURL;

      const callbackFinal = (cb as string) ||
        `http://localhost:8047/credentials/${config.type || 'googlesheets'}/update_oauth2_authtoken`;

      return {
        ...config,
        type: 'googlesheets',
        allTextMode: atm,
        extractHeaders: eh,
        authMode: am,
        oAuthConfig: {
          callbackURL: callbackFinal,
          authorizationURL: AUTH_URI,
          authorizationParams: {
            response_type: 'code',
            scope: DEFAULT_SCOPE,
          },
        },
        credentialsProvider: {
          credentialsProviderType: 'PlainCredentialsProvider',
          credentials: {
            clientID: cid as string,
            clientSecret: csec as string,
            tokenURI: TOKEN_URI,
          },
          userCredentials: {},
        },
      };
    },
    [clientID, clientSecret, allTextMode, extractHeaders, authMode, callbackURL, config]
  );

  const emitFull = useCallback(
    (overrides: Record<string, unknown> = {}) => {
      onChange(buildConfig(overrides));
    },
    [buildConfig, onChange]
  );

  // ─── Wizard Steps ───

  const stepItems = [
    { title: 'Google Cloud Setup' },
    { title: 'Credentials' },
    { title: 'Options' },
    { title: 'Review' },
  ];

  const renderStep0 = () => (
    <div style={{ maxWidth: 600 }}>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="Before you begin"
        description="You need a Google Cloud project with the Google Sheets API and Google Drive API enabled, and an OAuth 2.0 credential (Client ID and Secret)."
      />
      <Text strong style={{ display: 'block', marginBottom: 8 }}>
        Step-by-step:
      </Text>
      <ol style={{ paddingLeft: 20, lineHeight: 2 }}>
        <li>
          Go to the{' '}
          <Link href="https://console.cloud.google.com/apis/dashboard" target="_blank">
            Google Cloud Console
          </Link>
        </li>
        <li>Create a project (or select an existing one)</li>
        <li>
          Enable the{' '}
          <Link href="https://console.cloud.google.com/apis/library/sheets.googleapis.com" target="_blank">
            Google Sheets API
          </Link>
          {' '}and the{' '}
          <Link href="https://console.cloud.google.com/apis/library/drive.googleapis.com" target="_blank">
            Google Drive API
          </Link>
        </li>
        <li>
          Go to{' '}
          <Link href="https://console.cloud.google.com/apis/credentials" target="_blank">
            Credentials
          </Link>
          {' '}and create an <strong>OAuth 2.0 Client ID</strong> (type: Web Application)
        </li>
        <li>
          Add an <strong>Authorized redirect URI</strong>:{' '}
          <Text code>
            http://&lt;your-drill-host&gt;:8047/credentials/googlesheets/update_oauth2_authtoken
          </Text>
        </li>
        <li>Copy the <strong>Client ID</strong> and <strong>Client Secret</strong> for the next step</li>
      </ol>
    </div>
  );

  const renderStep1 = () => (
    <Form layout="vertical" style={{ maxWidth: 500 }}>
      <Form.Item
        label={label('Client ID', 'The OAuth 2.0 Client ID from your Google Cloud Console credentials.')}
        required
      >
        <Input
          value={clientID}
          onChange={(e) => setClientID(e.target.value)}
          placeholder="xxxxxxxxxxxx.apps.googleusercontent.com"
        />
      </Form.Item>

      <Form.Item
        label={label('Client Secret', 'The OAuth 2.0 Client Secret from your Google Cloud Console credentials.')}
        required
      >
        <Input.Password
          value={clientSecret}
          onChange={(e) => setClientSecret(e.target.value)}
          placeholder="GOCSPX-xxxxxxxxxxxx"
        />
      </Form.Item>

      <Form.Item
        label={label('Callback URL', 'The redirect URI registered in your Google Cloud Console. Must match exactly. Uses the Drill default if left empty.')}
      >
        <Input
          value={callbackURL}
          onChange={(e) => setCallbackURL(e.target.value)}
          placeholder="http://localhost:8047/credentials/googlesheets/update_oauth2_authtoken"
        />
      </Form.Item>
    </Form>
  );

  const renderStep2 = () => (
    <Form layout="vertical" style={{ maxWidth: 500 }}>
      <Form.Item label={label('Auth Mode', 'Shared User \u2014 All queries use a single OAuth token. User Translation \u2014 Each user authorizes independently with their own Google account.')}>
        <Select
          value={authMode}
          onChange={(val) => setAuthMode(val)}
          style={{ width: 300 }}
          options={[
            { value: 'SHARED_USER', label: 'Shared User' },
            { value: 'USER_TRANSLATION', label: 'User Translation' },
          ]}
        />
      </Form.Item>

      <Form.Item label={label('Extract Headers', 'When enabled, the first row of each sheet is treated as column headers. When disabled, columns are named field_0, field_1, etc.')}>
        <Switch
          checked={extractHeaders}
          onChange={(checked) => setExtractHeaders(checked)}
        />
      </Form.Item>

      <Form.Item label={label('All Text Mode', 'When enabled, all columns are read as VARCHAR (text). Disabling allows Drill to infer data types automatically.')}>
        <Switch
          checked={allTextMode}
          onChange={(checked) => setAllTextMode(checked)}
        />
      </Form.Item>
    </Form>
  );

  const renderStep3 = () => {
    const displayCallback = callbackURL ||
      'http://localhost:8047/credentials/googlesheets/update_oauth2_authtoken';
    return (
      <div style={{ maxWidth: 500 }}>
        <Text strong style={{ display: 'block', marginBottom: 12 }}>
          Review your configuration:
        </Text>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <tbody>
            {[
              ['Client ID', clientID ? `${clientID.substring(0, 20)}...` : '(not set)'],
              ['Client Secret', clientSecret ? '\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022' : '(not set)'],
              ['Callback URL', displayCallback],
              ['Auth Mode', authMode === 'USER_TRANSLATION' ? 'User Translation' : 'Shared User'],
              ['Extract Headers', extractHeaders ? 'Yes' : 'No'],
              ['All Text Mode', allTextMode ? 'Yes' : 'No'],
            ].map(([k, v]) => (
              <tr key={k} style={{ borderBottom: '1px solid var(--color-border, #f0f0f0)' }}>
                <td style={{ padding: '8px 12px 8px 0', fontWeight: 500 }}>{k}</td>
                <td style={{ padding: '8px 0', wordBreak: 'break-all' }}>{v}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {(!clientID || !clientSecret) && (
          <Alert
            type="warning"
            showIcon
            style={{ marginTop: 16 }}
            message="Client ID and Client Secret are required. Go back to fill them in."
          />
        )}
        <Alert
          type="info"
          showIcon
          style={{ marginTop: 16 }}
          message="After saving, click Enable on the plugin, then use the Authorize button to complete the OAuth flow with Google."
        />
      </div>
    );
  };

  const wizardSteps = [renderStep0, renderStep1, renderStep2, renderStep3];

  const handleNext = () => {
    if (step === stepItems.length - 1) {
      // Final step — emit config
      emitFull();
    } else {
      setStep(step + 1);
    }
  };

  const handlePrev = () => {
    if (step > 0) {
      setStep(step - 1);
    }
  };

  const handleFinish = () => {
    emitFull();
  };

  // ─── Manual Mode ───

  const renderManual = () => (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Credentials
      </Text>

      <Form.Item label={label('Client ID', 'The OAuth 2.0 Client ID from your Google Cloud Console credentials.')}>
        <Input
          value={clientID}
          onChange={(e) => {
            setClientID(e.target.value);
            emitFull({ clientID: e.target.value });
          }}
          placeholder="xxxxxxxxxxxx.apps.googleusercontent.com"
        />
      </Form.Item>

      <Form.Item label={label('Client Secret', 'The OAuth 2.0 Client Secret from your Google Cloud Console credentials.')}>
        <Input.Password
          value={clientSecret}
          onChange={(e) => {
            setClientSecret(e.target.value);
            emitFull({ clientSecret: e.target.value });
          }}
          placeholder="GOCSPX-xxxxxxxxxxxx"
        />
      </Form.Item>

      <Form.Item label={label('Callback URL', 'The redirect URI registered in your Google Cloud Console. Must match exactly.')}>
        <Input
          value={callbackURL}
          onChange={(e) => {
            setCallbackURL(e.target.value);
            emitFull({ callbackURL: e.target.value });
          }}
          placeholder="http://localhost:8047/credentials/googlesheets/update_oauth2_authtoken"
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Authentication
      </Text>

      <Form.Item label={label('Auth Mode', 'Shared User \u2014 All queries use a single OAuth token. User Translation \u2014 Each user authorizes independently.')}>
        <Select
          value={authMode}
          onChange={(val) => {
            setAuthMode(val);
            emitFull({ authMode: val });
          }}
          style={{ width: 300 }}
          options={[
            { value: 'SHARED_USER', label: 'Shared User' },
            { value: 'USER_TRANSLATION', label: 'User Translation' },
          ]}
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Options
      </Text>

      <Form.Item label={label('Extract Headers', 'When enabled, the first row of each sheet is treated as column headers.')}>
        <Switch
          checked={extractHeaders}
          onChange={(checked) => {
            setExtractHeaders(checked);
            emitFull({ extractHeaders: checked });
          }}
        />
      </Form.Item>

      <Form.Item label={label('All Text Mode', 'When enabled, all columns are read as VARCHAR. Disabling allows automatic type inference.')}>
        <Switch
          checked={allTextMode}
          onChange={(checked) => {
            setAllTextMode(checked);
            emitFull({ allTextMode: checked });
          }}
        />
      </Form.Item>
    </Form>
  );

  // ─── Render ───

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end' }}>
        <Button
          type="link"
          onClick={() => {
            setUseWizard(!useWizard);
            if (useWizard) {
              // Switching to manual — emit current state
              emitFull();
            }
          }}
        >
          {useWizard ? 'Switch to manual configuration' : 'Switch to setup wizard'}
        </Button>
      </div>

      {useWizard ? (
        <div>
          <Steps
            current={step}
            items={stepItems}
            style={{ marginBottom: 24 }}
            size="small"
          />
          <div style={{ minHeight: 200, marginBottom: 24 }}>
            {wizardSteps[step]()}
          </div>
          <Space>
            {step > 0 && (
              <Button onClick={handlePrev}>Previous</Button>
            )}
            {step < stepItems.length - 1 ? (
              <Button
                type="primary"
                onClick={handleNext}
                disabled={step === 1 && (!clientID || !clientSecret)}
              >
                Next
              </Button>
            ) : (
              <Button
                type="primary"
                onClick={handleFinish}
                disabled={!clientID || !clientSecret}
              >
                Apply Configuration
              </Button>
            )}
          </Space>
        </div>
      ) : (
        renderManual()
      )}
    </div>
  );
}
