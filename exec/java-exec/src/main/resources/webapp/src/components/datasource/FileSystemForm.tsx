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
import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Form,
  Input,
  Switch,
  Select,
  Table,
  Button,
  Space,
  Tabs,
  Typography,
  Alert,
  Tooltip,
} from 'antd';
import { PlusOutlined, DeleteOutlined, ApiOutlined, RobotOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import Markdown from 'react-markdown';
import type { WorkspaceConfig } from '../../types';
import { testConnection, TestConnectionResult } from '../../api/storage';
import { getAiStatus, streamChat } from '../../api/ai';
import Editor from '@monaco-editor/react';

const { Text } = Typography;

// ---------------------------------------------------------------------------
// File system type definitions
// ---------------------------------------------------------------------------

interface FsTypeOption {
  value: string;
  label: string;
  protocol: string;
  placeholder: string;
  logo?: string;
}

const FS_TYPES: FsTypeOption[] = [
  { value: 'azure', label: 'Azure', protocol: 'wasbs://', placeholder: 'wasbs://container@account.blob.core.windows.net', logo: '/static/img/storage_logos/Azure.png' },
  { value: 'box', label: 'Box', protocol: 'box:///', placeholder: 'box:///', logo: '/static/img/storage_logos/Box.png' },
  { value: 'classpath', label: 'Classpath', protocol: 'classpath:///', placeholder: 'classpath:///', logo: '/static/img/storage_logos/FileSystem.png' },
  { value: 'dropbox', label: 'Dropbox', protocol: 'dropbox:///', placeholder: 'dropbox:///', logo: '/static/img/storage_logos/Dropbox.png' },
  { value: 'gcp', label: 'GCP', protocol: 'gs://', placeholder: 'gs://bucket-name', logo: '/static/img/storage_logos/GCP.png' },
  { value: 'hdfs', label: 'HDFS', protocol: 'hdfs://', placeholder: 'hdfs://namenode:8020', logo: '/static/img/storage_logos/Hadoop.png' },
  { value: 'file', label: 'Local File System', protocol: 'file:///', placeholder: 'file:///', logo: '/static/img/storage_logos/FileSystem.png' },
  { value: 'oci', label: 'OCI (Oracle Cloud)', protocol: 'oci://', placeholder: 'oci://bucket@namespace', logo: '/static/img/storage_logos/OCI.png' },
  { value: 's3', label: 'S3', protocol: 's3a://', placeholder: 's3a://bucket-name', logo: '/static/img/storage_logos/S3.png' },
  { value: 'sftp', label: 'SFTP', protocol: 'sftp://', placeholder: 'sftp://host', logo: '/static/img/storage_logos/SFTP.png' },
  { value: 'other', label: 'Other', protocol: '', placeholder: 'Enter connection URL' },
];

/** FS types that need no authentication options. */
const NO_AUTH_FS_TYPES = new Set(['file', 'classpath', 'hdfs', 's3', 'gcp', 'azure', 'oci']);

function detectFsType(connection: string): string {
  const lower = connection.toLowerCase();
  // Azure can use multiple protocols
  if (lower.startsWith('wasbs://') || lower.startsWith('wasb://') ||
      lower.startsWith('abfss://') || lower.startsWith('abfs://')) {
    return 'azure';
  }
  for (const fs of FS_TYPES) {
    if (fs.value !== 'other' && fs.protocol && lower.startsWith(fs.protocol.toLowerCase())) {
      return fs.value;
    }
  }
  return connection ? 'other' : 'file';
}

// ---------------------------------------------------------------------------
// S3 region / endpoint list
// ---------------------------------------------------------------------------

const S3_REGIONS: { value: string; label: string }[] = [
  { value: 's3.us-east-1.amazonaws.com', label: 'US East (N. Virginia) — us-east-1' },
  { value: 's3.us-east-2.amazonaws.com', label: 'US East (Ohio) — us-east-2' },
  { value: 's3.us-west-1.amazonaws.com', label: 'US West (N. California) — us-west-1' },
  { value: 's3.us-west-2.amazonaws.com', label: 'US West (Oregon) — us-west-2' },
  { value: 's3.af-south-1.amazonaws.com', label: 'Africa (Cape Town) — af-south-1' },
  { value: 's3.ap-east-1.amazonaws.com', label: 'Asia Pacific (Hong Kong) — ap-east-1' },
  { value: 's3.ap-south-1.amazonaws.com', label: 'Asia Pacific (Mumbai) — ap-south-1' },
  { value: 's3.ap-southeast-1.amazonaws.com', label: 'Asia Pacific (Singapore) — ap-southeast-1' },
  { value: 's3.ap-southeast-2.amazonaws.com', label: 'Asia Pacific (Sydney) — ap-southeast-2' },
  { value: 's3.ap-northeast-1.amazonaws.com', label: 'Asia Pacific (Tokyo) — ap-northeast-1' },
  { value: 's3.ap-northeast-2.amazonaws.com', label: 'Asia Pacific (Seoul) — ap-northeast-2' },
  { value: 's3.ap-northeast-3.amazonaws.com', label: 'Asia Pacific (Osaka) — ap-northeast-3' },
  { value: 's3.ca-central-1.amazonaws.com', label: 'Canada (Central) — ca-central-1' },
  { value: 's3.eu-central-1.amazonaws.com', label: 'Europe (Frankfurt) — eu-central-1' },
  { value: 's3.eu-west-1.amazonaws.com', label: 'Europe (Ireland) — eu-west-1' },
  { value: 's3.eu-west-2.amazonaws.com', label: 'Europe (London) — eu-west-2' },
  { value: 's3.eu-west-3.amazonaws.com', label: 'Europe (Paris) — eu-west-3' },
  { value: 's3.eu-north-1.amazonaws.com', label: 'Europe (Stockholm) — eu-north-1' },
  { value: 's3.eu-south-1.amazonaws.com', label: 'Europe (Milan) — eu-south-1' },
  { value: 's3.me-south-1.amazonaws.com', label: 'Middle East (Bahrain) — me-south-1' },
  { value: 's3.sa-east-1.amazonaws.com', label: 'South America (São Paulo) — sa-east-1' },
];

// ---------------------------------------------------------------------------
// Azure helpers
// ---------------------------------------------------------------------------

/** Parse Azure account name and container from a WASB(S) connection string. */
function parseAzureConnection(conn: string): { account: string; container: string } {
  const match = conn.match(/^wasbs?:\/\/([^@]*)@([^.]*)/i);
  if (match) {
    return { container: match[1], account: match[2] };
  }
  const abfsMatch = conn.match(/^abfss?:\/\/([^@]*)@([^.]*)/i);
  if (abfsMatch) {
    return { container: abfsMatch[1], account: abfsMatch[2] };
  }
  return { account: '', container: '' };
}

/** Build the Azure WASBS connection URL from account + container. */
function buildAzureConnection(account: string, container: string): string {
  if (!account) {
    return 'wasbs://';
  }
  return `wasbs://${container}@${account}.blob.core.windows.net`;
}

/** Build the Azure Hadoop config key for the account key. */
function azureAccountKeyProp(account: string): string {
  return `fs.azure.account.key.${account}.blob.core.windows.net`;
}

/** Find the Azure account key value in the Hadoop config map. */
function findAzureAccountKey(hadoopCfg: Record<string, string> | undefined): string {
  if (!hadoopCfg) {
    return '';
  }
  for (const [k, v] of Object.entries(hadoopCfg)) {
    if (k.startsWith('fs.azure.account.key.') && k.endsWith('.blob.core.windows.net')) {
      return v;
    }
  }
  return '';
}

/** Find the old Azure account key property name so we can remove it on rename. */
function findAzureAccountKeyProp(hadoopCfg: Record<string, string> | undefined): string | undefined {
  if (!hadoopCfg) {
    return undefined;
  }
  for (const k of Object.keys(hadoopCfg)) {
    if (k.startsWith('fs.azure.account.key.') && k.endsWith('.blob.core.windows.net')) {
      return k;
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// OCI helpers
// ---------------------------------------------------------------------------

const OCI_REGIONS: { value: string; label: string }[] = [
  { value: 'https://objectstorage.us-ashburn-1.oraclecloud.com', label: 'US East (Ashburn) — us-ashburn-1' },
  { value: 'https://objectstorage.us-phoenix-1.oraclecloud.com', label: 'US West (Phoenix) — us-phoenix-1' },
  { value: 'https://objectstorage.us-sanjose-1.oraclecloud.com', label: 'US West (San Jose) — us-sanjose-1' },
  { value: 'https://objectstorage.us-chicago-1.oraclecloud.com', label: 'US Midwest (Chicago) — us-chicago-1' },
  { value: 'https://objectstorage.ca-toronto-1.oraclecloud.com', label: 'Canada SE (Toronto) — ca-toronto-1' },
  { value: 'https://objectstorage.ca-montreal-1.oraclecloud.com', label: 'Canada SE (Montreal) — ca-montreal-1' },
  { value: 'https://objectstorage.sa-saopaulo-1.oraclecloud.com', label: 'Brazil East (São Paulo) — sa-saopaulo-1' },
  { value: 'https://objectstorage.sa-vinhedo-1.oraclecloud.com', label: 'Brazil SE (Vinhedo) — sa-vinhedo-1' },
  { value: 'https://objectstorage.eu-frankfurt-1.oraclecloud.com', label: 'Germany Central (Frankfurt) — eu-frankfurt-1' },
  { value: 'https://objectstorage.eu-zurich-1.oraclecloud.com', label: 'Switzerland North (Zurich) — eu-zurich-1' },
  { value: 'https://objectstorage.eu-amsterdam-1.oraclecloud.com', label: 'Netherlands NW (Amsterdam) — eu-amsterdam-1' },
  { value: 'https://objectstorage.eu-madrid-1.oraclecloud.com', label: 'Spain Central (Madrid) — eu-madrid-1' },
  { value: 'https://objectstorage.eu-marseille-1.oraclecloud.com', label: 'France South (Marseille) — eu-marseille-1' },
  { value: 'https://objectstorage.eu-milan-1.oraclecloud.com', label: 'Italy NW (Milan) — eu-milan-1' },
  { value: 'https://objectstorage.eu-stockholm-1.oraclecloud.com', label: 'Sweden Central (Stockholm) — eu-stockholm-1' },
  { value: 'https://objectstorage.uk-london-1.oraclecloud.com', label: 'UK South (London) — uk-london-1' },
  { value: 'https://objectstorage.uk-cardiff-1.oraclecloud.com', label: 'UK West (Cardiff) — uk-cardiff-1' },
  { value: 'https://objectstorage.me-jeddah-1.oraclecloud.com', label: 'Saudi Arabia West (Jeddah) — me-jeddah-1' },
  { value: 'https://objectstorage.me-dubai-1.oraclecloud.com', label: 'UAE East (Dubai) — me-dubai-1' },
  { value: 'https://objectstorage.af-johannesburg-1.oraclecloud.com', label: 'South Africa Central (Johannesburg) — af-johannesburg-1' },
  { value: 'https://objectstorage.ap-mumbai-1.oraclecloud.com', label: 'India West (Mumbai) — ap-mumbai-1' },
  { value: 'https://objectstorage.ap-hyderabad-1.oraclecloud.com', label: 'India South (Hyderabad) — ap-hyderabad-1' },
  { value: 'https://objectstorage.ap-tokyo-1.oraclecloud.com', label: 'Japan East (Tokyo) — ap-tokyo-1' },
  { value: 'https://objectstorage.ap-osaka-1.oraclecloud.com', label: 'Japan Central (Osaka) — ap-osaka-1' },
  { value: 'https://objectstorage.ap-seoul-1.oraclecloud.com', label: 'South Korea Central (Seoul) — ap-seoul-1' },
  { value: 'https://objectstorage.ap-singapore-1.oraclecloud.com', label: 'Singapore (Singapore) — ap-singapore-1' },
  { value: 'https://objectstorage.ap-sydney-1.oraclecloud.com', label: 'Australia East (Sydney) — ap-sydney-1' },
  { value: 'https://objectstorage.ap-melbourne-1.oraclecloud.com', label: 'Australia SE (Melbourne) — ap-melbourne-1' },
];

/** Parse OCI bucket name and namespace from an oci:// connection string. */
function parseOciConnection(conn: string): { bucket: string; namespace: string } {
  const match = conn.match(/^oci:\/\/([^@]*)@(.*)$/i);
  if (match) {
    return { bucket: match[1], namespace: match[2] };
  }
  return { bucket: '', namespace: '' };
}

/** Build the OCI connection URL from bucket + namespace. */
function buildOciConnection(bucket: string, namespace: string): string {
  if (!bucket && !namespace) {
    return 'oci://';
  }
  return `oci://${bucket}@${namespace}`;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface FileSystemFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
  onValidationChange?: (isValid: boolean) => void;
  pluginName?: string;
}

interface WorkspaceRow {
  key: string;
  name: string;
  location: string;
  writable: boolean;
  defaultInputFormat?: string;
}

/** Read a string from the Hadoop config map inside the plugin config. */
function hGet(cfg: Record<string, unknown>, key: string): string {
  const hadoopCfg = cfg.config as Record<string, string> | undefined;
  return hadoopCfg?.[key] || '';
}

export default function FileSystemForm({ config, onChange, onValidationChange, pluginName }: FileSystemFormProps) {
  const connStr = (config.connection as string) || 'file:///';
  const [connection, setConnection] = useState<string>(connStr);
  const [fsType, setFsType] = useState<string>(() => detectFsType(connStr));
  const [authMode, setAuthMode] = useState<string>((config.authMode as string) || 'SHARED_USER');
  const [workspaces, setWorkspaces] = useState<WorkspaceRow[]>([]);
  const [formatsJson, setFormatsJson] = useState<string>('{}');

  // Test connection state
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null);
  const [aiAvailable, setAiAvailable] = useState(false);
  const [aiExplaining, setAiExplaining] = useState(false);
  const [aiExplanation, setAiExplanation] = useState('');
  const aiAbortRef = useRef<AbortController | null>(null);

  // S3 fields
  const [s3AccessKey, setS3AccessKey] = useState<string>('');
  const [s3SecretKey, setS3SecretKey] = useState<string>('');
  const [s3Endpoint, setS3Endpoint] = useState<string>('');
  const [s3DisableCache, setS3DisableCache] = useState<boolean>(false);

  // Azure fields
  const [azureAccount, setAzureAccount] = useState<string>('');
  const [azureContainer, setAzureContainer] = useState<string>('');
  const [azureAccountKey, setAzureAccountKey] = useState<string>('');

  // OCI fields
  const [ociBucket, setOciBucket] = useState<string>('');
  const [ociNamespace, setOciNamespace] = useState<string>('');
  const [ociHostname, setOciHostname] = useState<string>('');
  const [ociTenantId, setOciTenantId] = useState<string>('');
  const [ociUserId, setOciUserId] = useState<string>('');
  const [ociFingerprint, setOciFingerprint] = useState<string>('');
  const [ociPemFilePath, setOciPemFilePath] = useState<string>('');

  // GCP fields
  const [gcpProjectId, setGcpProjectId] = useState<string>('');
  const [gcpServiceAccountEmail, setGcpServiceAccountEmail] = useState<string>('');
  const [gcpPrivateKeyId, setGcpPrivateKeyId] = useState<string>('');
  const [gcpPrivateKey, setGcpPrivateKey] = useState<string>('');

  // SFTP fields
  const [sftpUsername, setSftpUsername] = useState<string>('');
  const [sftpPassword, setSftpPassword] = useState<string>('');

  // Dropbox fields
  const [dropboxAuthMethod, setDropboxAuthMethod] = useState<'token' | 'oauth'>('token');
  const [dropboxAccessToken, setDropboxAccessToken] = useState<string>('');
  const [dropboxClientId, setDropboxClientId] = useState<string>('');
  const [dropboxClientSecret, setDropboxClientSecret] = useState<string>('');

  // Box fields (OAuth only)
  const [boxClientId, setBoxClientId] = useState<string>('');
  const [boxClientSecret, setBoxClientSecret] = useState<string>('');

  useEffect(() => {
    const conn = (config.connection as string) || 'file:///';
    setConnection(conn);
    setFsType(detectFsType(conn));
    setAuthMode((config.authMode as string) || 'SHARED_USER');

    // S3 fields
    setS3AccessKey(hGet(config, 'fs.s3a.access.key'));
    setS3SecretKey(hGet(config, 'fs.s3a.secret.key'));
    setS3Endpoint(hGet(config, 'fs.s3a.endpoint'));
    setS3DisableCache(hGet(config, 'fs.s3a.impl.disable.cache') === 'true');

    // Azure fields
    const azParsed = parseAzureConnection(conn);
    setAzureAccount(azParsed.account);
    setAzureContainer(azParsed.container);
    setAzureAccountKey(findAzureAccountKey(config.config as Record<string, string> | undefined));

    // OCI fields
    const ociParsed = parseOciConnection(conn);
    setOciBucket(ociParsed.bucket);
    setOciNamespace(ociParsed.namespace);
    setOciHostname(hGet(config, 'fs.oci.client.hostname'));
    setOciTenantId(hGet(config, 'fs.oci.client.auth.tenantId'));
    setOciUserId(hGet(config, 'fs.oci.client.auth.userId'));
    setOciFingerprint(hGet(config, 'fs.oci.client.auth.fingerprint'));
    setOciPemFilePath(hGet(config, 'fs.oci.client.auth.pemfilepath'));

    // GCP fields
    setGcpProjectId(hGet(config, 'fs.gs.project.id'));
    setGcpServiceAccountEmail(hGet(config, 'fs.gs.auth.service.account.email'));
    setGcpPrivateKeyId(hGet(config, 'fs.gs.auth.service.account.private.key.id'));
    setGcpPrivateKey(hGet(config, 'fs.gs.auth.service.account.private.key'));

    // SFTP fields — credentials stored in credentialsProvider
    const cp = config.credentialsProvider as Record<string, unknown> | undefined;
    const cpCreds = cp?.credentials as Record<string, string> | undefined;
    setSftpUsername(cpCreds?.username || '');
    setSftpPassword(cpCreds?.password || '');

    // Dropbox fields — detect auth method from existing config
    const detectedType = detectFsType(conn);
    const hasOAuth = !!(config.oAuthConfig || cpCreds?.clientID);
    const dbToken = hGet(config, 'dropboxAccessToken');
    setDropboxAuthMethod(hasOAuth && detectedType === 'dropbox' ? 'oauth' : 'token');
    setDropboxAccessToken(dbToken);
    setDropboxClientId(detectedType === 'dropbox' ? (cpCreds?.clientID || '') : '');
    setDropboxClientSecret(detectedType === 'dropbox' ? (cpCreds?.clientSecret || '') : '');

    // Box fields — always OAuth
    setBoxClientId(detectedType === 'box' ? (cpCreds?.clientID || '') : '');
    setBoxClientSecret(detectedType === 'box' ? (cpCreds?.clientSecret || '') : '');

    const ws = (config.workspaces as Record<string, WorkspaceConfig>) || {};
    setWorkspaces(
      Object.entries(ws).map(([name, w]) => ({
        key: name,
        name,
        location: w.location || '/',
        writable: w.writable || false,
        defaultInputFormat: w.defaultInputFormat,
      }))
    );

    const formats = config.formats || {};
    setFormatsJson(JSON.stringify(formats, null, 2));
  }, [config]);

  // -----------------------------------------------------------------------
  // emitChange — build the full config and notify parent
  // -----------------------------------------------------------------------

  const emitChange = useCallback(
    (
      conn: string,
      auth: string,
      ws: WorkspaceRow[],
      fmtJson: string,
      hadoopOverrides?: Record<string, string | undefined>,
      topLevelOverrides?: Record<string, unknown>,
    ) => {
      const wsMap: Record<string, WorkspaceConfig> = {};
      ws.forEach((row) => {
        if (row.name) {
          const entry: WorkspaceConfig = {
            location: row.location,
            writable: row.writable,
            allowAccessOutsideWorkspace: false,
          };
          if (row.defaultInputFormat) {
            entry.defaultInputFormat = row.defaultInputFormat;
          }
          wsMap[row.name] = entry;
        }
      });

      let formats: Record<string, unknown> = {};
      try {
        formats = JSON.parse(fmtJson);
      } catch {
        formats = config.formats as Record<string, unknown> || {};
      }

      // Merge hadoop config: preserve existing entries, apply overrides
      const hadoopCfg: Record<string, string> = {
        ...((config.config as Record<string, string>) || {}),
      };
      if (hadoopOverrides) {
        for (const [k, v] of Object.entries(hadoopOverrides)) {
          if (v === undefined || v === '') {
            delete hadoopCfg[k];
          } else {
            hadoopCfg[k] = v;
          }
        }
      }
      const hasHadoopCfg = Object.keys(hadoopCfg).length > 0;

      const updated: Record<string, unknown> = {
        ...config,
        connection: conn,
        authMode: auth,
        workspaces: wsMap,
        formats,
        config: hasHadoopCfg ? hadoopCfg : undefined,
      };

      // Apply top-level overrides (credentialsProvider, oAuthConfig, etc.)
      // A value of null means "delete this key"
      if (topLevelOverrides) {
        for (const [k, v] of Object.entries(topLevelOverrides)) {
          if (v === null) {
            delete updated[k];
          } else {
            updated[k] = v;
          }
        }
      }

      onChange(updated);
    },
    [config, onChange, onValidationChange]
  );

  // -----------------------------------------------------------------------
  // Connection / FS type handlers
  // -----------------------------------------------------------------------

  const handleConnectionChange = (val: string) => {
    setConnection(val);
    const detected = detectFsType(val);
    setFsType(detected);
    if (detected === 'azure') {
      const parsed = parseAzureConnection(val);
      setAzureAccount(parsed.account);
      setAzureContainer(parsed.container);
    }
    if (detected === 'oci') {
      const parsed = parseOciConnection(val);
      setOciBucket(parsed.bucket);
      setOciNamespace(parsed.namespace);
    }
    emitChange(val, authMode, workspaces, formatsJson);
  };

  const handleFsTypeChange = (val: string) => {
    setFsType(val);
    const fs = FS_TYPES.find((f) => f.value === val);
    const newConn = fs?.protocol || '';
    setConnection(newConn);
    if (val === 'azure') {
      setAzureAccount('');
      setAzureContainer('');
      setAzureAccountKey('');
    }
    if (val === 'oci') {
      setOciBucket('');
      setOciNamespace('');
      setOciHostname('');
      setOciTenantId('');
      setOciUserId('');
      setOciFingerprint('');
      setOciPemFilePath('');
    }
    if (val === 'gcp') {
      setGcpProjectId('');
      setGcpServiceAccountEmail('');
      setGcpPrivateKeyId('');
      setGcpPrivateKey('');
    }
    if (val === 'sftp') {
      setSftpUsername('');
      setSftpPassword('');
    }
    if (val === 'dropbox') {
      setDropboxAuthMethod('token');
      setDropboxAccessToken('');
      setDropboxClientId('');
      setDropboxClientSecret('');
    }
    if (val === 'box') {
      setBoxClientId('');
      setBoxClientSecret('');
    }
    // Clear credentialsProvider/oAuthConfig when switching FS type
    const needsCredentials = val === 'sftp' || val === 'dropbox' || val === 'box';
    const topLevel: Record<string, unknown> = {};
    if (!needsCredentials) {
      topLevel.credentialsProvider = null;
      topLevel.oAuthConfig = null;
    }
    emitChange(newConn, authMode, workspaces, formatsJson, undefined,
      Object.keys(topLevel).length > 0 ? topLevel : undefined);
  };

  const handleAuthModeChange = (val: string) => {
    setAuthMode(val);
    emitChange(connection, val, workspaces, formatsJson);
  };

  // -----------------------------------------------------------------------
  // S3-specific handlers
  // -----------------------------------------------------------------------

  const handleS3AccessKeyChange = (val: string) => {
    setS3AccessKey(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.s3a.access.key': val,
    });
  };

  const handleS3SecretKeyChange = (val: string) => {
    setS3SecretKey(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.s3a.secret.key': val,
    });
  };

  const handleS3EndpointChange = (val: string) => {
    setS3Endpoint(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.s3a.endpoint': val,
    });
  };

  const handleS3DisableCacheChange = (checked: boolean) => {
    setS3DisableCache(checked);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.s3a.impl.disable.cache': checked ? 'true' : undefined,
    });
  };

  // -----------------------------------------------------------------------
  // Azure-specific handlers
  // -----------------------------------------------------------------------

  const handleAzureAccountChange = (val: string) => {
    // Remove old key prop, add new one with existing key value
    const oldProp = findAzureAccountKeyProp(config.config as Record<string, string> | undefined);
    const overrides: Record<string, string | undefined> = {};
    if (oldProp) {
      overrides[oldProp] = undefined; // remove old
    }
    if (val && azureAccountKey) {
      overrides[azureAccountKeyProp(val)] = azureAccountKey;
    }
    setAzureAccount(val);
    const newConn = buildAzureConnection(val, azureContainer);
    setConnection(newConn);
    emitChange(newConn, authMode, workspaces, formatsJson, overrides);
  };

  const handleAzureContainerChange = (val: string) => {
    setAzureContainer(val);
    const newConn = buildAzureConnection(azureAccount, val);
    setConnection(newConn);
    emitChange(newConn, authMode, workspaces, formatsJson);
  };

  const handleAzureAccountKeyChange = (val: string) => {
    setAzureAccountKey(val);
    if (azureAccount) {
      emitChange(connection, authMode, workspaces, formatsJson, {
        [azureAccountKeyProp(azureAccount)]: val,
      });
    }
  };

  // -----------------------------------------------------------------------
  // OCI-specific handlers
  // -----------------------------------------------------------------------

  const handleOciBucketChange = (val: string) => {
    setOciBucket(val);
    const newConn = buildOciConnection(val, ociNamespace);
    setConnection(newConn);
    emitChange(newConn, authMode, workspaces, formatsJson);
  };

  const handleOciNamespaceChange = (val: string) => {
    setOciNamespace(val);
    const newConn = buildOciConnection(ociBucket, val);
    setConnection(newConn);
    emitChange(newConn, authMode, workspaces, formatsJson);
  };

  const handleOciHostnameChange = (val: string) => {
    setOciHostname(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.oci.client.hostname': val,
    });
  };

  const handleOciTenantIdChange = (val: string) => {
    setOciTenantId(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.oci.client.auth.tenantId': val,
    });
  };

  const handleOciUserIdChange = (val: string) => {
    setOciUserId(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.oci.client.auth.userId': val,
    });
  };

  const handleOciFingerprintChange = (val: string) => {
    setOciFingerprint(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.oci.client.auth.fingerprint': val,
    });
  };

  const handleOciPemFilePathChange = (val: string) => {
    setOciPemFilePath(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.oci.client.auth.pemfilepath': val,
    });
  };

  // -----------------------------------------------------------------------
  // GCP-specific handlers
  // -----------------------------------------------------------------------

  const handleGcpProjectIdChange = (val: string) => {
    setGcpProjectId(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.gs.project.id': val,
    });
  };

  const handleGcpServiceAccountEmailChange = (val: string) => {
    setGcpServiceAccountEmail(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.gs.auth.service.account.email': val,
    });
  };

  const handleGcpPrivateKeyIdChange = (val: string) => {
    setGcpPrivateKeyId(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.gs.auth.service.account.private.key.id': val,
    });
  };

  const handleGcpPrivateKeyChange = (val: string) => {
    setGcpPrivateKey(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      'fs.gs.auth.service.account.private.key': val,
    });
  };

  // -----------------------------------------------------------------------
  // SFTP-specific handlers
  // -----------------------------------------------------------------------

  const buildSftpCredentialsProvider = (user: string, pass: string) => {
    if (!user && !pass) {
      return null;
    }
    return {
      credentialsProviderType: 'PlainCredentialsProvider',
      credentials: { username: user, password: pass },
      userCredentials: {},
    };
  };

  const handleSftpUsernameChange = (val: string) => {
    setSftpUsername(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildSftpCredentialsProvider(val, sftpPassword),
    });
  };

  const handleSftpPasswordChange = (val: string) => {
    setSftpPassword(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildSftpCredentialsProvider(sftpUsername, val),
    });
  };

  // -----------------------------------------------------------------------
  // Dropbox-specific handlers
  // -----------------------------------------------------------------------

  const buildDropboxOAuthConfig = (name: string) => ({
    callbackURL: `${window.location.origin}/credentials/${encodeURIComponent(name)}/update_oauth2_authtoken`,
    authorizationURL: 'https://www.dropbox.com/oauth2/authorize',
    authorizationParams: {
      response_type: 'code',
      token_access_type: 'offline',
    },
  });

  const buildDropboxOAuthCredentials = (clientId: string, clientSecret: string) => {
    if (!clientId && !clientSecret) {
      return null;
    }
    return {
      credentialsProviderType: 'PlainCredentialsProvider',
      credentials: {
        clientID: clientId,
        clientSecret: clientSecret,
        tokenURI: 'https://www.dropbox.com/oauth2/token',
      },
      userCredentials: {},
    };
  };

  const handleDropboxAuthMethodChange = (method: 'token' | 'oauth') => {
    setDropboxAuthMethod(method);
    if (method === 'token') {
      // Switch to token: clear OAuth fields, set token in hadoop config
      setDropboxClientId('');
      setDropboxClientSecret('');
      emitChange(connection, authMode, workspaces, formatsJson, {
        dropboxAccessToken: dropboxAccessToken,
      }, {
        credentialsProvider: null,
        oAuthConfig: null,
      });
    } else {
      // Switch to OAuth: clear token from hadoop config, set up OAuth
      setDropboxAccessToken('');
      emitChange(connection, authMode, workspaces, formatsJson, {
        dropboxAccessToken: undefined,
      }, {
        credentialsProvider: buildDropboxOAuthCredentials(dropboxClientId, dropboxClientSecret),
        oAuthConfig: pluginName ? buildDropboxOAuthConfig(pluginName) : null,
      });
    }
  };

  const handleDropboxAccessTokenChange = (val: string) => {
    setDropboxAccessToken(val);
    emitChange(connection, authMode, workspaces, formatsJson, {
      dropboxAccessToken: val,
    });
  };

  const handleDropboxClientIdChange = (val: string) => {
    setDropboxClientId(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildDropboxOAuthCredentials(val, dropboxClientSecret),
      oAuthConfig: pluginName ? buildDropboxOAuthConfig(pluginName) : null,
    });
  };

  const handleDropboxClientSecretChange = (val: string) => {
    setDropboxClientSecret(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildDropboxOAuthCredentials(dropboxClientId, val),
      oAuthConfig: pluginName ? buildDropboxOAuthConfig(pluginName) : null,
    });
  };

  const handleDropboxAuthorize = () => {
    if (!pluginName || !dropboxClientId) {
      return;
    }
    const callbackUrl = `${window.location.origin}/credentials/${encodeURIComponent(pluginName)}/update_oauth2_authtoken`;
    const authUrl =
      `https://www.dropbox.com/oauth2/authorize` +
      `?client_id=${encodeURIComponent(dropboxClientId)}` +
      `&redirect_uri=${encodeURIComponent(callbackUrl)}` +
      `&response_type=code` +
      `&token_access_type=offline`;

    const popup = window.open(authUrl, 'Authorize Dropbox',
      'toolbar=no,menubar=no,scrollbars=yes,resizable=yes,width=500,height=700');
    const timer = setInterval(() => {
      if (!popup || popup.closed) {
        clearInterval(timer);
      }
    }, 1000);
  };

  // -----------------------------------------------------------------------
  // Box-specific handlers (OAuth only)
  // -----------------------------------------------------------------------

  const buildBoxOAuthConfig = (name: string) => ({
    callbackURL: `${window.location.origin}/credentials/${encodeURIComponent(name)}/update_oauth2_authtoken`,
    authorizationURL: 'https://account.box.com/api/oauth2/authorize',
    authorizationParams: {
      response_type: 'code',
    },
  });

  const buildBoxOAuthCredentials = (clientId: string, clientSecret: string) => {
    if (!clientId && !clientSecret) {
      return null;
    }
    return {
      credentialsProviderType: 'PlainCredentialsProvider',
      credentials: {
        clientID: clientId,
        clientSecret: clientSecret,
        tokenURI: 'https://api.box.com/oauth2/token',
      },
      userCredentials: {},
    };
  };

  const handleBoxClientIdChange = (val: string) => {
    setBoxClientId(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildBoxOAuthCredentials(val, boxClientSecret),
      oAuthConfig: pluginName ? buildBoxOAuthConfig(pluginName) : null,
    });
  };

  const handleBoxClientSecretChange = (val: string) => {
    setBoxClientSecret(val);
    emitChange(connection, authMode, workspaces, formatsJson, undefined, {
      credentialsProvider: buildBoxOAuthCredentials(boxClientId, val),
      oAuthConfig: pluginName ? buildBoxOAuthConfig(pluginName) : null,
    });
  };

  const handleBoxAuthorize = () => {
    if (!pluginName || !boxClientId) {
      return;
    }
    const callbackUrl = `${window.location.origin}/credentials/${encodeURIComponent(pluginName)}/update_oauth2_authtoken`;
    const authUrl =
      `https://account.box.com/api/oauth2/authorize` +
      `?client_id=${encodeURIComponent(boxClientId)}` +
      `&redirect_uri=${encodeURIComponent(callbackUrl)}` +
      `&response_type=code`;

    window.open(authUrl, 'Authorize Box',
      'toolbar=no,menubar=no,scrollbars=yes,resizable=yes,width=500,height=700');
  };

  // -----------------------------------------------------------------------
  // Test connection & AI explanation
  // -----------------------------------------------------------------------

  useEffect(() => {
    getAiStatus()
      .then((s) => setAiAvailable(s.enabled && s.configured))
      .catch(() => {});
  }, []);

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    setAiExplanation('');
    setAiExplaining(false);
    try {
      const result = await testConnection(pluginName || 'test', config);
      setTestResult(result);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Test connection request failed';
      setTestResult({ success: false, message: msg });
    } finally {
      setTesting(false);
    }
  };

  const handleAiExplain = () => {
    if (!testResult || testResult.success) {
      return;
    }

    setAiExplaining(true);
    setAiExplanation('');

    if (aiAbortRef.current) {
      aiAbortRef.current.abort();
    }

    const fsLabel = FS_TYPES.find((f) => f.value === fsType)?.label || fsType;
    const prompt =
      `I'm configuring an Apache Drill storage plugin. The connection test failed.\n` +
      `Connection URL: ${connection}\n` +
      `File system type: ${fsLabel}\n` +
      `Error: ${testResult.message}\n` +
      (testResult.errorClass ? `Error class: ${testResult.errorClass}\n` : '') +
      `Please explain what this error means and provide specific steps to fix it. Be concise.`;

    const controller = streamChat(
      {
        messages: [{ role: 'user', content: prompt }],
        tools: [],
        context: {},
      },
      (event) => {
        if (event.type === 'content') {
          setAiExplanation((prev) => prev + event.content);
        }
      },
      () => {
        setAiExplaining(false);
      },
      (error) => {
        setAiExplanation((prev) => prev + '\n\nError: ' + error.message);
        setAiExplaining(false);
      },
    );
    aiAbortRef.current = controller;
  };

  // -----------------------------------------------------------------------
  // Workspace handlers
  // -----------------------------------------------------------------------

  const hasValidWorkspaces = useCallback((ws: WorkspaceRow[]) => {
    return ws.filter(w => w.name && w.name.trim()).length > 0;
  }, []);

  // Update validation when fsType changes (e.g., switching to/from classpath)
  useEffect(() => {
    const isValid = fsType === 'classpath' || hasValidWorkspaces(workspaces);
    onValidationChange?.(isValid);
  }, [fsType, workspaces, hasValidWorkspaces, onValidationChange]);

  const handleWorkspaceChange = (newWorkspaces: WorkspaceRow[]) => {
    setWorkspaces(newWorkspaces);
    // Classpath plugin doesn't support workspaces, so it's always valid
    const isValid = fsType === 'classpath' || hasValidWorkspaces(newWorkspaces);
    onValidationChange?.(isValid);
    emitChange(connection, authMode, newWorkspaces, formatsJson);
  };

  const addWorkspace = () => {
    handleWorkspaceChange([
      ...workspaces,
      { key: `ws_${Date.now()}`, name: '', location: '/', writable: false },
    ]);
  };

  const removeWorkspace = (key: string) => {
    handleWorkspaceChange(workspaces.filter((w) => w.key !== key));
  };

  const updateWorkspace = (key: string, field: keyof WorkspaceRow, value: unknown) => {
    handleWorkspaceChange(
      workspaces.map((w) =>
        w.key === key ? { ...w, [field]: value } : w
      )
    );
  };

  // -----------------------------------------------------------------------
  // Workspace table columns
  // -----------------------------------------------------------------------

  const wsColumns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      width: 150,
      render: (_: string, record: WorkspaceRow) => (
        <Input
          size="small"
          value={record.name}
          onChange={(e) => updateWorkspace(record.key, 'name', e.target.value)}
          placeholder="workspace name"
        />
      ),
    },
    {
      title: 'Location',
      dataIndex: 'location',
      key: 'location',
      render: (_: string, record: WorkspaceRow) => (
        <Input
          size="small"
          value={record.location}
          onChange={(e) => updateWorkspace(record.key, 'location', e.target.value)}
          placeholder="/"
        />
      ),
    },
    {
      title: 'Writable',
      dataIndex: 'writable',
      key: 'writable',
      width: 80,
      render: (_: boolean, record: WorkspaceRow) => (
        <Switch
          size="small"
          checked={record.writable}
          onChange={(checked) => updateWorkspace(record.key, 'writable', checked)}
        />
      ),
    },
    {
      title: 'Default Format',
      dataIndex: 'defaultInputFormat',
      key: 'defaultInputFormat',
      width: 140,
      render: (_: string | undefined, record: WorkspaceRow) => (
        <Select
          size="small"
          value={record.defaultInputFormat}
          onChange={(val) => updateWorkspace(record.key, 'defaultInputFormat', val)}
          allowClear
          placeholder="auto"
          style={{ width: '100%' }}
          options={[
            { value: 'csv', label: 'CSV' },
            { value: 'tsv', label: 'TSV' },
            { value: 'json', label: 'JSON' },
            { value: 'parquet', label: 'Parquet' },
            { value: 'avro', label: 'Avro' },
            { value: 'sequencefile', label: 'Sequence' },
          ]}
        />
      ),
    },
    {
      title: '',
      key: 'actions',
      width: 40,
      render: (_: unknown, record: WorkspaceRow) => (
        <Button
          type="text"
          size="small"
          danger
          icon={<DeleteOutlined />}
          onClick={() => removeWorkspace(record.key)}
        />
      ),
    },
  ];

  // -----------------------------------------------------------------------
  // Derived flags
  // -----------------------------------------------------------------------

  const showAuthMode = !NO_AUTH_FS_TYPES.has(fsType);
  const isS3 = fsType === 's3';
  const isAzure = fsType === 'azure';
  const isOci = fsType === 'oci';
  const isGcp = fsType === 'gcp';
  const isSftp = fsType === 'sftp';
  const isDropbox = fsType === 'dropbox';
  const isBox = fsType === 'box';

  // -----------------------------------------------------------------------
  // Tab content
  // -----------------------------------------------------------------------

  const connectionTab = (
    <Form layout="vertical" style={{ padding: '16px 0' }}>
      <Form.Item label="File System Type">
        <Select
          value={fsType}
          onChange={handleFsTypeChange}
          style={{ maxWidth: 300 }}
          optionLabelProp="label"
        >
          {FS_TYPES.map((fs) => (
            <Select.Option key={fs.value} value={fs.value} label={fs.label}>
              <Space>
                {fs.logo ? (
                  <img
                    src={fs.logo}
                    alt=""
                    style={{ width: 18, height: 18, objectFit: 'contain' }}
                  />
                ) : (
                  <span style={{ display: 'inline-block', width: 18 }} />
                )}
                {fs.label}
              </Space>
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      {/* Azure uses structured fields instead of a raw connection input */}
      {isAzure ? (
        <>
          <Form.Item label="Storage Account Name">
            <Input
              value={azureAccount}
              onChange={(e) => handleAzureAccountChange(e.target.value)}
              placeholder="mystorageaccount"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Container">
            <Input
              value={azureContainer}
              onChange={(e) => handleAzureContainerChange(e.target.value)}
              placeholder="mycontainer"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Account Key">
            <Input.Password
              value={azureAccountKey}
              onChange={(e) => handleAzureAccountKeyChange(e.target.value)}
              placeholder="Azure storage account key"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="Connection URL">
            <Input
              value={connection}
              onChange={(e) => handleConnectionChange(e.target.value)}
              placeholder="wasbs://container@account.blob.core.windows.net"
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              Auto-generated from the account name and container above. Edit directly for custom URLs.
            </Text>
          </Form.Item>
        </>
      ) : isOci ? (
        <>
          <Form.Item label="Bucket Name">
            <Input
              value={ociBucket}
              onChange={(e) => handleOciBucketChange(e.target.value)}
              placeholder="my-bucket"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Namespace">
            <Input
              value={ociNamespace}
              onChange={(e) => handleOciNamespaceChange(e.target.value)}
              placeholder="my-namespace"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Region / Endpoint">
            <Select
              value={ociHostname || undefined}
              onChange={handleOciHostnameChange}
              placeholder="Select an OCI region"
              showSearch
              allowClear
              optionFilterProp="label"
              style={{ maxWidth: 500 }}
              options={OCI_REGIONS}
            />
          </Form.Item>

          <Form.Item label="Tenant OCID">
            <Input
              value={ociTenantId}
              onChange={(e) => handleOciTenantIdChange(e.target.value)}
              placeholder="ocid1.tenancy.oc1..aaaa..."
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="User OCID">
            <Input
              value={ociUserId}
              onChange={(e) => handleOciUserIdChange(e.target.value)}
              placeholder="ocid1.user.oc1..aaaa..."
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="API Key Fingerprint">
            <Input
              value={ociFingerprint}
              onChange={(e) => handleOciFingerprintChange(e.target.value)}
              placeholder="aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="PEM File Path">
            <Input
              value={ociPemFilePath}
              onChange={(e) => handleOciPemFilePathChange(e.target.value)}
              placeholder="/path/to/oci_api_key.pem"
              style={{ maxWidth: 500 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              Absolute path to the private key PEM file on the Drill server.
            </Text>
          </Form.Item>

          <Form.Item label="Connection URL">
            <Input
              value={connection}
              onChange={(e) => handleConnectionChange(e.target.value)}
              placeholder="oci://bucket@namespace"
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              Auto-generated from the bucket and namespace above. Edit directly for custom URLs.
            </Text>
          </Form.Item>
        </>
      ) : (
        <Form.Item label="Connection">
          <Input
            value={connection}
            onChange={(e) => handleConnectionChange(e.target.value)}
            placeholder={FS_TYPES.find((f) => f.value === fsType)?.placeholder || 'Enter connection URL'}
          />
        </Form.Item>
      )}

      {/* S3-specific fields */}
      {isS3 && (
        <>
          <Form.Item label="Access Key">
            <Input
              value={s3AccessKey}
              onChange={(e) => handleS3AccessKeyChange(e.target.value)}
              placeholder="AWS access key"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Secret Key">
            <Input.Password
              value={s3SecretKey}
              onChange={(e) => handleS3SecretKeyChange(e.target.value)}
              placeholder="AWS secret key"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="S3 Endpoint">
            <Select
              value={s3Endpoint || undefined}
              onChange={handleS3EndpointChange}
              placeholder="Select an AWS region endpoint"
              showSearch
              allowClear
              optionFilterProp="label"
              style={{ maxWidth: 500 }}
              options={S3_REGIONS}
            />
          </Form.Item>

          <Form.Item label="Disable Cache">
            <Switch
              checked={s3DisableCache}
              onChange={handleS3DisableCacheChange}
            />
            <Text type="secondary" style={{ marginLeft: 8 }}>
              Allows reconnecting with different credentials
            </Text>
          </Form.Item>
        </>
      )}

      {/* GCP-specific fields */}
      {isGcp && (
        <>
          <Form.Item label="Project ID">
            <Input
              value={gcpProjectId}
              onChange={(e) => handleGcpProjectIdChange(e.target.value)}
              placeholder="my-gcp-project"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Service Account Email">
            <Input
              value={gcpServiceAccountEmail}
              onChange={(e) => handleGcpServiceAccountEmailChange(e.target.value)}
              placeholder="my-service-account@my-project.iam.gserviceaccount.com"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="Private Key ID">
            <Input
              value={gcpPrivateKeyId}
              onChange={(e) => handleGcpPrivateKeyIdChange(e.target.value)}
              placeholder="key-id"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="Private Key">
            <Input.TextArea
              value={gcpPrivateKey}
              onChange={(e) => handleGcpPrivateKeyChange(e.target.value)}
              placeholder="-----BEGIN PRIVATE KEY-----&#10;...&#10;-----END PRIVATE KEY-----"
              autoSize={{ minRows: 3, maxRows: 8 }}
              style={{ maxWidth: 500, fontFamily: 'monospace', fontSize: 12 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              The PEM private key from your service account JSON key file.
            </Text>
          </Form.Item>
        </>
      )}

      {/* SFTP-specific fields */}
      {isSftp && (
        <>
          <Form.Item label="Username">
            <Input
              value={sftpUsername}
              onChange={(e) => handleSftpUsernameChange(e.target.value)}
              placeholder="SFTP username"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>

          <Form.Item label="Password">
            <Input.Password
              value={sftpPassword}
              onChange={(e) => handleSftpPasswordChange(e.target.value)}
              placeholder="SFTP password"
              style={{ maxWidth: 400 }}
            />
          </Form.Item>
        </>
      )}

      {/* Dropbox-specific fields */}
      {isDropbox && (
        <>
          <Form.Item label="Authentication Method">
            <Select
              value={dropboxAuthMethod}
              onChange={handleDropboxAuthMethodChange}
              options={[
                { value: 'token', label: 'Access Token' },
                { value: 'oauth', label: 'OAuth 2.0' },
              ]}
              style={{ maxWidth: 300 }}
            />
          </Form.Item>

          {dropboxAuthMethod === 'token' && (
            <Form.Item label="Access Token">
              <Input.Password
                value={dropboxAccessToken}
                onChange={(e) => handleDropboxAccessTokenChange(e.target.value)}
                placeholder="Dropbox API access token"
                style={{ maxWidth: 500 }}
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Generate a token from the Dropbox App Console.
              </Text>
            </Form.Item>
          )}

          {dropboxAuthMethod === 'oauth' && (
            <>
              <Form.Item label="Client ID">
                <Input
                  value={dropboxClientId}
                  onChange={(e) => handleDropboxClientIdChange(e.target.value)}
                  placeholder="Dropbox app key"
                  style={{ maxWidth: 500 }}
                />
              </Form.Item>

              <Form.Item label="Client Secret">
                <Input.Password
                  value={dropboxClientSecret}
                  onChange={(e) => handleDropboxClientSecretChange(e.target.value)}
                  placeholder="Dropbox app secret"
                  style={{ maxWidth: 500 }}
                />
              </Form.Item>

              <Form.Item>
                <Space direction="vertical" size={4}>
                  <Button
                    type="primary"
                    onClick={handleDropboxAuthorize}
                    disabled={!pluginName || !dropboxClientId || !dropboxClientSecret}
                  >
                    Authorize with Dropbox
                  </Button>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Save the plugin first, then click to authorize. A popup will open for Dropbox login.
                  </Text>
                </Space>
              </Form.Item>
            </>
          )}
        </>
      )}

      {/* Box-specific fields (OAuth only) */}
      {isBox && (
        <>
          <Form.Item label="Client ID">
            <Input
              value={boxClientId}
              onChange={(e) => handleBoxClientIdChange(e.target.value)}
              placeholder="Box app client ID"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item label="Client Secret">
            <Input.Password
              value={boxClientSecret}
              onChange={(e) => handleBoxClientSecretChange(e.target.value)}
              placeholder="Box app client secret"
              style={{ maxWidth: 500 }}
            />
          </Form.Item>

          <Form.Item>
            <Space direction="vertical" size={4}>
              <Button
                type="primary"
                onClick={handleBoxAuthorize}
                disabled={!pluginName || !boxClientId || !boxClientSecret}
              >
                Authorize with Box
              </Button>
              <Text type="secondary" style={{ fontSize: 12 }}>
                Save the plugin first, then click to authorize. A popup will open for Box login.
              </Text>
            </Space>
          </Form.Item>
        </>
      )}

      {/* Auth mode — hidden for types that don't need it, or Dropbox with token auth */}
      {showAuthMode && !(isDropbox && dropboxAuthMethod === 'token') && (
        <Form.Item
          label={
            <span>
              Auth Mode{' '}
              <Tooltip
                title={
                  <div>
                    <b>Shared User</b> — All queries use a single set of credentials configured here.<br /><br />
                    {!(isSftp || isDropbox || isBox) && (
                      <><b>User Impersonation</b> — Drill authenticates with the stored credentials but impersonates the logged-in user for file access.<br /><br /></>
                    )}
                    <b>User Translation</b> — Each user provides their own credentials. Queries run with that user's identity.
                  </div>
                }
              >
                <QuestionCircleOutlined style={{ color: '#999', cursor: 'help' }} />
              </Tooltip>
            </span>
          }
        >
          <Select
            value={authMode}
            onChange={handleAuthModeChange}
            options={
              isSftp || isDropbox || isBox
                ? [
                    { value: 'SHARED_USER', label: 'Shared User' },
                    { value: 'USER_TRANSLATION', label: 'User Translation' },
                  ]
                : [
                    { value: 'SHARED_USER', label: 'Shared User' },
                    { value: 'USER_IMPERSONATION', label: 'User Impersonation' },
                    { value: 'USER_TRANSLATION', label: 'User Translation' },
                  ]
            }
            style={{ maxWidth: 300 }}
          />
        </Form.Item>
      )}

      {/* Test Connection */}
      <Form.Item>
        <Button
          icon={<ApiOutlined />}
          onClick={handleTestConnection}
          loading={testing}
        >
          Test Connection
        </Button>
      </Form.Item>

      {testResult && testResult.success && (
        <Alert
          type="success"
          showIcon
          message="Connection Successful"
          description={testResult.message}
          style={{ marginBottom: 16 }}
        />
      )}

      {testResult && !testResult.success && (
        <Alert
          type="error"
          showIcon
          message="Connection Failed"
          description={testResult.message}
          action={
            aiAvailable && !aiExplaining && !aiExplanation ? (
              <Button
                size="small"
                icon={<RobotOutlined />}
                onClick={handleAiExplain}
              >
                Explain with Prospector
              </Button>
            ) : undefined
          }
          style={{ marginBottom: 16 }}
        />
      )}

      {(aiExplaining || aiExplanation) && (
        <Alert
          type="info"
          showIcon
          icon={<RobotOutlined />}
          message="Prospector Analysis"
          description={
            <div>
              <Markdown>{aiExplanation || 'Analyzing...'}</Markdown>
              {aiExplaining && <span style={{ color: '#999' }}>Thinking...</span>}
            </div>
          }
          style={{ marginBottom: 16 }}
        />
      )}
    </Form>
  );

  const workspacesTab = (
    <div style={{ padding: '16px 0' }}>
      <Alert
        type="info"
        showIcon={false}
        message="Workspaces define named virtual directories that map to specific paths in your storage. They allow users to query data using schema notation like dfs.workspace_name.table_name instead of full paths."
        style={{ marginBottom: 16, backgroundColor: '#f0f5ff', border: '1px solid #d6e4ff' }}
      />
      <Space style={{ marginBottom: 12 }}>
        <Button
          type="primary"
          size="small"
          icon={<PlusOutlined />}
          onClick={addWorkspace}
        >
          Add Workspace
        </Button>
      </Space>
      <Table
        dataSource={workspaces}
        columns={wsColumns}
        pagination={false}
        size="small"
        rowKey="key"
      />
      {!hasValidWorkspaces(workspaces) && (
        <Alert
          type="warning"
          showIcon
          message="At least one workspace is required"
          description="The plugin cannot be saved without defining at least one workspace with a valid name."
          style={{ marginTop: 16 }}
        />
      )}
    </div>
  );

  const formatsTab = (
    <div style={{ padding: '16px 0' }}>
      <div style={{ border: '1px solid #d9d9d9', borderRadius: 4 }}>
        <Editor
          height="400px"
          language="json"
          value={formatsJson}
          onChange={(val) => {
            const v = val || '{}';
            setFormatsJson(v);
            emitChange(connection, authMode, workspaces, v);
          }}
          options={{
            minimap: { enabled: false },
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            fontSize: 13,
          }}
        />
      </div>
    </div>
  );

  // -----------------------------------------------------------------------
  // Render
  // -----------------------------------------------------------------------

  const tabItems = [
    { key: 'connection', label: 'Connection', children: connectionTab },
  ];

  // Only show workspaces tab for non-classpath plugins
  if (fsType !== 'classpath') {
    tabItems.push({ key: 'workspaces', label: 'Workspaces', children: workspacesTab });
  }

  tabItems.push({ key: 'formats', label: 'Formats', children: formatsTab });

  return (
    <Tabs
      defaultActiveKey="connection"
      items={tabItems}
    />
  );
}
