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
import { useEffect, useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import { Modal } from 'antd';
import {
  RobotOutlined,
  MailOutlined,
  BarChartOutlined,
  SettingOutlined,
  FileTextOutlined,
  RightOutlined,
  BgColorsOutlined,
  HistoryOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { ProspectorSettingsBody } from '../prospector/ProspectorSettingsModal';
import { SmtpSettingsBody } from '../smtp/SmtpSettingsModal';
import { ProfileSettingsBody } from '../data-profiler/ProfileSettingsModal';
import WorkspaceRecoveryBody from './WorkspaceRecoveryBody';
import TrashBody from './TrashBody';
import { useTheme } from '../../hooks/useTheme';

interface Section {
  key: string;
  title: string;
  icon: ReactNode;
  /** When set, navigate to this route instead of rendering inline */
  navigateTo?: string;
  /** Inline body renderer */
  render?: (close: () => void) => ReactNode;
  /** Subtitle shown in the sidebar */
  subtitle?: string;
}

interface Props {
  open: boolean;
  onClose: () => void;
  /** Initial section to focus when opening (defaults to 'appearance') */
  initialKey?: string;
}

export default function PreferencesModal({ open, onClose, initialKey = 'appearance' }: Props) {
  const navigate = useNavigate();
  const [activeKey, setActiveKey] = useState<string>(initialKey);
  const { mode, setMode } = useTheme();

  // Reset to the initial section each time the modal opens
  useEffect(() => {
    if (open) {
      setActiveKey(initialKey);
    }
  }, [open, initialKey]);

  const sections: Section[] = [
    {
      key: 'appearance',
      title: 'Appearance',
      subtitle: 'Light, dark, or system',
      icon: <BgColorsOutlined />,
      render: () => (
        <div className="settings-body">
          <div className="prefs-appearance-row">
            {(['light', 'dark', 'system'] as const).map((m) => (
              <button
                key={m}
                type="button"
                className={`prefs-appearance-card${mode === m ? ' is-selected' : ''}`}
                onClick={() => setMode(m)}
              >
                <div className={`prefs-appearance-swatch is-${m}`} />
                <span className="prefs-appearance-label">
                  {m === 'light' ? 'Light' : m === 'dark' ? 'Dark' : 'Match System'}
                </span>
              </button>
            ))}
          </div>
        </div>
      ),
    },
    {
      key: 'prospector',
      title: 'Prospector',
      subtitle: 'AI assistant configuration',
      icon: <RobotOutlined />,
      render: (close) => <ProspectorSettingsBody onSaved={close} />,
    },
    {
      key: 'email',
      title: 'Email',
      subtitle: 'SMTP for alerts',
      icon: <MailOutlined />,
      render: (close) => <SmtpSettingsBody onSaved={close} />,
    },
    {
      key: 'profiler',
      title: 'Data Profiler',
      subtitle: 'Sampling, histogram bins',
      icon: <BarChartOutlined />,
      render: (close) => <ProfileSettingsBody onSaved={close} />,
    },
    {
      key: 'recovery',
      title: 'Workspace Recovery',
      subtitle: 'Browse persisted SQL Lab tabs',
      icon: <HistoryOutlined />,
      render: () => <WorkspaceRecoveryBody />,
    },
    {
      key: 'trash',
      title: 'Trash',
      subtitle: 'Restore or permanently delete',
      icon: <DeleteOutlined />,
      render: () => <TrashBody />,
    },
    {
      key: 'system',
      title: 'System Options',
      subtitle: 'All Drill server options',
      icon: <SettingOutlined />,
      navigateTo: '/options',
    },
    {
      key: 'logs',
      title: 'Server Logs',
      subtitle: 'Live server log stream',
      icon: <FileTextOutlined />,
      navigateTo: '/logs',
    },
  ];

  const active = sections.find((s) => s.key === activeKey) ?? sections[0];

  const handleSectionClick = (s: Section) => {
    if (s.navigateTo) {
      onClose();
      navigate(s.navigateTo);
    } else {
      setActiveKey(s.key);
    }
  };

  return (
    <Modal
      open={open}
      onCancel={onClose}
      footer={null}
      width={820}
      destroyOnClose
      closable
      className="prefs-modal"
      title={null}
    >
      <div className="prefs-shell">
        <aside className="prefs-sidebar">
          <h2 className="prefs-sidebar-title">Preferences</h2>
          <ul className="prefs-section-list">
            {sections.map((s) => (
              <li key={s.key}>
                <button
                  type="button"
                  className={`prefs-section${active.key === s.key && !s.navigateTo ? ' is-active' : ''}`}
                  onClick={() => handleSectionClick(s)}
                >
                  <span className="prefs-section-icon">{s.icon}</span>
                  <span className="prefs-section-text">
                    <span className="prefs-section-title">{s.title}</span>
                    {s.subtitle && <span className="prefs-section-subtitle">{s.subtitle}</span>}
                  </span>
                  {s.navigateTo && <RightOutlined className="prefs-section-arrow" />}
                </button>
              </li>
            ))}
          </ul>
        </aside>
        <main className="prefs-pane">
          <header className="prefs-pane-header">
            <h3 className="prefs-pane-title">{active.title}</h3>
            {active.subtitle && <p className="prefs-pane-subtitle">{active.subtitle}</p>}
          </header>
          <div className="prefs-pane-body">
            {active.render ? active.render(onClose) : null}
          </div>
        </main>
      </div>
    </Modal>
  );
}
