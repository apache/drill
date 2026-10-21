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
import { Drawer } from 'antd';
import { useNavigate } from 'react-router-dom';
import SchemaExplorer from '../schema-explorer/SchemaExplorer';

interface Props {
  open: boolean;
  onClose: () => void;
}

/** Shape passed via react-router state when the drawer hands off to SQL Lab. */
export interface PendingQueryFromDrawer {
  sql: string;
  name?: string;
}

/**
 * Global "Browse data" drawer. Opens from the toolbar button or ⌘B (when no
 * page-specific left rail is registered) so users can explore plugins,
 * schemas, tables and columns from any page in the app.
 *
 * Double-click a table-like node → navigate to SQL Lab with the SELECT query
 * waiting in router state; SqlLabPage picks it up and opens it in a new tab.
 * Other clicks expand/inspect — there's no editor in the drawer to insert into.
 */
export default function BrowseDataDrawer({ open, onClose }: Props) {
  const navigate = useNavigate();

  const handleOpenInNewTab = (sql: string, name?: string) => {
    onClose();
    const pending: PendingQueryFromDrawer = { sql, name };
    navigate('/query', { state: { pendingQuery: pending } });
  };

  return (
    <Drawer
      title="Browse data"
      placement="left"
      open={open}
      onClose={onClose}
      width={Math.min(420, typeof window !== 'undefined' ? window.innerWidth - 80 : 420)}
      styles={{ body: { padding: 0 } }}
    >
      <SchemaExplorer onOpenInNewTab={handleOpenInNewTab} />
    </Drawer>
  );
}
