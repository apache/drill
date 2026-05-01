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
import { useRef, useState } from 'react';
import { Segmented, message, Spin } from 'antd';
import { CheckOutlined, PictureOutlined, DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { downscaleImageFile, type CoverStyle } from '../../hooks/useProjectAppearance';

// Re-export so consumers can import everything from the picker module.
export type { CoverStyle };

export const TINT_PALETTE: { from: string; to: string; mono: string }[] = [
  { from: '#FFB1A4', to: '#FF6B5C', mono: '#FF3B30' },
  { from: '#FFD080', to: '#FF9F0A', mono: '#FF9500' },
  { from: '#FFE074', to: '#FFCC00', mono: '#FFCC00' },
  { from: '#A8E6A0', to: '#34C759', mono: '#34C759' },
  { from: '#8DDFE0', to: '#00C7BE', mono: '#00C7BE' },
  { from: '#80C8FF', to: '#0A84FF', mono: '#007AFF' },
  { from: '#A8B5FF', to: '#5E5CE6', mono: '#5E5CE6' },
  { from: '#D2B0FF', to: '#AF52DE', mono: '#AF52DE' },
  { from: '#FFA8DA', to: '#FF2D55', mono: '#FF2D55' },
];

const MAX_FILE_BYTES = 5 * 1024 * 1024;

function hashIndex(input: string, mod: number): number {
  let h = 0;
  for (let i = 0; i < input.length; i++) {
    h = (h << 5) - h + input.charCodeAt(i);
    h |= 0;
  }
  return Math.abs(h) % mod;
}

function lighten(hex: string, amount = 0.45): string {
  const m = hex.replace('#', '');
  const r = parseInt(m.slice(0, 2), 16);
  const g = parseInt(m.slice(2, 4), 16);
  const b = parseInt(m.slice(4, 6), 16);
  const lr = Math.round(r + (255 - r) * amount);
  const lg = Math.round(g + (255 - g) * amount);
  const lb = Math.round(b + (255 - b) * amount);
  return `#${[lr, lg, lb].map((n) => n.toString(16).padStart(2, '0')).join('')}`;
}

/** Compute the tile gradient/image background CSS for a given cover style */
export function resolveTileBackground(
  cover: CoverStyle,
  hashSeed: string,
): { background?: string; backgroundImage?: string; backgroundSize?: string; backgroundPosition?: string } {
  if (cover.kind === 'image') {
    return {
      backgroundImage: `url(${cover.dataUri})`,
      backgroundSize: 'cover',
      backgroundPosition: 'center',
    };
  }
  let from: string;
  let to: string;
  if (cover.kind === 'preset') {
    const t = TINT_PALETTE[cover.index] ?? TINT_PALETTE[0];
    from = t.from;
    to = t.to;
  } else if (cover.kind === 'custom') {
    from = lighten(cover.color, 0.4);
    to = cover.color;
  } else {
    const t = TINT_PALETTE[hashIndex(hashSeed, TINT_PALETTE.length)];
    from = t.from;
    to = t.to;
  }
  return { background: `linear-gradient(135deg, ${from} 0%, ${to} 100%)` };
}

function monogramFor(name: string): string {
  const words = name.trim().split(/\s+/);
  if (words.length >= 2) {
    return (words[0][0] + words[1][0]).toUpperCase();
  }
  return name.slice(0, 2).toUpperCase();
}

interface Props {
  value: CoverStyle;
  onChange: (next: CoverStyle) => void;
  /** Used for the live preview's monogram and the auto-hash color */
  projectName: string;
  projectIdSeed: string;
}

type Tab = 'auto' | 'color' | 'image';

export default function ProjectAppearanceField({ value, onChange, projectName, projectIdSeed }: Props) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  // Lets the user sit on the "Image" tab before picking a file (cover stays
  // 'auto' until they actually choose something).
  const [forceTab, setForceTab] = useState<Tab | null>(null);

  const derivedTab: Tab =
    value.kind === 'image'
      ? 'image'
      : value.kind === 'preset' || value.kind === 'custom'
        ? 'color'
        : 'auto';
  const tab: Tab = forceTab ?? derivedTab;

  const switchTab = (next: Tab) => {
    if (next === tab) {
      // If user re-clicks "Image" and there's no image yet, re-open the picker.
      if (next === 'image' && value.kind !== 'image') {
        fileInputRef.current?.click();
      }
      return;
    }
    if (next === 'auto') {
      setForceTab(null);
      onChange({ kind: 'auto' });
    } else if (next === 'color') {
      setForceTab(null);
      onChange({ kind: 'preset', index: 5 });
    } else {
      // 'image' — sit on the Image tab; auto-open the file picker so the
      // segment click feels actionable. The cover stays at its previous value
      // until the user picks something.
      setForceTab('image');
      requestAnimationFrame(() => fileInputRef.current?.click());
    }
  };

  const handleFile = async (file: File | null | undefined) => {
    if (!file) {
      return;
    }
    if (!file.type.startsWith('image/')) {
      message.error('Please choose an image file');
      return;
    }
    if (file.size > MAX_FILE_BYTES) {
      message.error('Image too large — please choose one under 5 MB');
      return;
    }
    setUploading(true);
    try {
      const dataUri = await downscaleImageFile(file);
      onChange({ kind: 'image', dataUri });
    } catch (e) {
      message.error(`Couldn't process image: ${(e as Error).message}`);
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    if (e.dataTransfer.files?.[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const previewBg = resolveTileBackground(value, projectIdSeed);
  const isImage = value.kind === 'image';

  return (
    <div className="appearance-field">
      <div
        className={`appearance-preview${isImage ? ' has-image' : ''}`}
        style={previewBg}
      >
        {!isImage && <span className="appearance-preview-monogram">{monogramFor(projectName || 'New')}</span>}
      </div>

      <Segmented
        block
        value={tab}
        onChange={(v) => switchTab(v as 'auto' | 'color' | 'image')}
        options={[
          { label: 'Auto', value: 'auto' },
          { label: 'Color', value: 'color' },
          { label: 'Image', value: 'image' },
        ]}
      />

      {tab === 'auto' && (
        <p className="appearance-hint">
          Drill picks a stable color from the project's name.
        </p>
      )}

      {tab === 'color' && (
        <div className="appearance-color-row">
          {TINT_PALETTE.map((t, idx) => {
            const selected = value.kind === 'preset' && value.index === idx;
            return (
              <button
                key={idx}
                type="button"
                className={`appearance-swatch${selected ? ' is-selected' : ''}`}
                style={{ background: `linear-gradient(135deg, ${t.from} 0%, ${t.to} 100%)` }}
                onClick={() => onChange({ kind: 'preset', index: idx })}
                aria-label={`Color ${idx + 1}`}
              >
                {selected && <CheckOutlined />}
              </button>
            );
          })}
          <label className={`appearance-swatch appearance-swatch-custom${value.kind === 'custom' ? ' is-selected' : ''}`}>
            <input
              type="color"
              value={value.kind === 'custom' ? value.color : '#0A84FF'}
              onChange={(e) => onChange({ kind: 'custom', color: e.target.value })}
            />
          </label>
        </div>
      )}

      {tab === 'image' && (
        <div
          className="appearance-image-zone"
          onDrop={handleDrop}
          onDragOver={(e) => e.preventDefault()}
        >
          {uploading ? (
            <div className="appearance-image-uploading">
              <Spin size="small" /> Processing…
            </div>
          ) : isImage ? (
            <div className="appearance-image-actions">
              <button
                type="button"
                className="appearance-image-btn"
                onClick={() => fileInputRef.current?.click()}
              >
                <PictureOutlined /> Replace
              </button>
              <button
                type="button"
                className="appearance-image-btn is-danger"
                onClick={() => onChange({ kind: 'auto' })}
              >
                <DeleteOutlined /> Remove
              </button>
            </div>
          ) : (
            <button
              type="button"
              className="appearance-image-cta"
              onClick={() => fileInputRef.current?.click()}
            >
              <PlusOutlined />
              <span>Choose or drop an image</span>
              <span className="appearance-image-hint">PNG, JPEG, or WebP — auto-resized to 1200×600</span>
            </button>
          )}
          <input
            ref={fileInputRef}
            type="file"
            accept="image/*"
            style={{ display: 'none' }}
            onChange={(e) => {
              handleFile(e.target.files?.[0]);
              e.target.value = '';
            }}
          />
        </div>
      )}
    </div>
  );
}
