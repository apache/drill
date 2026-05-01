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
import type { Project } from '../types';

/**
 * Cover style for a project tile.
 *
 * The server stores tileColor (hex) and tileImage (data URI) on the Project record.
 * This UI-side type adds an explicit `auto` and `preset` distinction so the picker
 * can highlight palette swatches when the project's stored color matches one.
 */
export type CoverStyle =
  | { kind: 'auto' }
  | { kind: 'preset'; index: number }
  | { kind: 'custom'; color: string }
  | { kind: 'image'; dataUri: string };

/**
 * Build a CoverStyle from a Project's persisted tile fields.
 * Pass the picker's TINT_PALETTE so we can detect preset matches by hex.
 */
export function coverFromProject(
  project: Pick<Project, 'tileColor' | 'tileImage'>,
  palette: { mono: string }[],
): CoverStyle {
  if (project.tileImage) {
    return { kind: 'image', dataUri: project.tileImage };
  }
  if (project.tileColor) {
    const idx = palette.findIndex((t) => t.mono.toLowerCase() === project.tileColor!.toLowerCase());
    if (idx >= 0) {
      return { kind: 'preset', index: idx };
    }
    return { kind: 'custom', color: project.tileColor };
  }
  return { kind: 'auto' };
}

/**
 * Convert a CoverStyle back to the (tileColor, tileImage) wire fields for the Project API.
 */
export function coverToProjectFields(
  cover: CoverStyle,
  palette: { mono: string }[],
): { tileColor: string | null; tileImage: string | null } {
  switch (cover.kind) {
    case 'image':
      return { tileColor: null, tileImage: cover.dataUri };
    case 'preset':
      return { tileColor: palette[cover.index]?.mono ?? null, tileImage: null };
    case 'custom':
      return { tileColor: cover.color, tileImage: null };
    case 'auto':
    default:
      return { tileColor: null, tileImage: null };
  }
}

/**
 * Downscale an image File to a JPEG data URI suitable for tile backgrounds.
 * Caps dimensions to keep the persisted record reasonably small.
 */
export async function downscaleImageFile(
  file: File,
  maxWidth = 1200,
  maxHeight = 600,
  quality = 0.85,
): Promise<string> {
  const dataUri = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error ?? new Error('Read failed'));
    reader.readAsDataURL(file);
  });

  return await new Promise<string>((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const ratio = Math.min(maxWidth / img.width, maxHeight / img.height, 1);
      const w = Math.round(img.width * ratio);
      const h = Math.round(img.height * ratio);
      const canvas = document.createElement('canvas');
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext('2d');
      if (!ctx) {
        reject(new Error('Canvas not supported'));
        return;
      }
      ctx.drawImage(img, 0, 0, w, h);
      resolve(canvas.toDataURL('image/jpeg', quality));
    };
    img.onerror = () => reject(new Error('Could not decode image'));
    img.src = dataUri;
  });
}
