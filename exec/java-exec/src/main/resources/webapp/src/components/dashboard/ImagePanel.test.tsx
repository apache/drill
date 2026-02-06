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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ImagePanel from './ImagePanel';

// Mock the uploadImage API
const mockUploadImage = vi.fn();
vi.mock('../../api/dashboards', () => ({
  uploadImage: (...args: unknown[]) => mockUploadImage(...args),
}));

// Mock antd message
const mockMessageError = vi.fn();
const mockMessageSuccess = vi.fn();
vi.mock('antd', async () => {
  const actual = await vi.importActual('antd');
  return {
    ...(actual as object),
    message: {
      error: (...args: unknown[]) => mockMessageError(...args),
      success: (...args: unknown[]) => mockMessageSuccess(...args),
    },
  };
});

describe('ImagePanel', () => {
  const defaultProps = {
    content: '',
    config: undefined as Record<string, string> | undefined,
    editMode: false,
    onContentChange: vi.fn(),
    onConfigChange: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  // ==================== View Mode Tests ====================

  describe('view mode', () => {
    it('renders placeholder icon when content is empty', () => {
      const { container } = render(<ImagePanel {...defaultProps} />);
      expect(container.querySelector('.image-panel')).toBeInTheDocument();
      // Should not render an img tag
      expect(container.querySelector('img')).not.toBeInTheDocument();
    });

    it('renders image when content URL is provided', () => {
      const { container } = render(
        <ImagePanel {...defaultProps} content="https://example.com/photo.png" />
      );
      const img = container.querySelector('img');
      expect(img).toBeInTheDocument();
      expect(img).toHaveAttribute('src', 'https://example.com/photo.png');
    });

    it('renders image with alt text from config', () => {
      const { container } = render(
        <ImagePanel
          {...defaultProps}
          content="https://example.com/photo.png"
          config={{ imageAlt: 'A nice photo' }}
        />
      );
      const img = container.querySelector('img');
      expect(img).toHaveAttribute('alt', 'A nice photo');
    });

    it('shows placeholder when image fails to load', () => {
      const { container } = render(
        <ImagePanel {...defaultProps} content="https://example.com/broken.png" />
      );
      const img = container.querySelector('img')!;
      fireEvent.error(img);
      // After error, img should be gone and placeholder shown
      expect(container.querySelector('img')).not.toBeInTheDocument();
    });
  });

  // ==================== Edit Mode Tests ====================

  describe('edit mode', () => {
    const editProps = { ...defaultProps, editMode: true };

    it('renders URL and Upload tabs', () => {
      render(<ImagePanel {...editProps} />);
      expect(screen.getByRole('tab', { name: /url/i })).toBeInTheDocument();
      expect(screen.getByRole('tab', { name: /upload/i })).toBeInTheDocument();
    });

    it('defaults to URL tab when content is a regular URL', () => {
      render(
        <ImagePanel {...editProps} content="https://example.com/photo.png" />
      );
      const urlTab = screen.getByRole('tab', { name: /url/i });
      expect(urlTab).toHaveAttribute('aria-selected', 'true');
    });

    it('defaults to Upload tab when content is an uploaded image URL', () => {
      render(
        <ImagePanel
          {...editProps}
          content="/api/v1/dashboards/images/abc-123.png"
        />
      );
      const uploadTab = screen.getByRole('tab', { name: /upload/i });
      expect(uploadTab).toHaveAttribute('aria-selected', 'true');
    });

    it('calls onContentChange when URL input changes', () => {
      const onContentChange = vi.fn();
      render(
        <ImagePanel {...editProps} onContentChange={onContentChange} />
      );
      const urlInput = screen.getByPlaceholderText('Image URL...');
      fireEvent.change(urlInput, { target: { value: 'https://new-url.com/img.png' } });
      expect(onContentChange).toHaveBeenCalledWith('https://new-url.com/img.png');
    });

    it('renders alt text input', () => {
      render(<ImagePanel {...editProps} />);
      expect(screen.getByPlaceholderText('Alt text (optional)')).toBeInTheDocument();
    });

    it('calls onConfigChange when alt text changes', () => {
      const onConfigChange = vi.fn();
      render(
        <ImagePanel {...editProps} onConfigChange={onConfigChange} />
      );
      const altInput = screen.getByPlaceholderText('Alt text (optional)');
      fireEvent.change(altInput, { target: { value: 'My image' } });
      expect(onConfigChange).toHaveBeenCalledWith({ imageAlt: 'My image' });
    });

    it('shows image preview when content is set', () => {
      const { container } = render(
        <ImagePanel {...editProps} content="https://example.com/photo.png" />
      );
      const img = container.querySelector('.image-panel img');
      expect(img).toBeInTheDocument();
      expect(img).toHaveAttribute('src', 'https://example.com/photo.png');
    });

    it('shows upload dragger with instructions when Upload tab is clicked', () => {
      render(<ImagePanel {...editProps} />);
      fireEvent.click(screen.getByRole('tab', { name: /upload/i }));
      expect(screen.getByText('Click or drag an image file here')).toBeInTheDocument();
      expect(screen.getByText(/JPG, PNG, GIF, SVG, or WebP/)).toBeInTheDocument();
    });
  });

  // ==================== Upload Validation Tests ====================

  describe('upload validation', () => {
    const editProps = { ...defaultProps, editMode: true };

    it('rejects files with invalid type', () => {
      render(<ImagePanel {...editProps} />);
      fireEvent.click(screen.getByRole('tab', { name: /upload/i }));

      const file = new File(['data'], 'test.txt', { type: 'text/plain' });
      const dragger = screen.getByText('Click or drag an image file here').closest('.ant-upload')!;
      const input = dragger.querySelector('input[type="file"]')!;

      fireEvent.change(input, { target: { files: [file] } });
      expect(mockMessageError).toHaveBeenCalledWith(
        'Invalid file type. Allowed: JPG, PNG, GIF, SVG, WebP'
      );
      expect(mockUploadImage).not.toHaveBeenCalled();
    });

    it('rejects files exceeding 5 MB', () => {
      render(<ImagePanel {...editProps} />);
      fireEvent.click(screen.getByRole('tab', { name: /upload/i }));

      // Create a file object with a large size
      const file = new File(['x'], 'big.png', { type: 'image/png' });
      Object.defineProperty(file, 'size', { value: 6 * 1024 * 1024 });

      const dragger = screen.getByText('Click or drag an image file here').closest('.ant-upload')!;
      const input = dragger.querySelector('input[type="file"]')!;

      fireEvent.change(input, { target: { files: [file] } });
      expect(mockMessageError).toHaveBeenCalledWith(
        'File exceeds maximum size of 5 MB'
      );
      expect(mockUploadImage).not.toHaveBeenCalled();
    });
  });

  // ==================== Upload Success/Failure Tests ====================

  describe('upload behavior', () => {
    const editProps = { ...defaultProps, editMode: true };

    it('calls uploadImage and onContentChange on successful upload', async () => {
      const onContentChange = vi.fn();
      mockUploadImage.mockResolvedValue({
        url: '/api/v1/dashboards/images/uuid-123.png',
        filename: 'photo.png',
      });

      render(
        <ImagePanel {...editProps} onContentChange={onContentChange} />
      );
      fireEvent.click(screen.getByRole('tab', { name: /upload/i }));

      const file = new File(['image-data'], 'photo.png', { type: 'image/png' });
      const dragger = screen.getByText('Click or drag an image file here').closest('.ant-upload')!;
      const input = dragger.querySelector('input[type="file"]')!;

      fireEvent.change(input, { target: { files: [file] } });

      await waitFor(() => {
        expect(mockUploadImage).toHaveBeenCalledWith(file);
      });

      await waitFor(() => {
        expect(onContentChange).toHaveBeenCalledWith('/api/v1/dashboards/images/uuid-123.png');
        expect(mockMessageSuccess).toHaveBeenCalledWith('Uploaded photo.png');
      });
    });

    it('shows error message on upload failure', async () => {
      const onContentChange = vi.fn();
      mockUploadImage.mockRejectedValue(new Error('Network error'));

      render(
        <ImagePanel {...editProps} onContentChange={onContentChange} />
      );
      fireEvent.click(screen.getByRole('tab', { name: /upload/i }));

      const file = new File(['image-data'], 'photo.png', { type: 'image/png' });
      const dragger = screen.getByText('Click or drag an image file here').closest('.ant-upload')!;
      const input = dragger.querySelector('input[type="file"]')!;

      fireEvent.change(input, { target: { files: [file] } });

      await waitFor(() => {
        expect(mockMessageError).toHaveBeenCalledWith('Failed to upload image');
      });
      expect(onContentChange).not.toHaveBeenCalled();
    });
  });
});
