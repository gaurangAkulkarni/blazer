import React from 'react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react'
import { LogPanel } from '../../components/LogPanel/LogPanel'
import { appLog } from '../../lib/appLog'

beforeEach(() => {
  appLog.clear()
})

describe('LogPanel', () => {
  describe('initial render', () => {
    it('renders the APP LOG header text', () => {
      render(<LogPanel />)
      expect(screen.getByText(/app log/i)).toBeInTheDocument()
    })

    it('shows "No log entries yet" when the buffer is empty', () => {
      render(<LogPanel />)
      expect(screen.getByText('No log entries yet')).toBeInTheDocument()
    })
  })

  describe('entries', () => {
    it('renders an entry when appLog.info is called', () => {
      appLog.info('sql', 'Test query')
      render(<LogPanel />)
      expect(screen.getByText('Test query')).toBeInTheDocument()
    })

    it('shows a formatted timestamp in HH:MM:SS.mmm pattern', () => {
      appLog.info('sql', 'ts test')
      render(<LogPanel />)
      // Timestamp format: "HH:MM:SS.mmm" — digits, colons, dot
      const tsRegex = /\d{2}:\d{2}:\d{2}\.\d{3}/
      const tsElements = screen.queryAllByText(tsRegex)
      expect(tsElements.length).toBeGreaterThan(0)
    })

    it('shows a category badge [SQL ] for sql-category entries', () => {
      appLog.info('sql', 'category badge test')
      render(<LogPanel />)
      expect(screen.getByText('[SQL ]')).toBeInTheDocument()
    })

    it('applies bg-red-950/30 class to error-level entry rows', () => {
      appLog.error('app', 'Something went wrong')
      render(<LogPanel />)
      // Find the row containing the error entry — it should have the error background
      const msgEl = screen.getByText('Something went wrong')
      // Walk up to the row div
      const row = msgEl.closest('div[class*="bg-red-950"]')
      expect(row).toBeTruthy()
    })
  })

  describe('category filters', () => {
    it('renders a SQL filter button', () => {
      render(<LogPanel />)
      // The button labelled "SQL" should be present in the filter bar
      const buttons = screen.getAllByRole('button', { name: /^SQL$/i })
      expect(buttons.length).toBeGreaterThan(0)
    })

    it('toggles the SQL filter active state when clicked', () => {
      render(<LogPanel />)
      const sqlButtons = screen.getAllByRole('button', { name: /^SQL$/i })
      const sqlFilterBtn = sqlButtons[0]
      // Initially not active (no bg-gray-700 class)
      expect(sqlFilterBtn.className).not.toMatch(/bg-gray-700/)
      fireEvent.click(sqlFilterBtn)
      // After click it should have the active class
      expect(sqlFilterBtn.className).toMatch(/bg-gray-700/)
    })

    it('hides non-SQL entries when SQL filter is active', () => {
      appLog.info('llm', 'LLM message')
      appLog.info('sql', 'SQL message')
      render(<LogPanel />)

      // Both entries visible initially
      expect(screen.getByText('LLM message')).toBeInTheDocument()
      expect(screen.getByText('SQL message')).toBeInTheDocument()

      // Click SQL filter button
      const sqlButtons = screen.getAllByRole('button', { name: /^SQL$/i })
      fireEvent.click(sqlButtons[0])

      // Now only SQL entry should be visible
      expect(screen.queryByText('LLM message')).not.toBeInTheDocument()
      expect(screen.getByText('SQL message')).toBeInTheDocument()
    })
  })

  describe('clear button', () => {
    it('calls appLog.clear when the Clear button is clicked', () => {
      const clearSpy = vi.spyOn(appLog, 'clear')
      appLog.info('sql', 'entry')
      render(<LogPanel />)
      const clearBtn = screen.getByTitle('Clear in-memory log')
      fireEvent.click(clearBtn)
      expect(clearSpy).toHaveBeenCalledTimes(1)
      clearSpy.mockRestore()
    })
  })

  describe('search input', () => {
    it('filters entries by message text', async () => {
      appLog.info('sql', 'alpha query')
      appLog.info('llm', 'beta response')
      render(<LogPanel />)

      const searchInput = screen.getByPlaceholderText('Search logs…')
      fireEvent.change(searchInput, { target: { value: 'alpha' } })

      // Wait for the debounce (150ms) to resolve
      await waitFor(() => {
        expect(screen.queryByText('beta response')).not.toBeInTheDocument()
      }, { timeout: 500 })

      expect(screen.getByText('alpha query')).toBeInTheDocument()
    })
  })

  describe('footer', () => {
    it('shows the entry count in the footer', () => {
      appLog.info('sql', 'one')
      appLog.info('sql', 'two')
      render(<LogPanel />)
      // Footer shows "2 entries in memory"
      expect(screen.getByText(/2 entries in memory/i)).toBeInTheDocument()
    })

    it('shows "1 entry in memory" (singular) for a single entry', () => {
      appLog.info('sql', 'sole entry')
      render(<LogPanel />)
      expect(screen.getByText(/1 entry in memory/i)).toBeInTheDocument()
    })
  })
})
