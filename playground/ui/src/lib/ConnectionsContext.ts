import React from 'react'
import type { ConnectionAlias } from './types'

export const ConnectionsContext = React.createContext<ConnectionAlias[]>([])
