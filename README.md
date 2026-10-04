# Real-Time-Collaborative-Task-board
Real-Time Kanban Board

button\_magic Source guide 

### Real-Time Kanban Board

# Technical Deep-Dive: Real-Time Collaborative Task Board in Web &amp; Mobile Development Projects

In modern full-stack engineering, building software that updates instantly across multiple client sessions without manual browser refreshes is a fundamental requirement. Within the **GDG-USAR Web &amp; Mobile Development Projects curriculum**, the **Real-Time Collaborative Task Board** serves as the definitive engineering framework for mastering **low-latency event-driven architecture, WebSocket pub/sub data pipelines, and concurrent state reconciliation**.

\-------------------------------------------------------------------------------- 

## 1\. The Real-Time Task Board in the Larger Context of Web &amp; Mobile Projects

The GDG-USAR Web &amp; Mobile Development curriculum is structured across four distinct engineering disciplines, each addressing a specialized technical domain:

```
┌─────────────────────────────────────────────────────────────────────────┐
│              Web &amp; Mobile Development Engineering Portfolio             │
├────────────────────────────────┬────────────────────────────────────────┤
│ Project Domain                 │ Core Technical Focus                   │
├────────────────────────────────┼────────────────────────────────────────┤
│ 1. Interactive 3D Showcase     │ WebGL Render Loops, Draco Mesh         │
│    (3D Web &amp; Frontend)         │ Compression &amp; Raycasting     │
├────────────────────────────────┼────────────────────────────────────────┤
│ 2. File-Sharing Platform       │ Multi-Tenant Isolation, Row-Level      │
│    (Full-Stack Web &amp; Security) │ Security (RLS) &amp; Blob Streams│
├────────────────────────────────┼────────────────────────────────────────┤
│ 3. Collaborative Task Board    │ WebSocket Pub/Sub Channels, Postgres   │
│    (Realtime &amp; WebSockets)     │ CDC &amp; Concurrency Control     │
├────────────────────────────────┼────────────────────────────────────────┤
│ 4. Campus Utility App          │ Cross-Platform Mobile UX, On-Device    │
│    (Mobile Development)        │ Compression &amp; Offline Caching│
└────────────────────────────────┴────────────────────────────────────────┘

```

* **Interactive 3D Product Showcase**: Focuses on browser-based 3D graphics rendering using Three.js / React Three Fiber, optimizing frame rates on low-end GPUs via Device Pixel Ratio (DPR) capping, and converting 2D click coordinates into 3D world space using raycasting.
* **File-Sharing Platform**: Focuses on backend cloud security, managing multi-tenant file isolation using PostgreSQL Row-Level Security (RLS), streaming large uploads to prevent Node process memory exhaustion, and generating short-lived presigned URLs.
* **Campus Utility App**: Focuses on mobile client engineering, handling hardware integrations (Expo Camera/GPS), keyboard obscuration, and persistent local storage rehydration with graceful offline degradation.
* **Real-Time Collaborative Task Board**: Focuses on **distributed state synchronization**. While standard web applications operate on synchronous HTTP request-response cycles, the Task Board transitions to an **asynchronous, full-duplex event-driven model** where state mutations propagate across independent browser sessions in under 100ms.

\-------------------------------------------------------------------------------- 

## 2\. Core Architectural Blueprint, Tech Stack &amp; Schema

### Problem Statement &amp; Architectural Need

Traditional Kanban board implementations suffer from stale client data. When multiple team members interact with the same board, changes made by User A are invisible to User B until User B manually reloads the page or triggers a background HTTP polling request. HTTP polling introduces severe network overhead and fails to prevent race conditions during simultaneous edits.

To solve this, the architecture combines a **smooth drag-and-drop client interface** with an **event-driven pub/sub data pipeline**.

```
[ User Action: Drag Card / Edit Task ]
                  │
                  ▼
┌───────────────────────────────────────────────────────────┐
│ Module 1: Local Optimistic UI State                       │
│  ├── Instantly reorders card in local React state         │
│  └── Renders UI update immediately (&lt; 16ms frame target)  │
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 2: Mutation &amp; Concurrency Guard                    │
│  ├── Submits async UPDATE/INSERT payload to PostgreSQL    │
│  └── Evaluates Optimistic Concurrency Control (version ID)│
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 3: Postgres Logical Replication / Realtime Hub     │
│  ├── Database commits SQL mutation row                    │
│  └── Change-Data-Capture (CDC) converts change to WebSocket│
└────────────────────────┬──────────────────────────────────┘
                         │
                         ▼
┌───────────────────────────────────────────────────────────┐
│ Module 4: Remote Subscriber Browser Windows               │
│  ├── WebSocket receives `postgres_changes` event payload  │
│  ├── Merges delta payload into local component state      │
│  └── Re-renders card position dynamically without refresh │
└───────────────────────────────────────────────────────────┘

```

### Technology Stack Selection

* **Framework**: Next.js 14+ (App Router) for hybrid client/server rendering.
* **Realtime Engine**: Supabase Realtime (or self-hosted Socket.IO), utilizing managed WebSockets running over PostgreSQL Logical Replication / Change-Data-Capture (CDC).
* **Database**: PostgreSQL with native CDC publication support.
* **Drag-and-Drop Handler**: `@hello-pangea/dnd` (React 18+ fork of `react-beautiful-dnd`) for accessible card drag operations.
* **Styling**: Tailwind CSS for responsive multi-column layouts.

### Relational Database Schema &amp; CDC Enablers

The underlying PostgreSQL database schema must support atomic position reordering and concurrency tracking:

```
-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- Custom Task Status Enum
create type task_status as enum ('TODO', 'IN_PROGRESS', 'COMPLETED');

-- Tasks Table Schema
create table public.tasks (
  id uuid default uuid_generate_v4() primary key,
  title text not null,
  description text default '',
  status task_status default 'TODO'::task_status not null,
  position integer default 0 not null, -- Stores column ordering sequence
  assignee text default 'Unassigned',
  version integer default 1 not null, -- Used for Concurrency Control (OCC)
  updated_at timestamp with time zone default timezone('utc'::text, now()) not null,
  created_at timestamp with time zone default timezone('utc'::text, now()) not null
);

-- Index for status-based column queries
create index idx_tasks_status on public.tasks(status);

-- CRITICAL: Enable Postgres Logical Replication for Realtime Pub/Sub Broadcasting
alter publication supabase_realtime add table public.tasks;

```

\-------------------------------------------------------------------------------- 

## 3\. Code Breakdown &amp; Underlying Logic

The core logic of the collaborative board revolves around managing dual state sources: **Local Component State** (for responsive rendering) and **Remote Server State** (pushed via WebSockets).

### Foundational React + Realtime Synchronization Implementation

```
'use client';

import React, { useEffect, useState } from 'react';
import { createClient } from '@supabase/supabase-js';
import { DragDropContext, Droppable, Draggable, DropResult } from '@hello-pangea/dnd';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
const supabase = createClient(supabaseUrl, supabaseAnonKey);

type StatusType = 'TODO' | 'IN_PROGRESS' | 'COMPLETED';

interface Task {
  id: string;
  title: string;
  status: StatusType;
  position: number;
  version: number;
}

export default function KanbanBoard() {
  const [tasks, setTasks] = useState
```
