# Project Overview

## Description
<!-- TODO: Replace with your project description -->
This project uses Air for planning-first workflow, where planning documents serve as the single source of truth for requirements and specifications.

## Core Principles
<!-- Customize these principles based on your project's philosophy -->
- Planning-first methodology
- Clear specification before execution
- Trackable progress through document states
- Version-aware planning with milestone directories

## Technology Stack
<!-- TODO: Update with your project's technology stack -->
<!-- Example sections to consider:
- **Language**: [Your primary language]
- **Framework**: [Your framework choice]
- **Database**: [If applicable]
- **Testing**: [Testing framework]
- **Build System**: [Build tools]
-->

## Project Structure
<!-- Air directory structure (managed by Air) -->
- Main documentation: `./air/`
- Templates: `./air/templates/`
- Archived documents: `./air/archive/`
- Context files: `./air/context/`
- Version milestones: `./air/v0.1/`, `./air/v0.2/`, etc.

<!-- TODO: Add your source code structure -->
<!-- Example:
- Source code: `./src/`
- Tests: `./tests/`
- Documentation: `./docs/`
-->

## Architecture
<!-- TODO: Describe your project's architecture -->
<!-- Consider including:
- High-level components
- Key modules/packages
- External dependencies
- Integration points
-->

## Core Components

<!-- TODO: List and describe your project's main components -->
<!-- Example format:
### Component Name
Brief description of what this component does and its responsibilities.
-->

## Document States (Air Workflow)
Air uses these predefined states to track document lifecycle:
- `draft` - Initial planning phase
- `ready` - Specification complete, ready for execution
- `work-in-progress` - Currently being executed
- `complete` - Execution finished
- `dropped` - No longer needed
- `unknown` - State cannot be determined

## Getting Started
<!-- TODO: Customize for your project -->
1. Review current status: `airctl status`
2. Check ready work: `airctl status --state ready`
3. Read relevant Air documents in `./air/` before executing
4. Update document states as work progresses

## Current Focus
Use `airctl status --state work-in-progress,ready` to see current priorities and available work.