# Project Context for Claude

This project uses Air for planning-first workflow.
Please review these context files to understand the project:

## Essential Context
Review these files for project understanding:
- ./air/context/OVERVIEW.org
- ./air/context/air-conventions.org
- ./air/context/air-workflow.org
- ./air/context/architecture.org
- ./air/context/implementation-guide.org
- ./air/context/interface-design.org
- ./air/context/context-maintenance.org

## Planning Documents
For detailed specifications, browse the Air directory structure:

- Use `airctl status` to see document states and progress

## Before Implementation
1. Check current status: `airctl status --state work-in-progress,ready`
2. See all ready work: `airctl status --state ready`
3. Read the relevant Air document in ./air
4. Follow conventions in ./air/context/air-conventions.org
5. Update Implementation History after completing work

## Getting Current Project Status
Always use airctl commands for up-to-date information:
- `airctl status` - Full project status
- `airctl status --state ready` - Work ready for implementation
- `airctl status --state work-in-progress` - Currently active work
- `airctl status --by-tag` - Group by feature tags

## Creating New Features
For features without Air docs:
1. Create an Air document first if the feature is complex
2. Use org-mode format shown in ./air/context/air-conventions.org
3. Set initial state to 'draft'
4. Get approval before moving to 'ready' state

## Important Notes
- Always check existing Air docs before implementing
- Update document state when work is complete (use `airctl update` when available)
- Follow the workflow described in context files
- Keep Implementation History sections current
