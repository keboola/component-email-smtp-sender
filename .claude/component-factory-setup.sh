#!/bin/bash

# Skip if project doesn't use pre-commit
if ! test -f pyproject.toml || ! test -f .pre-commit-config.yaml; then
    echo '{"systemMessage": "⚠️ Pre-commit hooks: project does not use pre-commit, skipping setup"}'
    exit 0
fi

# Check if already installed
if test -f .git/hooks/pre-commit; then
    echo '{"systemMessage": "✅ Pre-commit hooks already installed"}'
    exit 0
fi

# Install
if uv sync && uv run pre-commit install; then
    echo '{"systemMessage": "✅ Pre-commit hooks installed successfully"}'
else
    echo '{"systemMessage": "❌ Pre-commit hooks failed to install"}'
    exit 1
fi
