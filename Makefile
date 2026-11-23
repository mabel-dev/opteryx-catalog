.PHONY: install run

PYTHON := python
PIP := pip

# Ruff is pinned because pyproject sets no lint.select: an unpinned ruff would
# apply its own default rule set, which changes between releases.
RUFF_VERSION := 0.16.2

# `ruff check` runs without --exit-zero: the tree is clean, so a violation fails
# `make lint`. CI does not run lint; run it before opening a PR.
lint: ## Run all linting tools
	@echo "Installing linting tools..."
	@$(PIP) install --quiet --upgrade pycln ruff==$(RUFF_VERSION)
	@echo "Running Ruff checks..."
	@$(PYTHON) -m ruff check --fix
	@echo "Cleaning unused imports..."
	@$(PYTHON) -m pycln .
	@echo "Formatting code..."
	@$(PYTHON) -m ruff format $(SRC_DIR)
	@echo "Linting complete!"