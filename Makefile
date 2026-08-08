.PHONY: install run

PYTHON := python
PIP := pip

# Ruff is pinned to the same version .github/workflows/tests.yaml installs.
# That job gates now, and pyproject sets no lint.select, so an unpinned local
# ruff would apply a different default rule set than the one you are judged by.
RUFF_VERSION := 0.16.2

# `ruff check` runs without --exit-zero: the tree is clean and CI gates on it,
# so a violation should stop you here rather than in a PR.
lint: ## Run all linting tools
	@echo "Installing linting tools..."
	@$(PIP) install --quiet --upgrade pycln isort ruff==$(RUFF_VERSION)
	@echo "Running Ruff checks..."
	@$(PYTHON) -m ruff check --fix
	@echo "Cleaning unused imports..."
	@$(PYTHON) -m pycln .
	@echo "Sorting imports..."
	@$(PYTHON) -m isort .
	@echo "Formatting code..."
	@$(PYTHON) -m ruff format $(SRC_DIR)
	@echo "Linting complete!"