# Bigdata

Project root for data / code. This repository contains a Python virtual environment at `.venv`.

Quick start

1. Activate the virtual environment (zsh):

```zsh
cd /Users/ravishan/Downloads/Bigdata
source .venv/bin/activate
```

2. Verify Python and pip:

```zsh
python -V
pip -V
```

3. (Optional) Upgrade pip and install dependencies from `requirements.txt` if present:

```zsh
python -m pip install --upgrade pip
pip install -r requirements.txt
```

VS Code

- Select the interpreter at `./.venv/bin/python` via `Python: Select Interpreter`.

Notes

- The `.gitignore` excludes `.venv` and common caches/IDE files.
