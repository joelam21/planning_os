
# choose a python version and pin it to the project
pyenv install -s 3.12.8
pyenv local 3.12.8
python --version

# create the venv 
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

# sanity check (must point to the .venv)
which python
python -c "import sys; print(sys.executable)"

