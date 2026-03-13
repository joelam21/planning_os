Cmd + Shift + P  # Open Command Palette (buttons not to go on command line)


chmod +x scripts/single_command_bootstrap.sh
./scripts/single_command_bootstrap.sh planning_os

# ===== Reload shell configuration ======
source ~/.zshrc

# ====== Activate the environment and verify ======
source ./enter.sh 
which python  
python -c "import sys; print(sys.executable)"

# ===== freeze requirements ======
pip list
python -m pip freeze > requirements.txt

# ===== install requirements ======
python -m pip install -r requirements.txt

chmod +x scripts/01_create_repo.sh
./scripts/01_create_repo.sh planning_os # You can replace "planning_os" with your desired project name.

chmod +x scripts/02_pin_python_create_venv.sh
./scripts/02_pin_python_create_venv.sh

chmod +x scripts/preflight.sh
./scripts/preflight.sh

git st
source ./enter.sh
./scripts/preflight.sh