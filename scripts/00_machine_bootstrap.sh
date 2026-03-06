
# Installs: pyenv, git, docker, snowflake-cli, and dbt
brew install pyenv
grep -q 'pyenv init' ~/.zshrc || echo 'eval "$(pyenv init -)"' >> ~/.zshrc
source ~/.zshrc

# Check pyenv installation
pyenv --version

