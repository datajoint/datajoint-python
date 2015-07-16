BASE='/home/vagrant'
PYENV_PATH="$BASE/.pyenv"
PYTHON_VER="3.4.3"
PROJECT="/vagrant"

echo "Setting up Python environment"
apt-get update >/dev/null 2>&1
echo "Installing essential packages"
apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
  libreadline-dev libsqlite3-dev wget curl llvm libpng12-dev libfreetype6-dev \
  pkg-config git vim >/dev/null 2>&1

echo "Installing Python 3"
apt-get install -y python3 python3-pip

pip3 install --upgrade pip


cd "$PROJECT"

# Install minimal requirement for running the package
if [ -f "$PROJECT/requirements.txt" ]; then
  echo "Installing Python packages"
  pip3 install -r "$PROJECT/requirements.txt"
fi

# Install additional development requirements
if [ -f "$PROJECT/misc/dev-requirements.txt" ]; then
  echo "Installing Python packages"
  pip3 install -r "$PROJECT/misc/dev-requirements.txt"
fi

echo "Setting up database connections"

sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password root'
sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password root'
sudo apt-get install -y mysql-server 2> /dev/null
sudo apt-get install -y mysql-client 2> /dev/null

mysql -uroot -proot < "$PROJECT/misc/db_setup.sql"
