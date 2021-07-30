# update index
sudo apt-get update

# add open ssh so you can ssh into this machine
sudo apt install openssh-server

# Allow port 22 so you can ssh in with airflow
sudo ufw allow 22

# Get net tools so you can run ifconfig and get ip (if needed)
sudo apt install net-tools 


# Enable linux vm for remote desktop connection
sudo apt install xrdp


# Get chrome download
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

# Install google chrome
sudo apt install ./googe-chrome-stable_current_amd64.deb

# Install git
sudo apt install git -y

git config --user


# Install Anaconda prerequisites
sudo apt-get install libgl1-mesa-glx libegl1-mesa libxrandr2 libxrandr2 libxss1 libxcursor1 libxcomposite1 libasound2 libxi6 libxtst6



# To Do setup conda virtual environment from variables


# Create a credentials file that will be ignored by .gitignore