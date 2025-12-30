#!/bin/bash

echo "Setting up project in WSL..."

# Create project directory in WSL
mkdir -p ~/bigdata_project

# Copy project files from Windows to WSL
cp -r /mnt/d/NAM_4/Hoc_ky_7/Thuchanh_BIGDATA/IE212.Q11/* ~/bigdata_project/

# Change to project directory
cd ~/bigdata_project

echo "Installing Python dependencies..."

# Update package list
sudo apt update

# Install Python3 and pip if not already installed
sudo apt install -y python3 python3-pip python3-venv python3-dev build-essential

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip and install build tools
pip install --upgrade pip setuptools wheel

# Install requirements one by one with compatible versions for Python 3.12
echo "Installing numpy..."
pip install "numpy<2.0"

echo "Installing opencv-python..."
pip install opencv-python>=4.8.0

echo "Installing mediapipe..."
pip install mediapipe>=0.10.13

echo "Installing pyspark..."
pip install pyspark==3.4.3

echo "Installing py4j..."
pip install py4j==0.10.9.7

echo "Installing pytest..."
pip install pytest>=7.0.0