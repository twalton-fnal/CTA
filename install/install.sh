#!/usr/bin/env bash

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
#  This script installs the packages that are deployed in the CTA analysis workflow
#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

set -o errexit


echo -e "Enter install the CTA python virtual environment software stack." 2>&1


# set directory
export CURRENT_INSTALL_DIR=${PWD}

# name of python virtual environment
VNAME="CTA_LTO9_WORKFLOW.venv"

# go to the install dir
cd ../../
export INSTALL_DIR=${PWD}
echo -e "\tThe installation directory is [$INSTALL_DIR]." 2>&1

# create a python virtual environment
if [[ -e "$INSTALL_DIR/$VNAME" ]]; then
   echo -e "\tThe python virtual environment [$VNAME] exists. Please delete it and rerun the script for installation."
   exit 1
fi

python -m venv "$VNAME"
source ${INSTALL_DIR}/${VNAME}/bin/activate


# install packages
pip install --upgrade pip matplotlib scipy

echo -e "Completed installation of external packages." 2>&1
echo -e "\t To exit: type deactivate." 2>&1
