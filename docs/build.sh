pip install --user sphinx furo
sphinx-apidoc -o source ../nebula3
make clean
make html
