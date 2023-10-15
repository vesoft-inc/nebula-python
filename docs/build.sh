pip install --user sphinx furo
sphinx-apidoc -f -o source ../nebula3 \
      ../nebula3/common/* \
      ../nebula3/data/* \
      ../nebula3/fbthrift/* \
      ../nebula3/graph/* \
      ../nebula3/mclient/* \
      ../nebula3/meta/* \
      ../nebula3/sclient/* \
      ../nebula3/storage/*
make clean
make html
