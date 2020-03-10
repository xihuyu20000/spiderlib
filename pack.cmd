rd /s/q build
rd /s/q dist
rd /s/q "spiderlib.egg-info"
C:/Users/Administrator/.virtualenvs/spiderlib/Scripts/python.exe setup.py sdist bdist_wheel
C:/Users/Administrator/.virtualenvs/spiderlib/Scripts/python.exe setup.py install
C:/Users/Administrator/.virtualenvs/spiderlib/Scripts/python.exe -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
C:/Users/Administrator/.virtualenvs/spiderlib/Scripts/python.exe pip install -U -i https://test.pypi.org/simple/ spliderlib
C:/Users/Administrator/.virtualenvs/spiderlib/Scripts/python.exe -m twine upload dist/*