# Gear to import dicom files from Google Healthcare API

## Example

```
docker build -t ghc-import .

$ fw login ...
$ fw gear upload
```

## Run tests

```
pip install -r requirements.txt
pip install --no-deps dicomweb-client
pip install -r test_requirements.txt

pytest test_imports.py --cov=run
```
