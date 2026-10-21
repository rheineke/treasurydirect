# treasurydirect
TreasuryDirect data retrieval and analysis

## Setup
```zsh
conda env create -f environment.yaml
conda activate treasurydirect
pre-commit install
```

and
```shell
pip install -e ".[dev,test]"
```

## Tests
```shell
pytest
```
