eval $(conda shell.bash hook)
conda env create -f environment.yml 
conda activate qsat-tender-scraper-env
git init 
pre-commit install 


