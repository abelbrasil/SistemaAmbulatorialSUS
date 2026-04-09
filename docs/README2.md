poetry new 01Streamlit
cd 01Streamlit
pyenv local 3.14.3 
poetry env use 3.14.3
code .
source .venv/bin/activate
poetry add pandas
pip freeze

ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/PACE2512.dbc
ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/PACE2511.dbc