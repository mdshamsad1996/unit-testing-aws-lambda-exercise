#### Lambda unit testing execrise
***Task:***
        ```1.Refactor the code to make more testable```
        ```2. Do the unit testing using pytest```
##### Steps
* install pipenv using command ```pip install --user pipenv```
* activate the virtual environment ```pipenv shell```
* install depedencies from pip file ```pipenv lock; pipenv sync --dev```
* Go to src directory ```cd src/```
* Run the unit test cases ```python -m pytest -v -s```
* create file ```.coveragec``` and do configuration aacording to the requirement
* run for coverage ```coverage run refactor_lambda.py```
* run command for report ```coverage report -m```
* for nicer representation run ```coverage html``` . A ```htmlcov``` folder would be created , open ```index.html``` from htmlcov folder

###### for CI/CD
```create buildspec.yml file and do the configuration. Use AWS build service to implement this```
