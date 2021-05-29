![](https://img.shields.io/badge/code%20style-black-000000.svg)

# Development

## Install dev requirements
`install Python 3.9`
`pip install -r requirements.txt`

## Run test
- `pytest --cov=app` run test
- `pytest --cov=app --cov-report html` run test with html report

# Using

## Application launch
1. Go to the root folder of the weather_analysis application
2. Type the command in the console: python app\weather.py arg1 arg2 arg3
where arg1 is the path to the resource file (the trailing slash is not needed)
      arg2 - path to the directory where to put the information after processing
      arg3 - number of threads to process data 

## Documentation location file 
root\documentation\build\html\index.html