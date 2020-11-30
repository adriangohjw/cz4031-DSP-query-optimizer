# cz4031-DSP-query-optimizer
NTU Computer Science - CZ4031 Database System Principles (Project 2: Query Optimizer)

## Setting up evironment from scratch

### Setup virtual environment in project

#### Windows
In Windows CMD, ensure you are in the folder of your repository

1. Run `python –m venv venv`
2. Run `venv\Scripts\activate` 
3. Run `pip install -r requirements.txt`

`venv\Scripts\activate` is also the <b>command to enter your virtual environment</b> whenever you want to run the application on 
All required packages should have been installed!

#### Ubuntu

In Ubuntu, ensure you are in the folder of your repository

1. Run `python –m venv venv`
2. Run `source venv/bin/activate` 
3. Run `pip install -r requirements.txt`

`source venv/bin/activate` is also the <b>command to enter your virtual environment</b> whenever you want to run the application on CMD
All required packages should have been installed!

#### Pre-generating statistical summaries

Run the command `python setup.py` in the console. This will start the generation of the statistical summaries and will be stored in the `temp` folder. This process will take 2-3 minutes, depending on the size of the dataset.

## Running the application

Once the requirements have been added, you can run the streamlit app by
`streamlit run app.py`

## Setting up environment using Docker
Alternatively you can build and run a docker image from the Dockerfile given in this repo. 
For more information on hwo to build and run images locally, you can follow this [link](https://docs.docker.com/get-started/part2/).
