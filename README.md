# Database 2 SQLite
## Description
The goal of this project is to cache the view of a specific query for a database like MongoDB. This view in SQLite can 
then be used in a project like Apache SuperSet or similar to further query and visualize the data.

## Installation
    cd /opt
    git clone https://github.com/guruevi/db2sqlite.git 
    cd db2sqlite
    mkdir queries db
    python -m venv ./venv
    source ./venv/bin/activate
    pip install -r requirements.txt

## Usage
Crontab example:

    0 0 * * * /opt/db2sqlite/venv/bin/python /opt/db2sqlite/db2sqlite.py

## Future goals
- [ ] Add support for more databases
- [ ] Break out each database into modules or classes