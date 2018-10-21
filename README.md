# CanDev-Finance-Canada

Realtime economic monitoring.

This project was broken down into three components:

1.  Data Gathering
2.  Forcasting and Data Smoothing
3.  Data Visualization

## Data Gathering

Data gathering was broken into several components:

1.  Get raw tables from [Statcan](https://www.statcan.gc.ca/)
2.  Structuring the data into fixed tables
3.  Storing resulting tables

The code for data gathering can be found under [gathernomics](gathernomics/)
(*gather* -ing eco- *nomics* data).

### How to Run
Download Python requirements.

```Bash
$ pip3 install psycopg --user
$ pip3 install psycopg-binary --user
$ pip3 install urllib3
```

Setup environment
```Bash
$ # From project root director
$ export PYTHONPATH=`pwd`:$PYTHONPATH
```

Run Program:
```Bash
$ # From project root director
$ python3 gathernomics
```

### Future Work

Remaining works with *Data Gathering*:

1.  Store results of the table dumps in a Postgresql DB
2.  Add filters to gather more granualized *capital* economic data
3.  Read from *Statcan*'s daily *Delta File* to more efficiently grab data

The Postgresql database back-end could not be fully implemented, though a
significant amount of the modeling work has been complete.
