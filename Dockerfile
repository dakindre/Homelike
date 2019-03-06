FROM python:3
ADD Homelike.py /
ADD conversion_data_large.csv /
RUN pip install pandas
RUN pip install "dask[dataframe]" 
CMD [ "python", "./Homelike.py" ]