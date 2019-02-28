FROM python:3
ADD Homelike.py /
ADD conversion_data.csv /
RUN pip install pandas
CMD [ "python", "./Homelike.py" ]