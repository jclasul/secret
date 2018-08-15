# Use an official Python runtime as a parent image
FROM python:3.6.6-alpine3.6

# Set the working directory to /app
WORKDIR /app/

# Copy the current directory contents into the container at /app
ADD dax/. /app/
ADD common/api.py /app/dax/src/

RUN pip install --upgrade pip
# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME orders_bitcoin

# only run when migrating
#CMD ["python", "src/datatransfer.py"]

# Run app.py when the container launches
CMD ["python", "src/daxy.py"]