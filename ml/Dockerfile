# Use an official Python runtime as a parent image
FROM python:3.6.6-jessie 
#alpine3.6

# Set the working directory to /app
WORKDIR /app/

# Copy the current directory contents into the container at /app
ADD ml/. /app/
ADD common/api.py /app/ml/src/

RUN pip install --upgrade pip
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 8888

# Define environment variable
ENV NAME prophet_bitcoin

# Run app.py when the container launches
CMD ["python", "src/fbp.py"]