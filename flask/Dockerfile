# Use an official Python runtime as a parent image
FROM python:3.6.6-jessie 

# Set the working directory to /app
WORKDIR /app/

# Copy the current directory contents into the container at /app
ADD dax/. /app/

RUN pip install --upgrade pip
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
ENV NAME flasky

# Run app.py when the container launches
RUN export FLASK_APP=application.py
CMD ["flask", "application.py", "--host=0.0.0.0"]