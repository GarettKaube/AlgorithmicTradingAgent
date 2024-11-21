FROM tiangolo/uvicorn-gunicorn-fastapi:latest
COPY fastapi_requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir
ENV my_secret=""
ENV sec_key=""
COPY . /app
WORKDIR /app
EXPOSE 8000
CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]
