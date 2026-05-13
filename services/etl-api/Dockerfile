FROM python:3.12-slim

# tesseract-ocr is needed by pytesseract; libgl + libglib are needed by PyMuPDF
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        libtesseract-dev \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

EXPOSE 8092
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8092"]
