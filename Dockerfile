# Base image nhẹ
FROM python:3.12-slim

# Tạo user không phải root
ENV USER=appuser
ENV HOME=/home/$USER
RUN useradd --create-home --shell /bin/bash $USER

# Cài đặt hệ thống cần thiết
RUN apt-get update && apt-get install -y curl build-essential && apt-get clean

# Thiết lập biến môi trường cho Poetry
ENV POETRY_VERSION=2.1.3
ENV PATH="${HOME}/.local/bin:$PATH"

# Cài đặt Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Fix lỗi quyền khi Poetry ghi file config lần đầu
RUN mkdir -p /home/appuser/.config/pypoetry && chown -R $USER:$USER /home/appuser/.config

# Tạo thư mục project và copy source code
WORKDIR /app

# Copy file cấu hình Poetry trước để cache tốt hơn
COPY --chown=$USER:$USER pyproject.toml poetry.lock ./

# Cài đặt dependencies (tự động tạo venv bên trong /app/.venv)
# RUN poetry config virtualenvs.in-project true \
#     && poetry install --no-root --only main
RUN poetry install --no-root

# Copy toàn bộ mã nguồn sau
COPY --chown=$USER:$USER ./src ./src

# Set biến môi trường
ENV PYTHONPATH=/app/src
WORKDIR /app/src

# Chuyển sang user không phải root
USER $USER

# Mở port cho FastAPI
EXPOSE 8000

# Lệnh chạy ứng dụng FastAPI
CMD ["poetry", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
