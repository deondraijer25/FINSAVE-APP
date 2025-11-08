FROM php:7.2-fpm

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    mariadb-client \
    libpng-dev \
    libjpeg62-turbo-dev \
    libxml2-dev \
    locales \
    zlib1g-dev \
    zip \
    jpegoptim optipng pngquant gifsicle \
    vim \
    unzip \
    git \
    curl

# Install extensions
RUN docker-php-ext-install pdo_mysql mbstring zip exif pcntl
RUN docker-php-ext-install gd
RUN docker-php-ext-install gettext
RUN docker-php-ext-install mysqli
RUN docker-php-ext-install soap 

WORKDIR '/var/www'

COPY . .
