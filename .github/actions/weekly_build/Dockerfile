FROM ubuntu:20.04

RUN apt-get update

# Make sure locale is set.
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
RUN apt-get install -y locales && \
    locale-gen en_US.UTF-8

# https://stackoverflow.com/questions/58269375/
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN apt-get install -y wget git sudo
RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 

RUN conda install -c anaconda h5py hdf5 notebook \
    && conda install -c conda-forge satpy cartopy pyhdf
RUN pip install pytest appdirs beautifulsoup4 matplotlib lxml boto3

COPY entrypoint.sh /entrypoint.sh
COPY cleanup.sh /cleanup.sh
#ENTRYPOINT ["/entrypoint.sh"]
