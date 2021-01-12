FROM ubuntu:20.04

# https://stackoverflow.com/questions/58269375/
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN apt-get update
RUN apt-get install -y wget git
RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 


RUN git clone https://github.com/see-mof/pansat/ --branch main --single-branch
cd pansat
RUN conda install -c anaconda hdf5 satpy cartopy
RUN pip install -e . pytest

