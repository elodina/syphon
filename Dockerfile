FROM ubuntu

MAINTAINER elodina

#Go settings
ENV GOLANG_VERSION 1.4
ENV GOLANG_RELEASE go$GOLANG_VERSION
ENV GOLANG_URL https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOROOT /usr/bin/go
ENV GOPATH /
ENV PATH $GOROOT/bin:$PATH

#Get git and mercurial
RUN sudo apt-get update
RUN sudo apt-get -y install git
RUN sudo apt-get -y install mercurial

#Get Go
RUN wget -q $GOLANG_URL -O /tmp/$GOLANG_RELEASE.tar.gz
RUN tar -xzf /tmp/$GOLANG_RELEASE.tar.gz -C /usr/bin
RUN mkdir -p $GOPATH/src

#Get GPM and GVP
RUN git clone https://github.com/pote/gpm.git && cd gpm && git checkout v1.3.1 && ./configure && make install

#Get Syphon
RUN git clone https://github.com/elodina/syphon.git && cd syphon && ./build.sh && ln -s $(pwd)/scheduler /usr/bin && ln -s $(pwd)/executor /usr/bin