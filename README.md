publisher
=========

A simple golang stomp publisher

Depends
=======

Depends on the following packages

  `go get github.com/gmallard/stompngo`
  `go get github.com/ogier/pflag`

Usage
=====
  `./publisher -h`
  `./publisher -b broker.net.com:61616 -q TEST -m "test this" --username=admin --password=admin`
  `./publisher -b broker.net.com:61616 -q TEST -f ./fileofmsgs.txt --username=admin --password=admin`
  